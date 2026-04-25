use std::collections::{HashMap, HashSet};
use std::net::Ipv4Addr;
use std::path::Path;
use std::str::FromStr;

use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};

use crate::config::{NetworkConfig, Pool};

#[derive(Debug, thiserror::Error)]
pub enum DbError {
    #[error("database error: {0}")]
    Sqlx(#[from] sqlx::Error),

    #[error("migration error: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("conflict: {0}")]
    Conflict(String),

    /// IP range or VNI range is fully allocated.
    #[error("exhausted: {0}")]
    Exhausted(String),

    /// `insert_vm` rejected the row because the host is unknown or
    /// unhealthy at the moment the insert ran. Atomic with the insert
    /// itself, so callers can release any IP they pre-allocated and
    /// surface a clean retry-able error.
    #[error("host '{0}' is unknown or unhealthy")]
    HostUnavailable(String),

    /// `insert_vm` rejected the row because the scheduling snapshot it
    /// was built from is stale: between `pick_host` reading capacity
    /// and the insert running, another concurrent `CreateMachine`
    /// consumed enough of the target host's cpu / memory / disk, or
    /// claimed one of the requested GPUs, that the placement is no
    /// longer valid. Not a host-gone signal — the host is still fine —
    /// so the caller should rebuild the snapshot and re-schedule
    /// rather than give up on the host.
    #[error("placement on host '{0}' raced a concurrent create")]
    CapacityRaced(String),

    #[error("malformed DB state: {0}")]
    Malformed(String),
}


#[derive(Debug, Clone)]
pub struct Db {
    /// Read-only pool. Multiple connections so independent read RPCs +
    /// the metrics poller + scheduler snapshots can run in parallel —
    /// SQLite in WAL mode allows unlimited concurrent readers alongside
    /// the single writer.
    reader: SqlitePool,
    /// Single-connection write pool. Every INSERT/UPDATE/DELETE funnels
    /// through this one connection so concurrent writes queue in
    /// tokio's mpsc-like connection acquisition (fair, cache-friendly)
    /// rather than inside SQLite's `busy_timeout` retry loop, which is
    /// an *unfair* sleep/retry and starves under heavy load.
    writer: SqlitePool,
    /// Multiplier applied to `hosts.total_cpu` before subtracting
    /// already-assigned vCPU in the `insert_vm` capacity gate. Lives
    /// on `Db` rather than being passed per-call because it's the
    /// storage-layer invariant `insert_vm` enforces — the scheduler
    /// reads the same value for its pre-check, and the two must agree.
    cpu_overcommit_ratio: f32,
}

impl Db {
    /// The cpu-overcommit multiplier `insert_vm` enforces at commit.
    /// Scheduler reads the same value for its pre-check so the two
    /// stages can't disagree.
    pub fn cpu_overcommit_ratio(&self) -> f32 {
        self.cpu_overcommit_ratio
    }

    pub async fn open(path: &Path, cpu_overcommit_ratio: f32) -> Result<Self, DbError> {
        let (write_options, read_options) = if path.to_string_lossy() == ":memory:" {
            let uri = format!(
                "sqlite:file:basis-mem-{}?mode=memory&cache=shared",
                uuid::Uuid::new_v4()
            );
            let shared = SqliteConnectOptions::from_str(&uri)?;
            (shared.clone(), shared)
        } else {
            let base = SqliteConnectOptions::new()
                .filename(path)
                .create_if_missing(true)
                .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
                .busy_timeout(std::time::Duration::from_secs(30));
            let read = base.clone().read_only(true);
            (base, read)
        };

        let writer = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(write_options)
            .await?;

        let reader = SqlitePoolOptions::new()
            .max_connections(32)
            .connect_with(read_options)
            .await?;

        let db = Self {
            reader,
            writer,
            cpu_overcommit_ratio,
        };
        db.migrate().await?;
        Ok(db)
    }

    async fn migrate(&self) -> Result<(), DbError> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS hosts (
                id TEXT PRIMARY KEY,
                hostname TEXT NOT NULL UNIQUE,
                total_cpu INTEGER NOT NULL,
                total_memory_mib INTEGER NOT NULL,
                total_disk_gib INTEGER NOT NULL,
                gpu_inventory TEXT NOT NULL DEFAULT '[]',
                vtep_address TEXT NOT NULL DEFAULT '',
                last_heartbeat TEXT NOT NULL,
                healthy INTEGER NOT NULL DEFAULT 1
            )",
        )
        .execute(&self.writer)
        .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS trees (
                id TEXT PRIMARY KEY,
                vni INTEGER NOT NULL UNIQUE,
                cidr TEXT NOT NULL,
                bridge_range_start TEXT NOT NULL,
                bridge_range_end TEXT NOT NULL,
                vm_range_start TEXT NOT NULL,
                vm_range_end TEXT NOT NULL,
                vip_range_start TEXT NOT NULL,
                vip_range_end TEXT NOT NULL,
                prefix_len INTEGER NOT NULL,
                created_at TEXT NOT NULL
            )",
        )
        .execute(&self.writer)
        .await?;

        // Per-(tree, host) gateway IP. Every hypervisor carrying a VM
        // in a tree owns a unique address from the tree's bridge_range
        // and assigns it to its local `brt<vni>`. VMs use their own
        // host's bridge IP as default gateway so cross-host replies
        // routing back through the gateway land on the correct
        // hypervisor — a single shared gateway IP gets hijacked by
        // whichever host happens to be the reply's source.
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS tree_host_bridges (
                tree_id TEXT NOT NULL REFERENCES trees(id),
                host_id TEXT NOT NULL,
                ip_address TEXT NOT NULL,
                PRIMARY KEY (tree_id, host_id),
                UNIQUE (tree_id, ip_address)
            )",
        )
        .execute(&self.writer)
        .await?;

        // `apiserver_pool` is the pool name chosen at CreateCluster; an
        // empty string means the VIP was carved from the tree's
        // vip_range. CreateMachine reads it on `edge: true` to decide
        // which pool the edge NIC draws from.
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS clusters (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                tree_id TEXT NOT NULL REFERENCES trees(id),
                parent_cluster_id TEXT REFERENCES clusters(id),
                control_plane_endpoint TEXT NOT NULL,
                apiserver_pool TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL
            )",
        )
        .execute(&self.writer)
        .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_clusters_tree ON clusters(tree_id)")
            .execute(&self.writer)
            .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS vms (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                cluster_id TEXT NOT NULL REFERENCES clusters(id),
                host_id TEXT NOT NULL REFERENCES hosts(id),
                ip_address TEXT NOT NULL,
                state INTEGER NOT NULL DEFAULT 0,
                cpu INTEGER NOT NULL,
                memory_mib INTEGER NOT NULL,
                disk_gib INTEGER NOT NULL,
                extra_disk_total_gib INTEGER NOT NULL DEFAULT 0,
                extra_disk_gibs TEXT NOT NULL DEFAULT '[]',
                image TEXT NOT NULL,
                error_message TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )",
        )
        .execute(&self.writer)
        .await?;

        // `(cluster_id, name)` uniqueness keeps the name-based idempotency
        // check in server.rs race-free: a concurrent retry of `CreateMachine`
        // either sees the existing row or is rejected at insert time.
        sqlx::query(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_vms_cluster_name
             ON vms (cluster_id, name)",
        )
        .execute(&self.writer)
        .await?;

        // Normalized GPU reservations — the sole source of truth for
        // which VM has which GPU on which host. The `UNIQUE (host_id,
        // pci_address)` constraint is what makes GPU scheduling
        // race-free: two concurrent `insert_vm` calls that both picked
        // the same PCI address on the same host serialize through the
        // writer, exactly one commits, and the loser gets a UNIQUE
        // violation that `insert_vm` turns into `CapacityRaced`.
        // `ON DELETE CASCADE` means deleting a VM releases its GPUs
        // with no extra application logic.
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS vm_gpus (
                vm_id TEXT NOT NULL REFERENCES vms(id) ON DELETE CASCADE,
                host_id TEXT NOT NULL,
                pci_address TEXT NOT NULL,
                model TEXT NOT NULL,
                iommu_group TEXT NOT NULL,
                nvlink_group INTEGER NOT NULL,
                PRIMARY KEY (vm_id, pci_address),
                UNIQUE (host_id, pci_address)
            )",
        )
        .execute(&self.writer)
        .await?;

        // SQLite needs FK enforcement turned on per-connection for
        // `ON DELETE CASCADE` to fire. The writer pool has
        // `max_connections = 1`, so one-shot is enough.
        sqlx::query("PRAGMA foreign_keys = ON")
            .execute(&self.writer)
            .await?;

        // Exactly one of vm_id / cluster_id is set; the CHECK enforces
        // that. Neither is FK'd because both the vm row and the
        // cluster row are inserted *after* their IPs are allocated
        // (allocation produces the IP the row stores). Release is
        // explicit via `release_vm_ips` / `release_cluster_ips`.
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS ip_allocations (
                ip_address TEXT PRIMARY KEY,
                scope TEXT NOT NULL,
                vm_id TEXT,
                cluster_id TEXT,
                CHECK ((vm_id IS NULL) != (cluster_id IS NULL))
            )",
        )
        .execute(&self.writer)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_ip_allocations_scope \
             ON ip_allocations(scope)",
        )
        .execute(&self.writer)
        .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_ip_allocations_vm ON ip_allocations(vm_id)")
            .execute(&self.writer)
            .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_ip_allocations_cluster ON ip_allocations(cluster_id)",
        )
        .execute(&self.writer)
        .await?;

        Ok(())
    }

    // --- Trees ---

    /// Atomically allocate a new tree: pick the next free VNI and carve
    /// the next free sub-CIDR out of `net.tree_supernet`.
    pub async fn allocate_tree(&self, net: &NetworkConfig) -> Result<TreeRow, DbError> {
        let mut tx = self.writer.begin().await?;

        let taken: Vec<(i64, String)> = sqlx::query_as("SELECT vni, cidr FROM trees")
            .fetch_all(&mut *tx)
            .await?;
        let used_vnis: HashSet<u32> = taken.iter().map(|(v, _)| *v as u32).collect();
        let mut used_cidrs: Vec<ipnet::Ipv4Net> = Vec::with_capacity(taken.len());
        for (_, c) in &taken {
            used_cidrs.push(
                c.parse()
                    .map_err(|e| DbError::Malformed(format!("trees.cidr '{c}': {e}")))?,
            );
        }

        let vni = (net.vni_range.start..=net.vni_range.end)
            .find(|v| !used_vnis.contains(v))
            .ok_or_else(|| {
                DbError::Exhausted(format!(
                    "VNI range [{}, {}] fully allocated",
                    net.vni_range.start, net.vni_range.end
                ))
            })?;

        let supernet: ipnet::Ipv4Net = net
            .tree_supernet
            .parse()
            .map_err(|e| DbError::Malformed(format!("tree_supernet: {e}")))?;
        let candidate = supernet
            .subnets(net.tree_prefix)
            .map_err(|e| DbError::Malformed(format!("tree_prefix: {e}")))?
            .find(|c| !used_cidrs.iter().any(|u| cidrs_overlap(u, c)))
            .ok_or_else(|| {
                DbError::Exhausted(format!(
                    "tree supernet {} fully carved into /{} slices",
                    net.tree_supernet, net.tree_prefix
                ))
            })?;

        let layout = TreeLayout::carve(&candidate, net.bridge_reserve, net.vip_reserve);

        let id = uuid::Uuid::new_v4().to_string();
        let created_at = basis_common::time::now_rfc3339();
        sqlx::query(
            "INSERT INTO trees (
                id, vni, cidr,
                bridge_range_start, bridge_range_end,
                vm_range_start, vm_range_end,
                vip_range_start, vip_range_end,
                prefix_len,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&id)
        .bind(vni as i64)
        .bind(candidate.to_string())
        .bind(layout.bridge_start.to_string())
        .bind(layout.bridge_end.to_string())
        .bind(layout.vm_start.to_string())
        .bind(layout.vm_end.to_string())
        .bind(layout.vip_start.to_string())
        .bind(layout.vip_end.to_string())
        .bind(net.tree_prefix as i64)
        .bind(&created_at)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(TreeRow {
            id,
            vni: vni as i64,
            cidr: candidate.to_string(),
            bridge_range_start: layout.bridge_start.to_string(),
            bridge_range_end: layout.bridge_end.to_string(),
            vm_range_start: layout.vm_start.to_string(),
            vm_range_end: layout.vm_end.to_string(),
            vip_range_start: layout.vip_start.to_string(),
            vip_range_end: layout.vip_end.to_string(),
            prefix_len: net.tree_prefix as i64,
            created_at,
        })
    }

    pub async fn get_tree(&self, id: &str) -> Result<TreeRow, DbError> {
        sqlx::query_as::<_, TreeRow>("SELECT * FROM trees WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.reader)
            .await?
            .ok_or_else(|| DbError::NotFound(format!("tree '{id}'")))
    }

    pub async fn delete_tree(&self, id: &str) -> Result<(), DbError> {
        let mut tx = self.writer.begin().await?;
        sqlx::query("DELETE FROM tree_host_bridges WHERE tree_id = ?")
            .bind(id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM trees WHERE id = ?")
            .bind(id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    // --- Per-host bridge IPs ---

    /// Find-or-allocate the bridge IP this host uses for VMs in `tree`.
    /// Idempotent: repeat calls for the same (tree, host) return the
    /// same IP. On first call for the pair, picks the lowest free
    /// address in the tree's `bridge_range` and inserts the mapping.
    pub async fn ensure_host_bridge_ip(
        &self,
        tree: &TreeRow,
        host_id: &str,
    ) -> Result<String, DbError> {
        let range = tree.bridge_range()?;
        let mut tx = self.writer.begin().await?;

        if let Some((ip,)) = sqlx::query_as::<_, (String,)>(
            "SELECT ip_address FROM tree_host_bridges \
             WHERE tree_id = ? AND host_id = ?",
        )
        .bind(&tree.id)
        .bind(host_id)
        .fetch_optional(&mut *tx)
        .await?
        {
            tx.commit().await?;
            return Ok(ip);
        }

        let allocated: Option<String> = sqlx::query_scalar(
            r#"
            WITH RECURSIVE
              candidate(n) AS (
                  SELECT ? UNION ALL SELECT n + 1 FROM candidate WHERE n < ?
              ),
              picked(ip) AS (
                  SELECT printf('%d.%d.%d.%d',
                                (n >> 24) & 255, (n >> 16) & 255,
                                (n >>  8) & 255,  n        & 255)
                  FROM candidate
                  WHERE printf('%d.%d.%d.%d',
                               (n >> 24) & 255, (n >> 16) & 255,
                               (n >>  8) & 255,  n        & 255)
                        NOT IN (SELECT ip_address FROM tree_host_bridges
                                WHERE tree_id = ?)
                  ORDER BY n
                  LIMIT 1
              )
            INSERT INTO tree_host_bridges (tree_id, host_id, ip_address)
            SELECT ?, ?, ip FROM picked
            RETURNING ip_address
            "#,
        )
        .bind(range.start as i64)
        .bind(range.end as i64)
        .bind(&tree.id)
        .bind(&tree.id)
        .bind(host_id)
        .fetch_optional(&mut *tx)
        .await?;

        tx.commit().await?;

        allocated.ok_or_else(|| {
            DbError::Exhausted(format!(
                "tree {} bridge_range [{}..={}] fully allocated",
                tree.id,
                Ipv4Addr::from(range.start),
                Ipv4Addr::from(range.end),
            ))
        })
    }

    /// Bridge IP this host uses for the given tree, if any. Called by
    /// `build_reconcile_command` — a host should always have a bridge
    /// IP for every tree it carries a VM in, but the lookup is tolerant
    /// of the brief window between a VM delete and the mapping release.
    pub async fn get_host_bridge_ip(
        &self,
        tree_id: &str,
        host_id: &str,
    ) -> Result<Option<String>, DbError> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT ip_address FROM tree_host_bridges \
             WHERE tree_id = ? AND host_id = ?",
        )
        .bind(tree_id)
        .bind(host_id)
        .fetch_optional(&self.reader)
        .await?)
    }

    /// Release the bridge IP for (tree, host) iff no VMs remain on that
    /// host in that tree. Caller invokes this after every VM delete;
    /// on the last VM for a (host, tree) the bridge mapping drops and
    /// the address is available for reuse elsewhere in the tree.
    pub async fn release_host_bridge_ip_if_idle(
        &self,
        tree_id: &str,
        host_id: &str,
    ) -> Result<(), DbError> {
        sqlx::query(
            "DELETE FROM tree_host_bridges \
             WHERE tree_id = ? AND host_id = ? \
               AND NOT EXISTS (
                   SELECT 1 FROM vms v
                   JOIN clusters c ON v.cluster_id = c.id
                   WHERE v.host_id = ? AND c.tree_id = ?
               )",
        )
        .bind(tree_id)
        .bind(host_id)
        .bind(host_id)
        .bind(tree_id)
        .execute(&self.writer)
        .await?;
        Ok(())
    }

    // --- Host↔tree queries (derived from vms JOIN clusters) ---

    /// VTEP addresses of every host currently carrying a VM in this
    /// tree. Drives the agent-side FDB reconcile.
    pub async fn list_tree_vteps(&self, tree_id: &str) -> Result<Vec<String>, DbError> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT DISTINCT h.vtep_address
             FROM vms v
             JOIN clusters c ON v.cluster_id = c.id
             JOIN hosts h ON v.host_id = h.id
             WHERE c.tree_id = ? AND h.vtep_address != ''",
        )
        .bind(tree_id)
        .fetch_all(&self.reader)
        .await?;
        Ok(rows.into_iter().map(|(v,)| v).collect())
    }

    /// Every tree this host currently carries.
    pub async fn list_host_trees(&self, host_id: &str) -> Result<Vec<TreeRow>, DbError> {
        Ok(sqlx::query_as::<_, TreeRow>(
            "SELECT DISTINCT t.* FROM trees t
             JOIN clusters c ON c.tree_id = t.id
             JOIN vms v ON v.cluster_id = c.id
             WHERE v.host_id = ?",
        )
        .bind(host_id)
        .fetch_all(&self.reader)
        .await?)
    }

    /// Host IDs currently carrying VMs in this tree.
    pub async fn list_hosts_in_tree(&self, tree_id: &str) -> Result<Vec<String>, DbError> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT DISTINCT v.host_id
             FROM vms v
             JOIN clusters c ON v.cluster_id = c.id
             WHERE c.tree_id = ?",
        )
        .bind(tree_id)
        .fetch_all(&self.reader)
        .await?;
        Ok(rows.into_iter().map(|(h,)| h).collect())
    }

    // --- IP allocation ---

    /// Allocate the next free tree-side IP for a VM.
    pub async fn allocate_tree_vm_ip(
        &self,
        tree: &TreeRow,
        vm_id: &str,
    ) -> Result<String, DbError> {
        let range = tree.vm_range()?;
        self.allocate_from_range(&tree.id, &range, Some(vm_id), None)
            .await
    }

    /// Allocate the next free tree-side VIP for a cluster. Used for
    /// nested clusters whose apiserver VIP stays inside the tree
    /// overlay; external callers reach it through a parent-cell
    /// auth proxy.
    pub async fn allocate_tree_vip(
        &self,
        tree: &TreeRow,
        cluster_id: &str,
    ) -> Result<String, DbError> {
        let range = tree.vip_range()?;
        self.allocate_from_range(&tree.id, &range, None, Some(cluster_id))
            .await
    }

    /// Allocate the next free address from a named pool for a
    /// cluster's apiserver VIP. Pool /32s are advertised by the host
    /// running the VIP-claiming VM via the cell's BGP reflector.
    pub async fn allocate_pool_vip(
        &self,
        pool: &Pool,
        cluster_id: &str,
    ) -> Result<String, DbError> {
        let range = ParsedRange::parse_pool_range(pool)?;
        self.allocate_from_range(&pool.name, &range, None, Some(cluster_id))
            .await
    }

    async fn allocate_from_range(
        &self,
        scope: &str,
        range: &ParsedRange,
        vm_id: Option<&str>,
        cluster_id: Option<&str>,
    ) -> Result<String, DbError> {
        debug_assert!(
            vm_id.is_some() != cluster_id.is_some(),
            "exactly one of vm_id / cluster_id must be set",
        );
        let allocated: Option<String> = sqlx::query_scalar(
            r#"
            WITH RECURSIVE
              candidate(n) AS (
                  SELECT ? UNION ALL SELECT n + 1 FROM candidate WHERE n < ?
              ),
              picked(ip) AS (
                  SELECT printf('%d.%d.%d.%d',
                                (n >> 24) & 255, (n >> 16) & 255,
                                (n >>  8) & 255,  n        & 255)
                  FROM candidate
                  WHERE printf('%d.%d.%d.%d',
                               (n >> 24) & 255, (n >> 16) & 255,
                               (n >>  8) & 255,  n        & 255)
                        NOT IN (SELECT ip_address FROM ip_allocations
                                WHERE scope = ?)
                  ORDER BY n
                  LIMIT 1
              )
            INSERT INTO ip_allocations (ip_address, scope, vm_id, cluster_id)
            SELECT ip, ?, ?, ? FROM picked
            RETURNING ip_address
            "#,
        )
        .bind(range.start as i64)
        .bind(range.end as i64)
        .bind(scope)
        .bind(scope)
        .bind(vm_id)
        .bind(cluster_id)
        .fetch_optional(&self.writer)
        .await?;

        allocated.ok_or_else(|| {
            DbError::Exhausted(format!(
                "no available IPs in scope '{scope}' sub-range [{}..={}]",
                Ipv4Addr::from(range.start),
                Ipv4Addr::from(range.end),
            ))
        })
    }

    /// Release a VM's tree-side IP. Called on `delete_vm` and on
    /// rollback when `insert_vm` loses a capacity race.
    pub async fn release_vm_ips(&self, vm_id: &str) -> Result<(), DbError> {
        sqlx::query("DELETE FROM ip_allocations WHERE vm_id = ?")
            .bind(vm_id)
            .execute(&self.writer)
            .await?;
        Ok(())
    }

    /// Release every IP held by a cluster (apiserver VIP). Called on
    /// `DeleteCluster` and on rollback when `insert_cluster` or a
    /// subsequent allocation fails. Pool slices and pod CIDRs cascade
    /// automatically via their FKs on cluster delete.
    pub async fn release_cluster_ips(&self, cluster_id: &str) -> Result<(), DbError> {
        sqlx::query("DELETE FROM ip_allocations WHERE cluster_id = ?")
            .bind(cluster_id)
            .execute(&self.writer)
            .await?;
        Ok(())
    }

    // --- Clusters ---

    pub async fn insert_cluster(&self, cluster: &ClusterRow) -> Result<(), DbError> {
        sqlx::query(
            "INSERT INTO clusters (id, name, tree_id, parent_cluster_id, control_plane_endpoint, apiserver_pool, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&cluster.id)
        .bind(&cluster.name)
        .bind(&cluster.tree_id)
        .bind(&cluster.parent_cluster_id)
        .bind(&cluster.control_plane_endpoint)
        .bind(&cluster.apiserver_pool)
        .bind(&cluster.created_at)
        .execute(&self.writer)
        .await
        .map_err(|e| match e {
            sqlx::Error::Database(db_err) if db_err.is_unique_violation() => {
                DbError::Conflict(format!("cluster '{}' already exists", cluster.name))
            }
            other => DbError::Sqlx(other),
        })?;
        Ok(())
    }

    pub async fn get_cluster(&self, id: &str) -> Result<ClusterRow, DbError> {
        sqlx::query_as::<_, ClusterRow>("SELECT * FROM clusters WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.reader)
            .await?
            .ok_or_else(|| DbError::NotFound(format!("cluster '{id}'")))
    }

    pub async fn get_cluster_by_name(&self, name: &str) -> Result<Option<ClusterRow>, DbError> {
        Ok(
            sqlx::query_as::<_, ClusterRow>("SELECT * FROM clusters WHERE name = ?")
                .bind(name)
                .fetch_optional(&self.reader)
                .await?,
        )
    }

    pub async fn delete_cluster(&self, id: &str) -> Result<(), DbError> {
        sqlx::query("DELETE FROM clusters WHERE id = ?")
            .bind(id)
            .execute(&self.writer)
            .await?;
        Ok(())
    }

    pub async fn list_clusters(&self) -> Result<Vec<ClusterRow>, DbError> {
        Ok(sqlx::query_as::<_, ClusterRow>("SELECT * FROM clusters")
            .fetch_all(&self.reader)
            .await?)
    }

    /// Direct children of a cluster. Used by `DeleteCluster` to refuse
    /// the delete if the cluster has live descendants.
    pub async fn list_child_clusters(&self, parent_id: &str) -> Result<Vec<ClusterRow>, DbError> {
        Ok(
            sqlx::query_as::<_, ClusterRow>("SELECT * FROM clusters WHERE parent_cluster_id = ?")
                .bind(parent_id)
                .fetch_all(&self.reader)
                .await?,
        )
    }

    /// Number of clusters still attached to a tree.
    pub async fn count_clusters_in_tree(&self, tree_id: &str) -> Result<i64, DbError> {
        let (n,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM clusters WHERE tree_id = ?")
            .bind(tree_id)
            .fetch_one(&self.reader)
            .await?;
        Ok(n)
    }

    // --- Hosts ---

    pub async fn upsert_host(&self, host: &HostRow) -> Result<(), DbError> {
        sqlx::query(
            "INSERT INTO hosts (id, hostname, total_cpu, total_memory_mib, total_disk_gib,
                gpu_inventory, vtep_address, last_heartbeat, healthy)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(id) DO UPDATE SET
                hostname = excluded.hostname,
                total_cpu = excluded.total_cpu,
                total_memory_mib = excluded.total_memory_mib,
                total_disk_gib = excluded.total_disk_gib,
                gpu_inventory = excluded.gpu_inventory,
                vtep_address = excluded.vtep_address,
                last_heartbeat = excluded.last_heartbeat,
                healthy = excluded.healthy",
        )
        .bind(&host.id)
        .bind(&host.hostname)
        .bind(host.total_cpu)
        .bind(host.total_memory_mib)
        .bind(host.total_disk_gib)
        .bind(&host.gpu_inventory)
        .bind(&host.vtep_address)
        .bind(&host.last_heartbeat)
        .bind(host.healthy)
        .execute(&self.writer)
        .await?;
        Ok(())
    }

    pub async fn get_host(&self, id: &str) -> Result<HostRow, DbError> {
        sqlx::query_as::<_, HostRow>("SELECT * FROM hosts WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.reader)
            .await?
            .ok_or_else(|| DbError::NotFound(format!("host '{id}'")))
    }

    pub async fn get_host_by_hostname(&self, hostname: &str) -> Result<Option<HostRow>, DbError> {
        Ok(
            sqlx::query_as::<_, HostRow>("SELECT * FROM hosts WHERE hostname = ?")
                .bind(hostname)
                .fetch_optional(&self.reader)
                .await?,
        )
    }

    pub async fn list_healthy_hosts(&self) -> Result<Vec<HostRow>, DbError> {
        Ok(
            sqlx::query_as::<_, HostRow>("SELECT * FROM hosts WHERE healthy = 1")
                .fetch_all(&self.reader)
                .await?,
        )
    }

    pub async fn list_hosts(&self) -> Result<Vec<HostRow>, DbError> {
        Ok(sqlx::query_as::<_, HostRow>("SELECT * FROM hosts")
            .fetch_all(&self.reader)
            .await?)
    }

    pub async fn update_host_heartbeat(&self, host_id: &str, now: &str) -> Result<(), DbError> {
        let result = sqlx::query("UPDATE hosts SET last_heartbeat = ?, healthy = 1 WHERE id = ?")
            .bind(now)
            .bind(host_id)
            .execute(&self.writer)
            .await?;

        if result.rows_affected() == 0 {
            return Err(DbError::NotFound(format!("host '{host_id}'")));
        }
        Ok(())
    }

    pub async fn mark_host_unhealthy(&self, host_id: &str) -> Result<(), DbError> {
        sqlx::query("UPDATE hosts SET healthy = 0 WHERE id = ?")
            .bind(host_id)
            .execute(&self.writer)
            .await?;
        Ok(())
    }

    // --- VMs ---

    /// Insert a scheduled VM row together with its GPU reservations,
    /// atomically enforcing that the chosen host still has free
    /// capacity (cpu scaled by `cpu_overcommit_ratio`; memory and disk
    /// strict) and that none of `gpus` is already claimed on the host.
    ///
    /// This is the commit step of the scheduler's optimistic-concurrency
    /// protocol: `pick_host` reads a snapshot from the reader pool and
    /// decides on placement; `insert_vm` re-validates that snapshot
    /// against the writer's current state in a single transaction:
    ///
    /// - Capacity races show up as a zero-row insert into `vms`
    ///   (the `WHERE EXISTS` gate rejected the placement).
    /// - GPU races show up as a `UNIQUE (host_id, pci_address)`
    ///   violation on `vm_gpus`.
    ///
    /// Either way we roll back and return [`DbError::CapacityRaced`];
    /// [`DbError::HostUnavailable`] is reserved for the host actually
    /// being gone or unhealthy, which is a different retry policy
    /// (pick another host) from a capacity race (re-snapshot and retry
    /// the same scheduler pass, which may land here again).
    pub async fn insert_vm(&self, vm: &VmRow, gpus: &[GpuAssignment]) -> Result<(), DbError> {
        let mut tx = self.writer.begin().await?;

        let affected = sqlx::query(
            "INSERT INTO vms (
                id, name, cluster_id, host_id, ip_address, state,
                cpu, memory_mib, disk_gib, extra_disk_total_gib,
                extra_disk_gibs, image, error_message,
                created_at, updated_at)
             SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
             WHERE EXISTS (
                 SELECT 1 FROM hosts h
                 WHERE h.id = ? AND h.healthy = 1
                   AND CAST(h.total_cpu * ? AS INTEGER)
                       - COALESCE((SELECT SUM(cpu) FROM vms
                                   WHERE host_id = h.id), 0) >= ?
                   AND h.total_memory_mib
                       - COALESCE((SELECT SUM(memory_mib) FROM vms
                                   WHERE host_id = h.id), 0) >= ?
                   AND h.total_disk_gib
                       - COALESCE((SELECT SUM(disk_gib + extra_disk_total_gib) FROM vms
                                   WHERE host_id = h.id), 0) >= ?
             )",
        )
        .bind(&vm.id)
        .bind(&vm.name)
        .bind(&vm.cluster_id)
        .bind(&vm.host_id)
        .bind(&vm.ip_address)
        .bind(vm.state)
        .bind(vm.cpu)
        .bind(vm.memory_mib)
        .bind(vm.disk_gib)
        .bind(vm.extra_disk_total_gib)
        .bind(&vm.extra_disk_gibs)
        .bind(&vm.image)
        .bind(&vm.error_message)
        .bind(&vm.created_at)
        .bind(&vm.updated_at)
        .bind(&vm.host_id)
        .bind(self.cpu_overcommit_ratio as f64)
        .bind(vm.cpu)
        .bind(vm.memory_mib)
        .bind(vm.total_disk_gib())
        .execute(&mut *tx)
        .await
        .map_err(|e| match e {
            sqlx::Error::Database(db_err) if db_err.is_unique_violation() => {
                DbError::Conflict(format!(
                    "vm '{}' already exists in cluster '{}'",
                    vm.name, vm.cluster_id
                ))
            }
            other => DbError::Sqlx(other),
        })?;

        if affected.rows_affected() == 0 {
            // Capacity gate rejected us. Tell "host is gone" apart from
            // "host is there but someone else took the room" with one
            // cheap follow-up read inside the same txn — sequenced
            // after the failed insert, so the state we see is at least
            // as fresh as the state that rejected us.
            let still_healthy: bool = sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM hosts WHERE id = ? AND healthy = 1",
            )
            .bind(&vm.host_id)
            .fetch_one(&mut *tx)
            .await
            .map(|n| n > 0)
            .unwrap_or(false);
            return if still_healthy {
                Err(DbError::CapacityRaced(vm.host_id.clone()))
            } else {
                Err(DbError::HostUnavailable(vm.host_id.clone()))
            };
        }

        for g in gpus {
            sqlx::query(
                "INSERT INTO vm_gpus (vm_id, host_id, pci_address, model, iommu_group, nvlink_group) \
                 VALUES (?, ?, ?, ?, ?, ?)",
            )
            .bind(&g.vm_id)
            .bind(&g.host_id)
            .bind(&g.pci_address)
            .bind(&g.model)
            .bind(&g.iommu_group)
            .bind(g.nvlink_group)
            .execute(&mut *tx)
            .await
            .map_err(|e| match e {
                sqlx::Error::Database(db_err) if db_err.is_unique_violation() => {
                    DbError::CapacityRaced(vm.host_id.clone())
                }
                other => DbError::Sqlx(other),
            })?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn get_vm(&self, id: &str) -> Result<VmRow, DbError> {
        sqlx::query_as::<_, VmRow>("SELECT * FROM vms WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.reader)
            .await?
            .ok_or_else(|| DbError::NotFound(format!("vm '{id}'")))
    }

    pub async fn get_vm_by_name(
        &self,
        cluster_id: &str,
        name: &str,
    ) -> Result<Option<VmRow>, DbError> {
        Ok(
            sqlx::query_as::<_, VmRow>("SELECT * FROM vms WHERE cluster_id = ? AND name = ?")
                .bind(cluster_id)
                .bind(name)
                .fetch_optional(&self.reader)
                .await?,
        )
    }

    pub async fn list_vms(&self, cluster_id: Option<&str>) -> Result<Vec<VmRow>, DbError> {
        match cluster_id {
            Some(c) => Ok(
                sqlx::query_as::<_, VmRow>("SELECT * FROM vms WHERE cluster_id = ?")
                    .bind(c)
                    .fetch_all(&self.reader)
                    .await?,
            ),
            None => Ok(sqlx::query_as::<_, VmRow>("SELECT * FROM vms")
                .fetch_all(&self.reader)
                .await?),
        }
    }

    /// IDs of VMs assigned to a host — the full payload the reconcile
    /// builder needs. Pulls only the `id` column, not whole `VmRow`s.
    pub async fn list_vm_ids_on_host(&self, host_id: &str) -> Result<Vec<String>, DbError> {
        Ok(sqlx::query_scalar("SELECT id FROM vms WHERE host_id = ?")
            .bind(host_id)
            .fetch_all(&self.reader)
            .await?)
    }

    /// Point-in-time, per-host view of everything the scheduler cares
    /// about: consumed cpu / memory / disk, and the set of GPU PCI
    /// addresses already claimed. Read from the reader pool in a
    /// handful of queries; the writer's `insert_vm` re-validates at
    /// commit time so a stale snapshot here never over-places.
    pub async fn host_usage_snapshot(&self) -> Result<HashMap<String, HostUsage>, DbError> {
        let mut out: HashMap<String, HostUsage> = HashMap::new();

        // Consumed cpu / mem / disk per host. COALESCE covers hosts with
        // zero VMs by leaving them absent from this result; the caller
        // fills in `HostUsage::default()` when a host isn't in the map.
        let rows = sqlx::query_as::<_, (String, i64, i64, i64)>(
            "SELECT host_id,
                    COALESCE(SUM(cpu), 0),
                    COALESCE(SUM(memory_mib), 0),
                    COALESCE(SUM(disk_gib + extra_disk_total_gib), 0)
             FROM vms GROUP BY host_id",
        )
        .fetch_all(&self.reader)
        .await?;
        for (host_id, cpu, mem, disk) in rows {
            out.insert(
                host_id,
                HostUsage {
                    used_cpu: cpu,
                    used_memory_mib: mem,
                    used_disk_gib: disk,
                    assigned_pci: HashSet::new(),
                },
            );
        }

        // Claimed PCI addresses, joined in by host.
        let gpus =
            sqlx::query_as::<_, (String, String)>("SELECT host_id, pci_address FROM vm_gpus")
                .fetch_all(&self.reader)
                .await?;
        for (host_id, pci) in gpus {
            out.entry(host_id).or_default().assigned_pci.insert(pci);
        }

        Ok(out)
    }

    /// All GPU assignments for a single VM. Empty when the VM has none.
    pub async fn gpus_for_vm(&self, vm_id: &str) -> Result<Vec<GpuAssignment>, DbError> {
        Ok(
            sqlx::query_as::<_, GpuAssignment>("SELECT * FROM vm_gpus WHERE vm_id = ?")
                .bind(vm_id)
                .fetch_all(&self.reader)
                .await?,
        )
    }

    /// Bulk-fetch GPU assignments for many VMs in a single query. Every
    /// input `vm_id` appears in the map — VMs without GPUs map to an
    /// empty vec — so callers can index without a default.
    pub async fn gpus_for_vms(
        &self,
        vm_ids: &[String],
    ) -> Result<HashMap<String, Vec<GpuAssignment>>, DbError> {
        let mut out: HashMap<String, Vec<GpuAssignment>> =
            vm_ids.iter().map(|id| (id.clone(), Vec::new())).collect();
        if vm_ids.is_empty() {
            return Ok(out);
        }
        let placeholders = vec!["?"; vm_ids.len()].join(",");
        let sql = format!("SELECT * FROM vm_gpus WHERE vm_id IN ({placeholders})");
        let mut q = sqlx::query_as::<_, GpuAssignment>(&sql);
        for id in vm_ids {
            q = q.bind(id);
        }
        for g in q.fetch_all(&self.reader).await? {
            out.entry(g.vm_id.clone()).or_default().push(g);
        }
        Ok(out)
    }

    pub async fn update_vm_state(
        &self,
        vm_id: &str,
        state: i64,
        error_message: &str,
        now: &str,
    ) -> Result<(), DbError> {
        let result =
            sqlx::query("UPDATE vms SET state = ?, error_message = ?, updated_at = ? WHERE id = ?")
                .bind(state)
                .bind(error_message)
                .bind(now)
                .bind(vm_id)
                .execute(&self.writer)
                .await?;

        if result.rows_affected() == 0 {
            return Err(DbError::NotFound(format!("vm '{vm_id}'")));
        }
        Ok(())
    }

    pub async fn delete_vm(&self, id: &str) -> Result<(), DbError> {
        sqlx::query("DELETE FROM vms WHERE id = ?")
            .bind(id)
            .execute(&self.writer)
            .await?;
        Ok(())
    }

    pub async fn mark_stale_hosts_unhealthy(&self, cutoff: &str) -> Result<Vec<String>, DbError> {
        let stale: Vec<String> =
            sqlx::query_scalar("SELECT id FROM hosts WHERE healthy = 1 AND last_heartbeat < ?")
                .bind(cutoff)
                .fetch_all(&self.reader)
                .await?;

        if !stale.is_empty() {
            sqlx::query("UPDATE hosts SET healthy = 0 WHERE healthy = 1 AND last_heartbeat < ?")
                .bind(cutoff)
                .execute(&self.writer)
                .await?;
        }

        Ok(stale)
    }

    pub async fn mark_vms_failed_on_host(
        &self,
        host_id: &str,
        reason: &str,
    ) -> Result<u64, DbError> {
        let now = basis_common::time::now_rfc3339();
        let result = sqlx::query(
            "UPDATE vms \
             SET state = ?, error_message = ?, updated_at = ? \
             WHERE host_id = ? AND state NOT IN (?, ?)",
        )
        .bind(basis_proto::MachineState::Failed as i64)
        .bind(reason)
        .bind(&now)
        .bind(host_id)
        .bind(basis_proto::MachineState::Stopped as i64)
        .bind(basis_proto::MachineState::Failed as i64)
        .execute(&self.writer)
        .await?;
        Ok(result.rows_affected())
    }
}

fn cidrs_overlap(a: &ipnet::Ipv4Net, b: &ipnet::Ipv4Net) -> bool {
    // Two /N networks overlap iff one contains the other's network
    // address. For equal-prefix slices this reduces to equality, which
    // is what we hit in tree carving.
    a.contains(&b.network()) || b.contains(&a.network())
}

/// Layout of a single tree's CIDR. Bottom `bridge_reserve` addresses
/// hold per-host gateway IPs; top `vip_reserve` addresses hold
/// tree-internal cluster VIPs (for nested clusters whose apiserver
/// stays inside the tree); everything between is the VM range.
/// Config validation guarantees every region has positive width.
struct TreeLayout {
    bridge_start: Ipv4Addr,
    bridge_end: Ipv4Addr,
    vm_start: Ipv4Addr,
    vm_end: Ipv4Addr,
    vip_start: Ipv4Addr,
    vip_end: Ipv4Addr,
}

impl TreeLayout {
    fn carve(cidr: &ipnet::Ipv4Net, bridge_reserve: u32, vip_reserve: u32) -> Self {
        let net = u32::from(cidr.network());
        let bcast = u32::from(cidr.broadcast());
        let bridge_start = net + 1;
        let bridge_end = net + bridge_reserve;
        let vm_start = bridge_end + 1;
        let vip_end = bcast - 1;
        let vip_start = vip_end - (vip_reserve - 1);
        let vm_end = vip_start - 1;
        Self {
            bridge_start: Ipv4Addr::from(bridge_start),
            bridge_end: Ipv4Addr::from(bridge_end),
            vm_start: Ipv4Addr::from(vm_start),
            vm_end: Ipv4Addr::from(vm_end),
            vip_start: Ipv4Addr::from(vip_start),
            vip_end: Ipv4Addr::from(vip_end),
        }
    }
}

// --- Row types ---

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct HostRow {
    pub id: String,
    pub hostname: String,
    pub total_cpu: i64,
    pub total_memory_mib: i64,
    pub total_disk_gib: i64,
    pub gpu_inventory: String,
    /// IP address the agent uses as the VXLAN src for outgoing tunneled
    /// frames. Reported on `RegisterHostRequest`; empty string means
    /// the agent is pre-VXLAN and cross-host traffic for any tree it
    /// hosts won't reach its peers.
    pub vtep_address: String,
    pub last_heartbeat: String,
    pub healthy: bool,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct VmRow {
    pub id: String,
    pub name: String,
    pub cluster_id: String,
    pub host_id: String,
    pub ip_address: String,
    pub state: i64,
    pub cpu: i64,
    pub memory_mib: i64,
    pub disk_gib: i64,
    /// Sum of `extra_disk_gibs`. Denormalized from the JSON blob so the
    /// capacity-gate SQL in `insert_vm` can `SUM(disk_gib +
    /// extra_disk_total_gib)` without a `json_each` call per row.
    /// `insert_vm` is the only writer; the two stay in sync by
    /// construction.
    pub extra_disk_total_gib: i64,
    /// Per-extra-disk sizes, JSON-encoded `Vec<u32>` of gibibytes. The
    /// authoritative per-disk breakdown the agent consumes on reconcile
    /// to decide how many LVs to carve; `extra_disk_total_gib` is its
    /// sum.
    pub extra_disk_gibs: String,
    pub image: String,
    pub error_message: String,
    pub created_at: String,
    pub updated_at: String,
}

impl VmRow {
    pub fn total_disk_gib(&self) -> i64 {
        self.disk_gib + self.extra_disk_total_gib
    }

    pub fn extra_disks(&self) -> Vec<u32> {
        basis_common::json::parse_owned_json(&self.extra_disk_gibs, "vms.extra_disk_gibs")
    }
}

/// One row of `vm_gpus` — a GPU-to-VM reservation on a specific host.
/// Every assignment lives here; there is no JSON-encoded duplicate on
/// `vms`. Callers that need the GPUs for a VM fetch them via
/// `Db::gpus_for_vm` / `Db::gpus_for_vms`.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct GpuAssignment {
    pub vm_id: String,
    pub host_id: String,
    pub pci_address: String,
    pub model: String,
    pub iommu_group: String,
    pub nvlink_group: i64,
}

impl GpuAssignment {
    /// Build a reservation from the scheduler's `GpuInfo` pick. Kept as
    /// an explicit conversion so the field mapping lives in one place.
    pub fn from_scheduler_pick(
        vm_id: &str,
        host_id: &str,
        info: &basis_common::gpu::GpuInfo,
    ) -> Self {
        Self {
            vm_id: vm_id.to_string(),
            host_id: host_id.to_string(),
            pci_address: info.pci_address.clone(),
            model: info.model.clone(),
            iommu_group: info.iommu_group.clone(),
            nvlink_group: info.nvlink_group as i64,
        }
    }

    pub fn to_proto(&self) -> basis_proto::GpuDevice {
        basis_proto::GpuDevice {
            pci_address: self.pci_address.clone(),
            model: self.model.clone(),
            iommu_group: self.iommu_group.clone(),
            nvlink_group: self.nvlink_group as u32,
        }
    }
}

/// Snapshot of one host's in-use capacity, derived from `vms` + `vm_gpus`.
/// This is all the scheduler needs — it never sees raw `VmRow`s, and
/// never parses a GPU JSON blob (because there isn't one).
#[derive(Debug, Clone, Default)]
pub struct HostUsage {
    pub used_cpu: i64,
    pub used_memory_mib: i64,
    pub used_disk_gib: i64,
    pub assigned_pci: HashSet<String>,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ClusterRow {
    pub id: String,
    pub name: String,
    pub tree_id: String,
    pub parent_cluster_id: Option<String>,
    pub control_plane_endpoint: String,
    /// Pool name the apiserver VIP was carved from at CreateCluster.
    /// Empty string means the VIP came from the tree's `vip_range`.
    pub apiserver_pool: String,
    pub created_at: String,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct TreeRow {
    pub id: String,
    pub vni: i64,
    pub cidr: String,
    pub bridge_range_start: String,
    pub bridge_range_end: String,
    pub vm_range_start: String,
    pub vm_range_end: String,
    pub vip_range_start: String,
    pub vip_range_end: String,
    pub prefix_len: i64,
    pub created_at: String,
}

impl TreeRow {
    pub fn bridge_range(&self) -> Result<ParsedRange, DbError> {
        ParsedRange::parse(
            &self.bridge_range_start,
            &self.bridge_range_end,
            &self.id,
            "bridge",
        )
    }

    pub fn vm_range(&self) -> Result<ParsedRange, DbError> {
        ParsedRange::parse(&self.vm_range_start, &self.vm_range_end, &self.id, "vm")
    }

    pub fn vip_range(&self) -> Result<ParsedRange, DbError> {
        ParsedRange::parse(&self.vip_range_start, &self.vip_range_end, &self.id, "vip")
    }
}

/// Inclusive IPv4 range expressed as host-order `u32`s.
#[derive(Debug, Clone, Copy)]
pub struct ParsedRange {
    pub start: u32,
    pub end: u32,
}

impl ParsedRange {
    fn parse(start: &str, end: &str, tree_id: &str, kind: &str) -> Result<Self, DbError> {
        let s: Ipv4Addr = start.parse().map_err(|e| {
            DbError::Malformed(format!("tree {tree_id} {kind}_range_start '{start}': {e}"))
        })?;
        let e: Ipv4Addr = end.parse().map_err(|e| {
            DbError::Malformed(format!("tree {tree_id} {kind}_range_end '{end}': {e}"))
        })?;
        Ok(Self {
            start: u32::from(s),
            end: u32::from(e),
        })
    }

    fn parse_pool_range(pool: &Pool) -> Result<Self, DbError> {
        let start: Ipv4Addr = pool
            .range_start
            .parse()
            .map_err(|e| DbError::Malformed(format!("pool '{}' range_start: {e}", pool.name)))?;
        let end: Ipv4Addr = pool
            .range_end
            .parse()
            .map_err(|e| DbError::Malformed(format!("pool '{}' range_end: {e}", pool.name)))?;
        Ok(Self {
            start: u32::from(start),
            end: u32::from(end),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{NetworkConfig, Pool, VniRange};

    async fn test_db() -> Db {
        Db::open(":memory:".as_ref(), 1.0).await.unwrap()
    }

    fn make_net_config() -> NetworkConfig {
        NetworkConfig {
            tree_supernet: "10.0.0.0/8".to_string(),
            tree_prefix: 20,
            bridge_reserve: 32,
            vip_reserve: 16,
            vni_range: VniRange {
                start: 10_000,
                end: 10_010,
            },
            pools: vec![Pool {
                name: "cell-internal".to_string(),
                cidr: "192.168.100.0/24".to_string(),
                gateway: "192.168.100.1".to_string(),
                range_start: "192.168.100.20".to_string(),
                range_end: "192.168.100.30".to_string(),
            }],
        }
    }

    fn pool<'a>(net: &'a NetworkConfig, name: &str) -> &'a Pool {
        net.pool_by_name(name)
            .unwrap_or_else(|| panic!("test network config missing pool '{name}'"))
    }

    fn make_host(id: &str, hostname: &str) -> HostRow {
        HostRow {
            id: id.to_string(),
            hostname: hostname.to_string(),
            total_cpu: 16,
            total_memory_mib: 65536,
            total_disk_gib: 1000,
            gpu_inventory: "[]".to_string(),
            vtep_address: format!("10.100.0.{}", id.bytes().last().unwrap_or(b'1')),
            last_heartbeat: "2025-01-01T00:00:00Z".to_string(),
            healthy: true,
        }
    }

    fn make_cluster(id: &str, name: &str, tree_id: &str, endpoint: &str) -> ClusterRow {
        ClusterRow {
            id: id.to_string(),
            name: name.to_string(),
            tree_id: tree_id.to_string(),
            parent_cluster_id: None,
            control_plane_endpoint: endpoint.to_string(),
            apiserver_pool: String::new(),
            created_at: "2025-01-01T00:00:00Z".to_string(),
        }
    }

    fn make_vm(id: &str, host_id: &str, cluster_id: &str, ip: &str) -> VmRow {
        VmRow {
            id: id.to_string(),
            name: format!("vm-{id}"),
            cluster_id: cluster_id.to_string(),
            host_id: host_id.to_string(),
            ip_address: ip.to_string(),
            state: 2,
            cpu: 4,
            memory_mib: 8192,
            disk_gib: 100,
            extra_disk_total_gib: 0,
            extra_disk_gibs: "[]".to_string(),
            image: "test:latest".to_string(),
            error_message: String::new(),
            created_at: "2025-01-01T00:00:00Z".to_string(),
            updated_at: "2025-01-01T00:00:00Z".to_string(),
        }
    }

    #[tokio::test]
    async fn allocate_tree_picks_sequential_vni_and_cidr() {
        let db = test_db().await;
        let net = make_net_config();

        let t1 = db.allocate_tree(&net).await.unwrap();
        let t2 = db.allocate_tree(&net).await.unwrap();
        assert_eq!(t1.vni, 10_000);
        assert_eq!(t2.vni, 10_001);
        assert_ne!(t1.cidr, t2.cidr);
        assert_eq!(t1.prefix_len, 20);
    }

    #[tokio::test]
    async fn tree_layout_is_sane() {
        let db = test_db().await;
        let net = make_net_config();
        let t = db.allocate_tree(&net).await.unwrap();
        let cidr: ipnet::Ipv4Net = t.cidr.parse().unwrap();
        let bridge_start: Ipv4Addr = t.bridge_range_start.parse().unwrap();
        let bridge_end: Ipv4Addr = t.bridge_range_end.parse().unwrap();
        let vm_start: Ipv4Addr = t.vm_range_start.parse().unwrap();
        let vm_end: Ipv4Addr = t.vm_range_end.parse().unwrap();
        let vip_start: Ipv4Addr = t.vip_range_start.parse().unwrap();
        let vip_end: Ipv4Addr = t.vip_range_end.parse().unwrap();

        assert_eq!(u32::from(bridge_start), u32::from(cidr.network()) + 1);
        assert_eq!(
            u32::from(bridge_end) - u32::from(bridge_start),
            (net.bridge_reserve - 1) as u32
        );
        assert_eq!(u32::from(vm_start), u32::from(bridge_end) + 1);
        assert_eq!(u32::from(vm_end), u32::from(vip_start) - 1);
        assert_eq!(u32::from(vip_end), u32::from(cidr.broadcast()) - 1);
        assert_eq!(
            u32::from(vip_end) - u32::from(vip_start),
            (net.vip_reserve - 1) as u32
        );
    }

    #[tokio::test]
    async fn deleting_tree_frees_its_vni() {
        let db = test_db().await;
        let mut net = make_net_config();
        net.vni_range.end = 10_000; // single VNI

        let t = db.allocate_tree(&net).await.unwrap();
        assert!(db.allocate_tree(&net).await.is_err());
        db.delete_tree(&t.id).await.unwrap();
        let t2 = db.allocate_tree(&net).await.unwrap();
        assert_eq!(t2.vni, 10_000);
    }

    #[tokio::test]
    async fn allocate_tree_vm_ip_starts_at_vm_range_start() {
        let db = test_db().await;
        let net = make_net_config();
        let t = db.allocate_tree(&net).await.unwrap();

        let ip = db.allocate_tree_vm_ip(&t, "vm1").await.unwrap();
        assert_eq!(ip, t.vm_range_start);
    }

    #[tokio::test]
    async fn cluster_vip_pool_vs_tree_scopes() {
        let db = test_db().await;
        let net = make_net_config();
        let t = db.allocate_tree(&net).await.unwrap();
        let cidr: ipnet::Ipv4Net = t.cidr.parse().unwrap();

        db.insert_cluster(&make_cluster("pool-cluster", "pool", &t.id, "unused"))
            .await
            .unwrap();
        db.insert_cluster(&make_cluster("tree-cluster", "tree", &t.id, "unused"))
            .await
            .unwrap();

        // Pool-scoped VIP — host BGP advertises this /32 with self
        // as next-hop, reachable from outside the tree.
        let pool_vip = db
            .allocate_pool_vip(pool(&net, "cell-internal"), "pool-cluster")
            .await
            .unwrap();
        assert!(pool_vip.starts_with("192.168.100."));
        assert!(
            !cidr.contains(&pool_vip.parse::<Ipv4Addr>().unwrap()),
            "pool VIP must land in the named pool, not the tree CIDR"
        );

        // Tree-scoped VIP — VIP stays inside the overlay; only
        // workers in the same tree can reach it.
        let tree_vip = db.allocate_tree_vip(&t, "tree-cluster").await.unwrap();
        assert!(
            cidr.contains(&tree_vip.parse::<Ipv4Addr>().unwrap()),
            "tree VIP must land inside the tree CIDR"
        );
        let vip_start: Ipv4Addr = t.vip_range_start.parse().unwrap();
        assert_eq!(
            tree_vip,
            vip_start.to_string(),
            "first tree VIP should be the low end of vip_range"
        );
    }

    #[tokio::test]
    async fn vm_tree_ip_release_frees_for_reuse() {
        let db = test_db().await;
        let net = make_net_config();
        let t = db.allocate_tree(&net).await.unwrap();

        let ip1 = db.allocate_tree_vm_ip(&t, "vm1").await.unwrap();
        db.release_vm_ips("vm1").await.unwrap();
        let ip2 = db.allocate_tree_vm_ip(&t, "vm2").await.unwrap();
        assert_eq!(ip1, ip2, "released tree IP must be reusable");
    }

    #[tokio::test]
    async fn cluster_vip_released_by_explicit_call() {
        let db = test_db().await;
        let net = make_net_config();
        let t = db.allocate_tree(&net).await.unwrap();
        db.insert_cluster(&make_cluster("c1", "c1", &t.id, "unused"))
            .await
            .unwrap();

        let vip = db
            .allocate_pool_vip(pool(&net, "cell-internal"), "c1")
            .await
            .unwrap();
        assert!(vip.starts_with("192.168.100."));

        db.release_cluster_ips("c1").await.unwrap();
        db.delete_cluster("c1").await.unwrap();

        db.insert_cluster(&make_cluster("c2", "c2", &t.id, "unused"))
            .await
            .unwrap();
        let vip2 = db
            .allocate_pool_vip(pool(&net, "cell-internal"), "c2")
            .await
            .unwrap();
        assert_eq!(
            vip2, vip,
            "released VIP must be reusable on the next allocation"
        );
    }

    #[tokio::test]
    async fn host_bridge_ip_is_unique_and_idempotent() {
        let db = test_db().await;
        let net = make_net_config();
        let tree = db.allocate_tree(&net).await.unwrap();

        let ip_h1 = db.ensure_host_bridge_ip(&tree, "h1").await.unwrap();
        let ip_h2 = db.ensure_host_bridge_ip(&tree, "h2").await.unwrap();
        assert_ne!(ip_h1, ip_h2);
        assert_eq!(ip_h1, tree.bridge_range_start);

        // Idempotent for the same host.
        let ip_h1_again = db.ensure_host_bridge_ip(&tree, "h1").await.unwrap();
        assert_eq!(ip_h1, ip_h1_again);

        // Read-only lookup agrees.
        assert_eq!(
            db.get_host_bridge_ip(&tree.id, "h1").await.unwrap(),
            Some(ip_h1.clone()),
        );
        assert_eq!(
            db.get_host_bridge_ip(&tree.id, "missing").await.unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn host_bridge_ip_release_only_when_idle() {
        let db = test_db().await;
        let mut host = make_host("h1", "node-1");
        host.vtep_address = "10.100.0.1".to_string();
        db.upsert_host(&host).await.unwrap();

        let net = make_net_config();
        let tree = db.allocate_tree(&net).await.unwrap();
        db.insert_cluster(&make_cluster("c1", "c1", &tree.id, "unused-endpoint"))
            .await
            .unwrap();

        let ip = db.ensure_host_bridge_ip(&tree, "h1").await.unwrap();

        db.insert_vm(&make_vm("v1", "h1", "c1", &tree.vm_range_start), &[])
            .await
            .unwrap();

        // Still has a VM → release is a no-op.
        db.release_host_bridge_ip_if_idle(&tree.id, "h1")
            .await
            .unwrap();
        assert_eq!(
            db.get_host_bridge_ip(&tree.id, "h1").await.unwrap(),
            Some(ip.clone())
        );

        // Last VM gone → release drops the mapping, freeing the IP.
        db.delete_vm("v1").await.unwrap();
        db.release_host_bridge_ip_if_idle(&tree.id, "h1")
            .await
            .unwrap();
        assert_eq!(db.get_host_bridge_ip(&tree.id, "h1").await.unwrap(), None);

        // A later host can reuse the address.
        let mut h2 = make_host("h2", "node-2");
        h2.vtep_address = "10.100.0.2".to_string();
        db.upsert_host(&h2).await.unwrap();
        let ip2 = db.ensure_host_bridge_ip(&tree, "h2").await.unwrap();
        assert_eq!(ip2, ip, "released IP must be reusable");
    }

    #[tokio::test]
    async fn delete_tree_cascades_bridge_mappings() {
        let db = test_db().await;
        let net = make_net_config();
        let tree = db.allocate_tree(&net).await.unwrap();
        db.ensure_host_bridge_ip(&tree, "h1").await.unwrap();
        db.delete_tree(&tree.id).await.unwrap();
        assert_eq!(db.get_host_bridge_ip(&tree.id, "h1").await.unwrap(), None);
    }

    #[tokio::test]
    async fn list_tree_vteps_derives_from_vm_placements() {
        let db = test_db().await;
        let mut h1 = make_host("h1", "node-1");
        h1.vtep_address = "10.100.0.1".to_string();
        let mut h2 = make_host("h2", "node-2");
        h2.vtep_address = "10.100.0.2".to_string();
        db.upsert_host(&h1).await.unwrap();
        db.upsert_host(&h2).await.unwrap();

        let net = make_net_config();
        let t = db.allocate_tree(&net).await.unwrap();
        db.insert_cluster(&make_cluster("c1", "c1", &t.id, "unused-endpoint"))
            .await
            .unwrap();

        // No VMs yet → no VTEPs.
        assert!(db.list_tree_vteps(&t.id).await.unwrap().is_empty());

        // One VM on h1 → one VTEP.
        db.insert_vm(&make_vm("v1", "h1", "c1", &t.vm_range_start), &[])
            .await
            .unwrap();
        assert_eq!(
            db.list_tree_vteps(&t.id).await.unwrap(),
            vec!["10.100.0.1".to_string()]
        );

        // Add a VM on h2 → both VTEPs.
        db.insert_vm(&make_vm("v2", "h2", "c1", "10.0.0.3"), &[])
            .await
            .unwrap();
        let mut vteps = db.list_tree_vteps(&t.id).await.unwrap();
        vteps.sort();
        assert_eq!(
            vteps,
            vec!["10.100.0.1".to_string(), "10.100.0.2".to_string()]
        );

        // Last VM on h1 gone → only h2's VTEP.
        db.delete_vm("v1").await.unwrap();
        assert_eq!(
            db.list_tree_vteps(&t.id).await.unwrap(),
            vec!["10.100.0.2".to_string()]
        );
    }

    /// Seed a `(host, tree, cluster)` trio the VM-race tests can
    /// aim placements at. Extracted here so the three optimistic-
    /// concurrency tests below don't repeat the same 10 lines.
    async fn seed_single_host_cluster(db: &Db) -> (HostRow, ClusterRow, TreeRow) {
        let mut host = make_host("h1", "node-1");
        host.vtep_address = "10.100.0.1".to_string();
        db.upsert_host(&host).await.unwrap();
        let net = make_net_config();
        let tree = db.allocate_tree(&net).await.unwrap();
        let cluster = make_cluster("c1", "c1", &tree.id, "unused-endpoint");
        db.insert_cluster(&cluster).await.unwrap();
        (host, cluster, tree)
    }

    /// The atomic capacity gate in `insert_vm` refuses the second
    /// placement if a concurrent winner has already consumed the
    /// cpu slice we targeted — the writer's single-connection
    /// serialization makes this a faithful stand-in for the real
    /// race in `create_machine`.
    #[tokio::test]
    async fn insert_vm_rejects_cpu_capacity_race() {
        let db = test_db().await;
        let (_, _, _) = seed_single_host_cluster(&db).await;

        let mut v1 = make_vm("v1", "h1", "c1", "10.0.0.9");
        v1.cpu = 10;
        db.insert_vm(&v1, &[]).await.unwrap();

        // Host has 16 cpu, 10 in use → 6 free. A second 10-cpu VM
        // can't fit; the commit-time gate must reject it.
        let mut v2 = make_vm("v2", "h1", "c1", "10.0.0.10");
        v2.cpu = 10;
        match db.insert_vm(&v2, &[]).await {
            Err(DbError::CapacityRaced(h)) => assert_eq!(h, "h1"),
            other => panic!("expected CapacityRaced, got {other:?}"),
        }
        assert!(
            db.get_vm("v2").await.is_err(),
            "rejected row must not persist"
        );
    }

    /// Two placements that each claim the same PCI address on the same
    /// host collide on `vm_gpus`' `UNIQUE (host_id, pci_address)`. The
    /// whole txn rolls back so the vm row doesn't leak either.
    #[tokio::test]
    async fn insert_vm_rejects_gpu_collision_and_rolls_back_vm_row() {
        let db = test_db().await;
        seed_single_host_cluster(&db).await;

        let gpu = |vm_id: &str| GpuAssignment {
            vm_id: vm_id.to_string(),
            host_id: "h1".to_string(),
            pci_address: "0000:41:00.0".to_string(),
            model: "A100".to_string(),
            iommu_group: "12".to_string(),
            nvlink_group: 1,
        };

        db.insert_vm(&make_vm("v1", "h1", "c1", "10.0.0.9"), &[gpu("v1")])
            .await
            .unwrap();

        match db
            .insert_vm(&make_vm("v2", "h1", "c1", "10.0.0.10"), &[gpu("v2")])
            .await
        {
            Err(DbError::CapacityRaced(h)) => assert_eq!(h, "h1"),
            other => panic!("expected CapacityRaced on GPU collision, got {other:?}"),
        }

        assert!(db.get_vm("v1").await.is_ok());
        assert!(
            db.get_vm("v2").await.is_err(),
            "gpu conflict must roll back the vm row, not leak it"
        );
        assert_eq!(db.gpus_for_vm("v1").await.unwrap().len(), 1);
        assert!(db.gpus_for_vm("v2").await.unwrap().is_empty());
    }

    /// When the host row flipped to unhealthy between scheduler
    /// snapshot and commit, the caller needs a different signal —
    /// `HostUnavailable` says "re-pick a host", `CapacityRaced` says
    /// "retry on the same host".
    #[tokio::test]
    async fn insert_vm_distinguishes_unhealthy_from_raced() {
        let db = test_db().await;
        seed_single_host_cluster(&db).await;
        db.mark_host_unhealthy("h1").await.unwrap();

        let vm = make_vm("v1", "h1", "c1", "10.0.0.9");
        match db.insert_vm(&vm, &[]).await {
            Err(DbError::HostUnavailable(h)) => assert_eq!(h, "h1"),
            other => panic!("expected HostUnavailable, got {other:?}"),
        }
    }

    /// GPUs released on VM delete — `ON DELETE CASCADE` on `vm_gpus`
    /// means no application-level cleanup is needed. A subsequent
    /// placement can reclaim the same PCI address.
    #[tokio::test]
    async fn delete_vm_cascades_gpu_reservations() {
        let db = test_db().await;
        seed_single_host_cluster(&db).await;

        let gpu = GpuAssignment {
            vm_id: "v1".to_string(),
            host_id: "h1".to_string(),
            pci_address: "0000:41:00.0".to_string(),
            model: "A100".to_string(),
            iommu_group: "12".to_string(),
            nvlink_group: 1,
        };
        db.insert_vm(&make_vm("v1", "h1", "c1", "10.0.0.9"), &[gpu])
            .await
            .unwrap();
        db.delete_vm("v1").await.unwrap();
        assert!(db.gpus_for_vm("v1").await.unwrap().is_empty());

        // Same PCI address is free again — a successor placement
        // doesn't hit the unique index.
        let gpu2 = GpuAssignment {
            vm_id: "v2".to_string(),
            host_id: "h1".to_string(),
            pci_address: "0000:41:00.0".to_string(),
            model: "A100".to_string(),
            iommu_group: "12".to_string(),
            nvlink_group: 1,
        };
        db.insert_vm(&make_vm("v2", "h1", "c1", "10.0.0.10"), &[gpu2])
            .await
            .unwrap();
    }

    /// `host_usage_snapshot` rolls up the exact view the scheduler
    /// uses — what `insert_vm`'s commit gate checks against — so a
    /// stale snapshot is never silently more permissive than the gate.
    #[tokio::test]
    async fn host_usage_snapshot_matches_insert_gate() {
        let db = test_db().await;
        seed_single_host_cluster(&db).await;

        let mut v1 = make_vm("v1", "h1", "c1", "10.0.0.9");
        v1.cpu = 4;
        v1.memory_mib = 8192;
        v1.disk_gib = 100;
        v1.extra_disk_total_gib = 50;
        db.insert_vm(&v1, &[]).await.unwrap();

        let snapshot = db.host_usage_snapshot().await.unwrap();
        let u = snapshot.get("h1").expect("host in snapshot");
        assert_eq!(u.used_cpu, 4);
        assert_eq!(u.used_memory_mib, 8192);
        assert_eq!(u.used_disk_gib, 150, "disk usage = rootfs + extras");
        assert!(u.assigned_pci.is_empty());
    }
}
