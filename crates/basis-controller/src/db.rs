use std::collections::HashSet;
use std::net::Ipv4Addr;
use std::path::Path;
use std::str::FromStr;

use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};

use crate::config::{NetworkConfig, EDGE_SCOPE};

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

    #[error(
        "tree '{tree}' has malformed {field} = '{value}' in the DB: {reason} \
         — controller.yaml validation should have caught this, so the DB has likely been \
         edited out-of-band; fix the row or re-seed from a validated config"
    )]
    MalformedTree {
        tree: String,
        field: &'static str,
        value: String,
        reason: String,
    },
}

/// Every IP allocation is owned by exactly one thing. Two kinds today —
/// a VM's address (either primary tree-scoped or edge-pool-scoped, both
/// keyed by `vm_id`) and a cluster's control-plane VIP.
#[derive(Debug, Clone, Copy)]
pub enum IpOwner<'a> {
    Vm(&'a str),
    ClusterVip(&'a str),
}

impl IpOwner<'_> {
    fn kind(&self) -> &'static str {
        match self {
            IpOwner::Vm(_) => "vm",
            IpOwner::ClusterVip(_) => "cluster_vip",
        }
    }
    fn id(&self) -> &str {
        match self {
            IpOwner::Vm(id) | IpOwner::ClusterVip(id) => id,
        }
    }
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
}

impl Db {
    pub async fn open(path: &Path) -> Result<Self, DbError> {
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

        let db = Self { reader, writer };
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
                vm_range_start TEXT NOT NULL,
                vm_range_end TEXT NOT NULL,
                vip_range_start TEXT NOT NULL,
                vip_range_end TEXT NOT NULL,
                gateway_ip TEXT NOT NULL,
                prefix_len INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                deleted_at TEXT
            )",
        )
        .execute(&self.writer)
        .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS clusters (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                tree_id TEXT NOT NULL REFERENCES trees(id),
                parent_cluster_id TEXT REFERENCES clusters(id),
                control_plane_endpoint TEXT NOT NULL,
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
                edge_ip TEXT,
                state INTEGER NOT NULL DEFAULT 0,
                cpu INTEGER NOT NULL,
                memory_mib INTEGER NOT NULL,
                disk_gib INTEGER NOT NULL,
                gpu_assignments TEXT NOT NULL DEFAULT '[]',
                extra_disk_gibs TEXT NOT NULL DEFAULT '[]',
                image TEXT NOT NULL,
                error_message TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )",
        )
        .execute(&self.writer)
        .await?;

        // Enforces that `(cluster_id, name)` is unique across VMs. CAPI
        // reconcilers may retry `CreateMachine` after a partial failure and
        // we rely on this constraint to keep the name-based idempotency
        // check in server.rs race-free: a second concurrent call either
        // sees the existing row or is rejected at insert time.
        sqlx::query(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_vms_cluster_name
             ON vms (cluster_id, name)",
        )
        .execute(&self.writer)
        .await?;

        // host_in_tree: which hosts carry at least one VM in which
        // tree. Maintained as an invariant alongside every VM
        // insert/delete. Drives the controller's reconcile broadcast
        // — a change here means the peer VTEP list for the tree
        // shifted and every participating agent needs a new FDB.
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS host_in_tree (
                host_id TEXT NOT NULL REFERENCES hosts(id),
                tree_id TEXT NOT NULL REFERENCES trees(id),
                PRIMARY KEY (host_id, tree_id)
            )",
        )
        .execute(&self.writer)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_host_in_tree_tree ON host_in_tree(tree_id)",
        )
        .execute(&self.writer)
        .await?;

        // IP allocations. `scope` is either a tree_id (UUID) or the
        // literal "edge" — UUIDs can't collide with the sentinel. One
        // VM with an edge NIC ends up with two rows in here: (tree
        // scope, primary IP) and (edge scope, edge IP), both with
        // owner_id = vm_id and owner_kind = "vm" so
        // release_ips(Vm(vm_id)) drops both in one statement.
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS ip_allocations (
                ip_address TEXT PRIMARY KEY,
                scope TEXT NOT NULL,
                owner_id TEXT NOT NULL,
                owner_kind TEXT NOT NULL
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

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_ip_allocations_owner \
             ON ip_allocations(owner_id, owner_kind)",
        )
        .execute(&self.writer)
        .await?;

        Ok(())
    }

    // --- Trees ---

    /// Atomically allocate a new tree: pick the next free VNI and carve
    /// the next free sub-CIDR out of `net.tree_supernet`. Reaps any tree
    /// whose `deleted_at` is older than `net.vni_cooldown_secs` before
    /// computing the free set, so a VNI comes back after the cooldown
    /// window without any operator action.
    pub async fn allocate_tree(
        &self,
        net: &NetworkConfig,
        now_unix: i64,
    ) -> Result<TreeRow, DbError> {
        let cooldown = i64::try_from(net.vni_cooldown_secs).unwrap_or(i64::MAX);
        let reap_before = now_unix.saturating_sub(cooldown);

        let mut tx = self.writer.begin().await?;

        // Reap trees past their cooldown. Stored `deleted_at` is unix
        // seconds — the whole cooldown machinery is numeric so we
        // don't have to parse RFC3339 here.
        sqlx::query(
            "DELETE FROM trees \
             WHERE deleted_at IS NOT NULL AND CAST(deleted_at AS INTEGER) < ?",
        )
        .bind(reap_before)
        .execute(&mut *tx)
        .await?;

        // Load what's still taken (active + in-cooldown).
        let taken: Vec<(i64, String)> = sqlx::query_as("SELECT vni, cidr FROM trees")
            .fetch_all(&mut *tx)
            .await?;
        let used_vnis: HashSet<u32> =
            taken.iter().map(|(v, _)| *v as u32).collect();
        let mut used_cidrs: Vec<ipnet::Ipv4Net> = Vec::with_capacity(taken.len());
        for (_, c) in &taken {
            let parsed: ipnet::Ipv4Net = c.parse().map_err(|e: ipnet::AddrParseError| {
                DbError::MalformedTree {
                    tree: "(any)".to_string(),
                    field: "cidr",
                    value: c.clone(),
                    reason: e.to_string(),
                }
            })?;
            used_cidrs.push(parsed);
        }

        // Pick the next VNI in range. The range is at most 16M large so
        // a linear scan is fine — allocation is cluster-create, not VM-
        // create.
        let vni = (net.vni_range.start..=net.vni_range.end)
            .find(|v| !used_vnis.contains(v))
            .ok_or_else(|| {
                DbError::Exhausted(format!(
                    "VNI range [{}, {}] fully allocated",
                    net.vni_range.start, net.vni_range.end
                ))
            })?;

        // Carve the next free /tree_prefix slice out of the supernet.
        let supernet: ipnet::Ipv4Net = net.tree_supernet.parse().map_err(|e: ipnet::AddrParseError| {
            DbError::MalformedTree {
                tree: "(config)".to_string(),
                field: "tree_supernet",
                value: net.tree_supernet.clone(),
                reason: e.to_string(),
            }
        })?;
        let candidate = supernet
            .subnets(net.tree_prefix)
            .map_err(|e| {
                DbError::MalformedTree {
                    tree: "(config)".to_string(),
                    field: "tree_prefix",
                    value: net.tree_prefix.to_string(),
                    reason: format!("{e}"),
                }
            })?
            .find(|c| !used_cidrs.iter().any(|u| cidrs_overlap(u, c)))
            .ok_or_else(|| {
                DbError::Exhausted(format!(
                    "tree supernet {} fully carved into /{} slices",
                    net.tree_supernet, net.tree_prefix
                ))
            })?;

        let layout = TreeLayout::carve(&candidate, net.vip_reserve);

        let id = uuid::Uuid::new_v4().to_string();
        let created_at = basis_common::time::now_rfc3339();
        sqlx::query(
            "INSERT INTO trees (
                id, vni, cidr,
                vm_range_start, vm_range_end,
                vip_range_start, vip_range_end,
                gateway_ip, prefix_len,
                created_at, deleted_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)",
        )
        .bind(&id)
        .bind(vni as i64)
        .bind(candidate.to_string())
        .bind(layout.vm_start.to_string())
        .bind(layout.vm_end.to_string())
        .bind(layout.vip_start.to_string())
        .bind(layout.vip_end.to_string())
        .bind(layout.gateway.to_string())
        .bind(net.tree_prefix as i64)
        .bind(&created_at)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(TreeRow {
            id,
            vni: vni as i64,
            cidr: candidate.to_string(),
            vm_range_start: layout.vm_start.to_string(),
            vm_range_end: layout.vm_end.to_string(),
            vip_range_start: layout.vip_start.to_string(),
            vip_range_end: layout.vip_end.to_string(),
            gateway_ip: layout.gateway.to_string(),
            prefix_len: net.tree_prefix as i64,
            created_at,
            deleted_at: None,
        })
    }

    pub async fn get_tree(&self, id: &str) -> Result<TreeRow, DbError> {
        sqlx::query_as::<_, TreeRow>("SELECT * FROM trees WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.reader)
            .await?
            .ok_or_else(|| DbError::NotFound(format!("tree '{id}'")))
    }

    /// Mark a tree as pending-reap. VNI is held until
    /// `vni_cooldown_secs` elapses; the next `allocate_tree` call
    /// sweeps expired rows.
    pub async fn mark_tree_deleted(&self, id: &str, now_unix: i64) -> Result<(), DbError> {
        let result = sqlx::query("UPDATE trees SET deleted_at = ? WHERE id = ?")
            .bind(now_unix)
            .bind(id)
            .execute(&self.writer)
            .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::NotFound(format!("tree '{id}'")));
        }
        Ok(())
    }

    // --- host_in_tree ---

    /// Ensure (host, tree) is present. Returns `true` if this call was
    /// the inserter — caller rebroadcasts a reconcile to the tree.
    pub async fn upsert_host_in_tree(
        &self,
        host_id: &str,
        tree_id: &str,
    ) -> Result<bool, DbError> {
        let inserted: Option<i64> = sqlx::query_scalar(
            "INSERT INTO host_in_tree (host_id, tree_id)
             VALUES (?, ?)
             ON CONFLICT DO NOTHING
             RETURNING 1",
        )
        .bind(host_id)
        .bind(tree_id)
        .fetch_optional(&self.writer)
        .await?;
        Ok(inserted.is_some())
    }

    /// Drop (host, tree) only if no VM of this tree still lives on this
    /// host. Returns `true` if the row was removed — caller broadcasts
    /// an updated peer list.
    pub async fn remove_host_in_tree_if_empty(
        &self,
        host_id: &str,
        tree_id: &str,
    ) -> Result<bool, DbError> {
        let removed = sqlx::query(
            "DELETE FROM host_in_tree
             WHERE host_id = ? AND tree_id = ?
               AND NOT EXISTS (
                 SELECT 1 FROM vms v
                 JOIN clusters c ON v.cluster_id = c.id
                 WHERE v.host_id = ? AND c.tree_id = ?
               )",
        )
        .bind(host_id)
        .bind(tree_id)
        .bind(host_id)
        .bind(tree_id)
        .execute(&self.writer)
        .await?;
        Ok(removed.rows_affected() > 0)
    }

    /// VTEP addresses of every host that currently carries this tree.
    /// Output drives the agent-side FDB reconcile.
    pub async fn list_tree_vteps(&self, tree_id: &str) -> Result<Vec<String>, DbError> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT h.vtep_address
             FROM host_in_tree hit
             JOIN hosts h ON h.id = hit.host_id
             WHERE hit.tree_id = ?",
        )
        .bind(tree_id)
        .fetch_all(&self.reader)
        .await?;
        Ok(rows.into_iter().map(|(v,)| v).filter(|v| !v.is_empty()).collect())
    }

    /// Every tree this host currently carries. Drives the agent's
    /// `ReconcileHostCommand.trees` list.
    pub async fn list_host_trees(&self, host_id: &str) -> Result<Vec<TreeRow>, DbError> {
        Ok(sqlx::query_as::<_, TreeRow>(
            "SELECT t.* FROM trees t
             JOIN host_in_tree hit ON hit.tree_id = t.id
             WHERE hit.host_id = ?",
        )
        .bind(host_id)
        .fetch_all(&self.reader)
        .await?)
    }

    /// Host IDs currently carrying VMs in this tree. The caller pushes
    /// a reconcile to each connected agent after a membership change.
    pub async fn list_hosts_in_tree(&self, tree_id: &str) -> Result<Vec<String>, DbError> {
        let rows: Vec<(String,)> =
            sqlx::query_as("SELECT host_id FROM host_in_tree WHERE tree_id = ?")
                .bind(tree_id)
                .fetch_all(&self.reader)
                .await?;
        Ok(rows.into_iter().map(|(h,)| h).collect())
    }

    // --- IP allocation ---

    /// Allocate the next free address from the tree's VM sub-range.
    pub async fn allocate_tree_vm_ip(
        &self,
        tree: &TreeRow,
        owner: IpOwner<'_>,
    ) -> Result<String, DbError> {
        let range = tree.vm_range()?;
        self.allocate_from_range(&tree.id, &range, owner).await
    }

    /// Allocate the next free address from the tree's VIP sub-range.
    pub async fn allocate_tree_vip(
        &self,
        tree: &TreeRow,
        owner: IpOwner<'_>,
    ) -> Result<String, DbError> {
        let range = tree.vip_range()?;
        self.allocate_from_range(&tree.id, &range, owner).await
    }

    /// Allocate the next free address from the global edge pool (the
    /// second NIC for `edge: true` machines).
    pub async fn allocate_edge_ip(
        &self,
        net: &NetworkConfig,
        owner: IpOwner<'_>,
    ) -> Result<String, DbError> {
        let start: Ipv4Addr = net
            .edge_pool
            .range_start
            .parse()
            .map_err(|e: std::net::AddrParseError| DbError::MalformedTree {
                tree: "(edge)".to_string(),
                field: "edge_pool.range_start",
                value: net.edge_pool.range_start.clone(),
                reason: e.to_string(),
            })?;
        let end: Ipv4Addr =
            net.edge_pool.range_end.parse().map_err(|e: std::net::AddrParseError| {
                DbError::MalformedTree {
                    tree: "(edge)".to_string(),
                    field: "edge_pool.range_end",
                    value: net.edge_pool.range_end.clone(),
                    reason: e.to_string(),
                }
            })?;
        let range = ParsedRange {
            start: u32::from(start),
            end: u32::from(end),
        };
        self.allocate_from_range(EDGE_SCOPE, &range, owner).await
    }

    async fn allocate_from_range(
        &self,
        scope: &str,
        range: &ParsedRange,
        owner: IpOwner<'_>,
    ) -> Result<String, DbError> {
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
            INSERT INTO ip_allocations (ip_address, scope, owner_id, owner_kind)
            SELECT ip, ?, ?, ? FROM picked
            RETURNING ip_address
            "#,
        )
        .bind(range.start as i64)
        .bind(range.end as i64)
        .bind(scope)
        .bind(scope)
        .bind(owner.id())
        .bind(owner.kind())
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

    /// Release every IP held by this owner, regardless of scope.
    /// Covers both a VM's primary (tree) IP and its edge IP in one call.
    pub async fn release_ips(&self, owner: IpOwner<'_>) -> Result<(), DbError> {
        sqlx::query("DELETE FROM ip_allocations WHERE owner_id = ? AND owner_kind = ?")
            .bind(owner.id())
            .bind(owner.kind())
            .execute(&self.writer)
            .await?;
        Ok(())
    }

    // --- Clusters ---

    pub async fn insert_cluster(&self, cluster: &ClusterRow) -> Result<(), DbError> {
        sqlx::query(
            "INSERT INTO clusters (id, name, tree_id, parent_cluster_id, control_plane_endpoint, created_at)
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(&cluster.id)
        .bind(&cluster.name)
        .bind(&cluster.tree_id)
        .bind(&cluster.parent_cluster_id)
        .bind(&cluster.control_plane_endpoint)
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
        Ok(sqlx::query_as::<_, ClusterRow>(
            "SELECT * FROM clusters WHERE parent_cluster_id = ?",
        )
        .bind(parent_id)
        .fetch_all(&self.reader)
        .await?)
    }

    /// Number of clusters still attached to a tree.
    pub async fn count_clusters_in_tree(&self, tree_id: &str) -> Result<i64, DbError> {
        let (n,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM clusters WHERE tree_id = ?")
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

    pub async fn insert_vm(&self, vm: &VmRow) -> Result<(), DbError> {
        let result = sqlx::query(
            "INSERT INTO vms (id, name, cluster_id, host_id, ip_address, edge_ip, state,
                cpu, memory_mib, disk_gib,
                gpu_assignments, extra_disk_gibs, image, error_message, created_at, updated_at)
             SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
             WHERE EXISTS (SELECT 1 FROM hosts WHERE id = ? AND healthy = 1)",
        )
        .bind(&vm.id)
        .bind(&vm.name)
        .bind(&vm.cluster_id)
        .bind(&vm.host_id)
        .bind(&vm.ip_address)
        .bind(&vm.edge_ip)
        .bind(vm.state)
        .bind(vm.cpu)
        .bind(vm.memory_mib)
        .bind(vm.disk_gib)
        .bind(&vm.gpu_assignments)
        .bind(&vm.extra_disk_gibs)
        .bind(&vm.image)
        .bind(&vm.error_message)
        .bind(&vm.created_at)
        .bind(&vm.updated_at)
        .bind(&vm.host_id)
        .execute(&self.writer)
        .await
        .map_err(|e| match e {
            sqlx::Error::Database(db_err) if db_err.is_unique_violation() => DbError::Conflict(
                format!("vm '{}' already exists in cluster '{}'", vm.name, vm.cluster_id),
            ),
            other => DbError::Sqlx(other),
        })?;

        if result.rows_affected() == 0 {
            return Err(DbError::HostUnavailable(vm.host_id.clone()));
        }
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

    pub async fn list_vms_on_host(&self, host_id: &str) -> Result<Vec<VmRow>, DbError> {
        Ok(
            sqlx::query_as::<_, VmRow>("SELECT * FROM vms WHERE host_id = ?")
                .bind(host_id)
                .fetch_all(&self.reader)
                .await?,
        )
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

/// Layout of a single tree's CIDR: gateway, VM range, VIP range. The
/// top `vip_reserve` addresses (below broadcast) go to VIPs; everything
/// between gateway+1 and the VIP floor is the VM range.
struct TreeLayout {
    gateway: Ipv4Addr,
    vm_start: Ipv4Addr,
    vm_end: Ipv4Addr,
    vip_start: Ipv4Addr,
    vip_end: Ipv4Addr,
}

impl TreeLayout {
    fn carve(cidr: &ipnet::Ipv4Net, vip_reserve: u32) -> Self {
        let net = u32::from(cidr.network());
        let bcast = u32::from(cidr.broadcast());
        // gateway = net + 1; vip_end = bcast - 1; vip_start = vip_end -
        // vip_reserve + 1; vm_start = gateway + 1; vm_end = vip_start - 1.
        // Config validation (`NetworkConfig::validate`) guarantees the
        // slice holds enough addresses for all of these.
        let gateway = net + 1;
        let vip_end = bcast - 1;
        let vip_start = vip_end - (vip_reserve - 1);
        let vm_start = gateway + 1;
        let vm_end = vip_start - 1;
        Self {
            gateway: Ipv4Addr::from(gateway),
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
    /// Non-NULL iff the machine was created with `edge: true`. The
    /// agent attaches a second TAP to the uplink bridge and configures
    /// this as `eth1` in cloud-init.
    pub edge_ip: Option<String>,
    pub state: i64,
    pub cpu: i64,
    pub memory_mib: i64,
    pub disk_gib: i64,
    pub gpu_assignments: String,
    pub extra_disk_gibs: String,
    pub image: String,
    pub error_message: String,
    pub created_at: String,
    pub updated_at: String,
}

impl VmRow {
    pub fn total_disk_gib(&self) -> i64 {
        let extras: Vec<u32> =
            basis_common::json::parse_owned_json(&self.extra_disk_gibs, "vms.extra_disk_gibs");
        self.disk_gib + extras.iter().map(|&g| g as i64).sum::<i64>()
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ClusterRow {
    pub id: String,
    pub name: String,
    pub tree_id: String,
    pub parent_cluster_id: Option<String>,
    pub control_plane_endpoint: String,
    pub created_at: String,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct TreeRow {
    pub id: String,
    pub vni: i64,
    pub cidr: String,
    pub vm_range_start: String,
    pub vm_range_end: String,
    pub vip_range_start: String,
    pub vip_range_end: String,
    pub gateway_ip: String,
    pub prefix_len: i64,
    pub created_at: String,
    pub deleted_at: Option<String>,
}

impl TreeRow {
    pub fn vm_range(&self) -> Result<ParsedRange, DbError> {
        ParsedRange::parse(&self.vm_range_start, &self.vm_range_end).map_err(|(field, reason)| {
            DbError::MalformedTree {
                tree: self.id.clone(),
                field,
                value: match field {
                    "range_start" => self.vm_range_start.clone(),
                    _ => self.vm_range_end.clone(),
                },
                reason,
            }
        })
    }

    pub fn vip_range(&self) -> Result<ParsedRange, DbError> {
        ParsedRange::parse(&self.vip_range_start, &self.vip_range_end).map_err(|(field, reason)| {
            DbError::MalformedTree {
                tree: self.id.clone(),
                field,
                value: match field {
                    "range_start" => self.vip_range_start.clone(),
                    _ => self.vip_range_end.clone(),
                },
                reason,
            }
        })
    }
}

/// Inclusive IPv4 range expressed as host-order `u32`s — the shape the
/// allocator actually iterates over.
#[derive(Debug, Clone, Copy)]
pub struct ParsedRange {
    pub start: u32,
    pub end: u32,
}

impl ParsedRange {
    fn parse(start: &str, end: &str) -> Result<Self, (&'static str, String)> {
        let s: Ipv4Addr = start
            .parse()
            .map_err(|e: std::net::AddrParseError| ("range_start", e.to_string()))?;
        let e: Ipv4Addr = end
            .parse()
            .map_err(|e: std::net::AddrParseError| ("range_end", e.to_string()))?;
        Ok(Self {
            start: u32::from(s),
            end: u32::from(e),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{EdgePool, NetworkConfig, VniRange};

    async fn test_db() -> Db {
        Db::open(":memory:".as_ref()).await.unwrap()
    }

    fn make_net_config() -> NetworkConfig {
        NetworkConfig {
            tree_supernet: "10.0.0.0/8".to_string(),
            tree_prefix: 20,
            vip_reserve: 16,
            vni_range: VniRange {
                start: 10_000,
                end: 10_010,
            },
            vni_cooldown_secs: 60,
            edge_pool: EdgePool {
                cidr: "192.168.100.0/24".to_string(),
                gateway: "192.168.100.1".to_string(),
                range_start: "192.168.100.20".to_string(),
                range_end: "192.168.100.30".to_string(),
            },
        }
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
            edge_ip: None,
            state: 2,
            cpu: 4,
            memory_mib: 8192,
            disk_gib: 100,
            gpu_assignments: "[]".to_string(),
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

        let t1 = db.allocate_tree(&net, 0).await.unwrap();
        let t2 = db.allocate_tree(&net, 0).await.unwrap();
        assert_eq!(t1.vni, 10_000);
        assert_eq!(t2.vni, 10_001);
        assert_ne!(t1.cidr, t2.cidr);
        assert_eq!(t1.prefix_len, 20);
    }

    #[tokio::test]
    async fn tree_layout_is_sane() {
        let db = test_db().await;
        let net = make_net_config();
        let t = db.allocate_tree(&net, 0).await.unwrap();
        // /20 = 4096 addrs. VIP reserve 16 → top 16 (below broadcast)
        // are VIPs; gateway = .1; VM range is rest.
        let net_addr: Ipv4Addr = t.cidr.split('/').next().unwrap().parse().unwrap();
        assert_eq!(
            t.gateway_ip,
            Ipv4Addr::from(u32::from(net_addr) + 1).to_string()
        );
        let vm_start: Ipv4Addr = t.vm_range_start.parse().unwrap();
        let vip_start: Ipv4Addr = t.vip_range_start.parse().unwrap();
        assert_eq!(u32::from(vm_start), u32::from(net_addr) + 2);
        let vip_end: Ipv4Addr = t.vip_range_end.parse().unwrap();
        assert_eq!(
            u32::from(vip_end) - u32::from(vip_start),
            (net.vip_reserve - 1) as u32
        );
    }

    #[tokio::test]
    async fn vni_cooldown_blocks_reuse_until_expiry() {
        let db = test_db().await;
        let mut net = make_net_config();
        net.vni_range.end = 10_000; // single VNI

        let t = db.allocate_tree(&net, 0).await.unwrap();
        db.mark_tree_deleted(&t.id, 0).await.unwrap();

        // Within cooldown: allocate fails.
        let err = db.allocate_tree(&net, 30).await.unwrap_err();
        assert!(matches!(err, DbError::Exhausted(_)));

        // Past cooldown: VNI reclaimed.
        let t2 = db.allocate_tree(&net, 1_000).await.unwrap();
        assert_eq!(t2.vni, 10_000);
    }

    #[tokio::test]
    async fn allocate_tree_vm_ip_starts_at_vm_range_start() {
        let db = test_db().await;
        let net = make_net_config();
        let t = db.allocate_tree(&net, 0).await.unwrap();

        let ip = db
            .allocate_tree_vm_ip(&t, IpOwner::Vm("vm1"))
            .await
            .unwrap();
        assert_eq!(ip, t.vm_range_start);
    }

    #[tokio::test]
    async fn vm_and_vip_are_disjoint_within_tree() {
        let db = test_db().await;
        let net = make_net_config();
        let t = db.allocate_tree(&net, 0).await.unwrap();

        let vm_ip = db
            .allocate_tree_vm_ip(&t, IpOwner::Vm("vm1"))
            .await
            .unwrap();
        let vip = db
            .allocate_tree_vip(&t, IpOwner::ClusterVip("c1"))
            .await
            .unwrap();
        assert_ne!(vm_ip, vip);
        let vm_u32 = u32::from(vm_ip.parse::<Ipv4Addr>().unwrap());
        let vip_u32 = u32::from(vip.parse::<Ipv4Addr>().unwrap());
        assert!(vm_u32 < vip_u32);
    }

    #[tokio::test]
    async fn edge_ip_allocation_separate_from_tree() {
        let db = test_db().await;
        let net = make_net_config();
        let t = db.allocate_tree(&net, 0).await.unwrap();

        let vm_ip = db
            .allocate_tree_vm_ip(&t, IpOwner::Vm("vm1"))
            .await
            .unwrap();
        let edge_ip = db
            .allocate_edge_ip(&net, IpOwner::Vm("vm1"))
            .await
            .unwrap();
        assert_ne!(vm_ip, edge_ip);
        assert!(edge_ip.starts_with("192.168.100."));

        // Both released by one call.
        db.release_ips(IpOwner::Vm("vm1")).await.unwrap();
        let edge_ip2 = db
            .allocate_edge_ip(&net, IpOwner::Vm("vm2"))
            .await
            .unwrap();
        assert_eq!(edge_ip2, edge_ip);
    }

    #[tokio::test]
    async fn host_in_tree_insert_and_remove() {
        let db = test_db().await;
        db.upsert_host(&make_host("h1", "node-1")).await.unwrap();
        let net = make_net_config();
        let t = db.allocate_tree(&net, 0).await.unwrap();
        db.insert_cluster(&make_cluster("c1", "c1", &t.id, &t.gateway_ip))
            .await
            .unwrap();

        // First insert returns true
        let first = db.upsert_host_in_tree("h1", &t.id).await.unwrap();
        assert!(first);
        // Second insert returns false (already there)
        let second = db.upsert_host_in_tree("h1", &t.id).await.unwrap();
        assert!(!second);

        // With no VMs yet, remove-if-empty succeeds
        let removed = db.remove_host_in_tree_if_empty("h1", &t.id).await.unwrap();
        assert!(removed);

        // Add a VM then upsert; remove-if-empty refuses because the VM
        // still claims the tree on this host.
        db.upsert_host_in_tree("h1", &t.id).await.unwrap();
        db.insert_vm(&make_vm("v1", "h1", "c1", &t.vm_range_start))
            .await
            .unwrap();
        let removed = db.remove_host_in_tree_if_empty("h1", &t.id).await.unwrap();
        assert!(!removed);
    }

    #[tokio::test]
    async fn list_tree_vteps_filters_empty_addresses() {
        let db = test_db().await;
        let mut h1 = make_host("h1", "node-1");
        h1.vtep_address = "10.100.0.1".to_string();
        let mut h2 = make_host("h2", "node-2");
        h2.vtep_address = String::new(); // legacy / pre-VXLAN
        db.upsert_host(&h1).await.unwrap();
        db.upsert_host(&h2).await.unwrap();

        let net = make_net_config();
        let t = db.allocate_tree(&net, 0).await.unwrap();
        db.upsert_host_in_tree("h1", &t.id).await.unwrap();
        db.upsert_host_in_tree("h2", &t.id).await.unwrap();

        let vteps = db.list_tree_vteps(&t.id).await.unwrap();
        assert_eq!(vteps, vec!["10.100.0.1".to_string()]);
    }
}
