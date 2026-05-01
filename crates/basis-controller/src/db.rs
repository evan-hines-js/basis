use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::Ipv4Addr;
use std::path::Path;
use std::str::FromStr;

use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use tracing::trace;

use basis_common::gpu::GpuInfo;

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

    /// `insert_cluster` rejected the row because another concurrent
    /// `CreateCluster` claimed the same VNI or CIDR between this
    /// caller's `allocate_cluster_network` snapshot and its insert.
    /// The pre-insert allocator only reads from `clusters`; it doesn't
    /// reserve, so two racers can pick the same (vni, cidr) and one
    /// loses the UNIQUE constraint. Caller should release any IPs it
    /// pre-allocated and retry the whole allocate-and-insert sequence.
    #[error("cluster network allocation raced a concurrent create: {0}")]
    AllocationRaced(String),

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
        // Hosts carry per-pool storage capacity in bytes. Two pools by
        // design: a thin pool for VM rootfs and a plain VG for raw data
        // disks. `*_total_bytes` is fixed at provision, refreshed on
        // every register; `*_free_bytes` is heartbeat-fresh and drives
        // the scheduler's per-pool budgets. `metadata_*_bytes` apply
        // only to the rootfs thin pool — the data VG reports zero.
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS hosts (
                id TEXT PRIMARY KEY,
                hostname TEXT NOT NULL UNIQUE,
                total_cpu INTEGER NOT NULL,
                total_memory_mib INTEGER NOT NULL,
                rootfs_total_bytes INTEGER NOT NULL DEFAULT 0,
                rootfs_free_bytes INTEGER NOT NULL DEFAULT 0,
                rootfs_metadata_total_bytes INTEGER NOT NULL DEFAULT 0,
                rootfs_metadata_free_bytes INTEGER NOT NULL DEFAULT 0,
                data_total_bytes INTEGER NOT NULL DEFAULT 0,
                data_free_bytes INTEGER NOT NULL DEFAULT 0,
                gpu_inventory TEXT NOT NULL DEFAULT '[]',
                vtep_address TEXT NOT NULL DEFAULT '',
                last_heartbeat TEXT NOT NULL,
                healthy INTEGER NOT NULL DEFAULT 1,
                rank INTEGER NOT NULL DEFAULT 0,
                labels TEXT NOT NULL DEFAULT '{}'
            )",
        )
        .execute(&self.writer)
        .await?;

        // Per-cluster network identity:
        //   * `vni` — VXLAN Network Identifier, unique cell-wide.
        //   * `cidr` — overlay CIDR carved from `network.clusterSupernet`.
        //     First usable = anycast gateway, last usable = apiserver VIP
        //     (when private), the rest = VM IPs.
        //   * `external_pool` — LAN-routable pool the LB Service block
        //     (and the apiserver VIP, if `apiserver_visibility = PUBLIC`)
        //     are allocated from.
        //   * `service_block_cidr` — LoadBalancer Service block CIDR
        //     (e.g. `10.0.0.224/28`). Empty when the cluster requested
        //     zero service IPs.
        //   * `apiserver_visibility` — `0` = PUBLIC (apiserver VIP from
        //     the pool, BGP-advertised cell-wide), `1` = PRIVATE
        //     (apiserver VIP from `cidr`, never advertised). Stored as
        //     i64 to match the proto enum.
        //   * `trust_domain` — string label that scopes the eager-
        //     bootstrap fan-out. A tree-scoped cluster is fanned out
        //     only to hosts that already carry a cluster with the
        //     same trust_domain. Cross-trust-domain traffic is
        //     blocked at the dataplane by the absence of a kernel
        //     route — no BGP filtering involved (tree VIPs aren't
        //     advertised). Empty string ("") is its own group:
        //     joins other empty-trust_domain clusters, doesn't
        //     merge with named ones.
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS clusters (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                vni INTEGER NOT NULL UNIQUE,
                cidr TEXT NOT NULL,
                bridge_range_start TEXT NOT NULL,
                bridge_range_end TEXT NOT NULL,
                vm_range_start TEXT NOT NULL,
                vm_range_end TEXT NOT NULL,
                prefix_len INTEGER NOT NULL,
                control_plane_endpoint TEXT NOT NULL,
                apiserver_visibility INTEGER NOT NULL DEFAULT 0,
                external_pool TEXT NOT NULL,
                service_block_cidr TEXT NOT NULL DEFAULT '',
                trust_domain TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL
            )",
        )
        .execute(&self.writer)
        .await?;

        // Per-(cluster, host) membership + bridge IP, plus the
        // tombstone state machine that drives reconcile emission.
        //
        // Every hypervisor carrying a VM in a cluster owns a unique
        // address from the cluster's `bridge_range` and assigns it to
        // its local `brc<vni>`. VMs use their own host's bridge IP as
        // default gateway so cross-host replies routing back through
        // the gateway land on the originating hypervisor — anycast on
        // a shared IP would require EVPN-style ARP suppression we
        // don't have yet.
        //
        // `state` tracks the tombstone lifecycle:
        //   * 0 (ACTIVE): host should carry the cluster. Emitted in
        //     `ReconcileHostCommand.clusters[]`.
        //   * 1 (PENDING_TEARDOWN): the last VM of the cluster on
        //     this host has been removed (or DeleteCluster ran).
        //     Emitted in `ReconcileHostCommand.cluster_tombstones[]`.
        //     Row deleted on `TombstoneAck`. A fresh CreateMachine
        //     for the same (host, cluster) flips state back to ACTIVE
        //     before any tombstone fires — resurrection cancels
        //     teardown, which absorbs CAPI delete-then-recreate
        //     churn without flapping bridges.
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS host_clusters (
                cluster_id TEXT NOT NULL REFERENCES clusters(id),
                host_id TEXT NOT NULL REFERENCES hosts(id),
                ip_address TEXT NOT NULL,
                state INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (cluster_id, host_id),
                UNIQUE (cluster_id, ip_address)
            )",
        )
        .execute(&self.writer)
        .await?;

        // `disk_gib` is the rootfs LV size (rootfs thin pool). Data
        // disks are tracked by the JSON `extra_disk_gibs` column —
        // single source of truth, no denormalised per-row sum. The
        // scheduler's capacity gate sums them per-host on the fly via
        // `json_each(extra_disk_gibs)`.
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
                extra_disk_gibs TEXT NOT NULL DEFAULT '[]',
                image TEXT NOT NULL,
                error_message TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )",
        )
        .execute(&self.writer)
        .await?;

        // Sticky LAN-VIP owner per cluster. The agent on `host_id` is
        // the single LAN proxy-ARP responder for this cluster's
        // `cluster_vips`; non-owner carriers install routes + BGP-
        // advertise but skip proxy-ARP. Sticky: a new carrier joining
        // does NOT displace the existing owner — re-election only
        // fires when the recorded owner is no longer in
        // `host_clusters` with state=ACTIVE. Without sticky, ownership
        // would shift on UUID-sort ordering of carriers and the
        // resulting owner-change reconcile black-holed LAN traffic to
        // the apiserver VIP whenever a worker landed on a host whose
        // host_id sorted lower than the existing carrier.
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS cluster_lan_vip_owner (
                cluster_id TEXT PRIMARY KEY REFERENCES clusters(id) ON DELETE CASCADE,
                host_id TEXT NOT NULL,
                elected_at TEXT NOT NULL
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

    // --- Cluster network allocation ---

    /// Atomically allocate a fresh `(vni, cidr)` pair for a new
    /// cluster: pick the next free VNI from the configured range and
    /// carve the next free `/cluster_prefix` slice out of
    /// `network.clusterSupernet`, plus the bridge_range (low end of
    /// the CIDR for per-host bridge IPs) and vm_range (the rest, less
    /// the broadcast). Returned struct is the cluster's network
    /// identity; the caller writes the cluster row via
    /// `insert_cluster`.
    pub async fn allocate_cluster_network(
        &self,
        net: &NetworkConfig,
    ) -> Result<ClusterNetwork, DbError> {
        let mut tx = self.writer.begin().await?;

        let taken: Vec<(i64, String)> = sqlx::query_as("SELECT vni, cidr FROM clusters")
            .fetch_all(&mut *tx)
            .await?;
        let used_vnis: HashSet<u32> = taken.iter().map(|(v, _)| *v as u32).collect();
        let mut used_cidrs: Vec<ipnet::Ipv4Net> = Vec::with_capacity(taken.len());
        for (_, c) in &taken {
            used_cidrs.push(
                c.parse()
                    .map_err(|e| DbError::Malformed(format!("clusters.cidr '{c}': {e}")))?,
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
            .cluster_supernet
            .parse()
            .map_err(|e| DbError::Malformed(format!("cluster_supernet: {e}")))?;
        let candidate = supernet
            .subnets(net.cluster_prefix)
            .map_err(|e| DbError::Malformed(format!("cluster_prefix: {e}")))?
            .find(|c| !used_cidrs.iter().any(|u| cidrs_overlap(u, c)))
            .ok_or_else(|| {
                DbError::Exhausted(format!(
                    "cluster supernet {} fully carved into /{} slices",
                    net.cluster_supernet, net.cluster_prefix
                ))
            })?;

        // Commit happens implicitly when the caller's `insert_cluster`
        // runs against the writer pool — they'll either succeed and
        // race-protect the (vni, cidr) pair via the UNIQUE constraint
        // on `clusters.vni`, or fail and leave nothing behind.
        tx.commit().await?;

        Ok(ClusterNetwork::carve(
            vni,
            candidate,
            net.cluster_prefix,
            net.bridge_reserve,
        ))
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

    /// Allocate the next free VM IP from the cluster's overlay CIDR.
    /// VMs come out of `vm_range` (above the bridge reserve, below
    /// the apiserver VIP if private). The apiserver VIP, when
    /// private, is recorded in `ip_allocations` at cluster create
    /// time so this allocator's "skip already-taken" path avoids it.
    pub async fn allocate_cluster_vm_ip(
        &self,
        cluster: &ClusterRow,
        vm_id: &str,
    ) -> Result<String, DbError> {
        let range = cluster.vm_range()?;
        let scope = format!("cluster:{}", cluster.id);
        self.allocate_from_range(&scope, &range, Some(vm_id), None)
            .await
    }

    /// Reserve a specific IP under `scope`, attributed to either a VM
    /// or a cluster. Used for the private apiserver VIP at cluster
    /// create — the IP is deterministic (last usable in cluster CIDR),
    /// so we can't go through the find-next-free allocator. Fails
    /// with `Conflict` if the address is already taken.
    pub async fn reserve_specific_ip(
        &self,
        scope: &str,
        ip: &str,
        vm_id: Option<&str>,
        cluster_id: Option<&str>,
    ) -> Result<(), DbError> {
        debug_assert!(
            vm_id.is_some() != cluster_id.is_some(),
            "exactly one of vm_id / cluster_id must be set",
        );
        sqlx::query(
            "INSERT INTO ip_allocations (ip_address, scope, vm_id, cluster_id) \
             VALUES (?, ?, ?, ?)",
        )
        .bind(ip)
        .bind(scope)
        .bind(vm_id)
        .bind(cluster_id)
        .execute(&self.writer)
        .await
        .map_err(|e| match e {
            sqlx::Error::Database(db_err) if db_err.is_unique_violation() => {
                DbError::Conflict(format!("ip {ip} already allocated in scope '{scope}'"))
            }
            other => DbError::Sqlx(other),
        })?;
        Ok(())
    }

    // --- Per-(host, cluster) membership + tombstone lifecycle ---

    /// Find-or-(re)activate the (host, cluster) row, returning the
    /// host's bridge IP for that cluster. On first call for a pair,
    /// picks the lowest free address in the cluster's `bridge_range`
    /// and inserts the row in state ACTIVE. On a row that's already
    /// PENDING_TEARDOWN, flips state back to ACTIVE and returns the
    /// pre-existing IP — resurrection cancels the pending tombstone
    /// without ever emitting it on the wire. This is what absorbs
    /// CAPI delete-then-recreate churn cleanly.
    pub async fn ensure_host_cluster_active(
        &self,
        cluster: &ClusterRow,
        host_id: &str,
    ) -> Result<String, DbError> {
        let range = cluster.bridge_range()?;
        let mut tx = self.writer.begin().await?;

        // Existing row: bump back to ACTIVE if it was pending teardown
        // and return its IP unchanged. Same code path for plain ACTIVE
        // — UPDATE is a no-op there.
        if let Some((ip,)) = sqlx::query_as::<_, (String,)>(
            "UPDATE host_clusters SET state = 0
             WHERE cluster_id = ? AND host_id = ?
             RETURNING ip_address",
        )
        .bind(&cluster.id)
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
                        NOT IN (SELECT ip_address FROM host_clusters
                                WHERE cluster_id = ?)
                  ORDER BY n
                  LIMIT 1
              )
            INSERT INTO host_clusters (cluster_id, host_id, ip_address, state)
            SELECT ?, ?, ip, 0 FROM picked
            RETURNING ip_address
            "#,
        )
        .bind(range.start as i64)
        .bind(range.end as i64)
        .bind(&cluster.id)
        .bind(&cluster.id)
        .bind(host_id)
        .fetch_optional(&mut *tx)
        .await?;

        tx.commit().await?;

        allocated.ok_or_else(|| {
            DbError::Exhausted(format!(
                "cluster {} bridge_range [{}..={}] fully allocated",
                cluster.id,
                Ipv4Addr::from(range.start),
                Ipv4Addr::from(range.end),
            ))
        })
    }

    /// Mark (host, cluster) as PENDING_TEARDOWN iff no live VMs of the
    /// cluster remain on the host. Idempotent: an already-pending row
    /// stays pending; a missing row is a no-op (cluster was never
    /// scheduled here, or already acked away). Live VMs are those NOT
    /// in PENDING_TEARDOWN — a VM still being torn down doesn't keep
    /// its host's cluster row pinned, since both will tombstone
    /// together on the same reconcile.
    pub async fn mark_host_cluster_pending_teardown(
        &self,
        cluster_id: &str,
        host_id: &str,
    ) -> Result<(), DbError> {
        sqlx::query(
            "UPDATE host_clusters SET state = 1
             WHERE cluster_id = ? AND host_id = ?
               AND NOT EXISTS (
                   SELECT 1 FROM vms
                   WHERE host_id = ? AND cluster_id = ?
                     AND state != ?
               )",
        )
        .bind(cluster_id)
        .bind(host_id)
        .bind(host_id)
        .bind(cluster_id)
        .bind(VM_STATE_PENDING_TEARDOWN)
        .execute(&self.writer)
        .await?;
        Ok(())
    }

    /// Mark every (host, cluster) row across all hosts pending teardown.
    /// Used by DeleteCluster — the cluster is going away cell-wide so
    /// every host carrying it must release its bridge.
    pub async fn mark_cluster_pending_teardown_all_hosts(
        &self,
        cluster_id: &str,
    ) -> Result<Vec<String>, DbError> {
        let hosts: Vec<String> = sqlx::query_scalar(
            "UPDATE host_clusters SET state = 1
             WHERE cluster_id = ?
             RETURNING host_id",
        )
        .bind(cluster_id)
        .fetch_all(&self.writer)
        .await?;
        Ok(hosts)
    }

    /// Active (state=ACTIVE) host_clusters rows for `host_id` joined
    /// with their cluster — drives `ReconcileHostCommand.clusters[]`.
    pub async fn list_active_host_clusters(
        &self,
        host_id: &str,
    ) -> Result<Vec<HostClusterMembership>, DbError> {
        Ok(sqlx::query_as::<_, HostClusterMembership>(
            "SELECT c.*, hc.ip_address AS bridge_ip
             FROM clusters c
             JOIN host_clusters hc ON hc.cluster_id = c.id
             WHERE hc.host_id = ? AND hc.state = 0",
        )
        .bind(host_id)
        .fetch_all(&self.reader)
        .await?)
    }

    /// Pending-teardown host_clusters rows for `host_id` — drives
    /// `ReconcileHostCommand.cluster_tombstones[]`. Returns
    /// (cluster_id, vni, cidr) for the agent's teardown path.
    pub async fn list_pending_cluster_tombstones(
        &self,
        host_id: &str,
    ) -> Result<Vec<PendingClusterTombstone>, DbError> {
        Ok(sqlx::query_as::<_, PendingClusterTombstone>(
            "SELECT c.id AS cluster_id, c.vni, c.cidr
             FROM clusters c
             JOIN host_clusters hc ON hc.cluster_id = c.id
             WHERE hc.host_id = ? AND hc.state = 1",
        )
        .bind(host_id)
        .fetch_all(&self.reader)
        .await?)
    }

    /// Allocate an aligned `count`-sized block to a cluster from the
    /// given range, returning its CIDR (e.g. `10.0.0.224/28`). All
    /// addresses in the block are reserved under `cluster_id` so
    /// `release_cluster_ips` frees the whole block on cluster delete.
    ///
    /// `count` must be a power of two; the allocator fails fast with
    /// `Malformed` otherwise. `scope` is the same scope key used by
    /// the per-IP allocators that draw from this range, so apiserver
    /// VIPs and Service blocks share allocation state and never
    /// collide.
    pub async fn allocate_service_block(
        &self,
        scope: &str,
        range: &ParsedRange,
        cluster_id: &str,
        count: u32,
    ) -> Result<String, DbError> {
        if count == 0 || !count.is_power_of_two() {
            return Err(DbError::Malformed(format!(
                "service block count {count} must be a power of two",
            )));
        }
        let prefix_len: u8 = 32 - count.trailing_zeros() as u8;

        let mut tx = self.writer.begin().await?;
        // Pull the current allocations + registered host underlay IPs
        // in one pass so the alignment search runs in memory — the
        // range's typical width is well under 256 IPs, and the writer
        // is single-threaded so serialization with concurrent
        // allocations is automatic. Host vtep_addresses are excluded
        // for the same reason as in `allocate_from_range`: when the
        // pool overlaps the host underlay range, an aligned block
        // that contains a hypervisor's IP would steal LAN traffic for
        // that host via the leader-host's proxy-ARP advertisement.
        let used_allocs: Vec<String> =
            sqlx::query_scalar("SELECT ip_address FROM ip_allocations WHERE scope = ?")
                .bind(scope)
                .fetch_all(&mut *tx)
                .await?;
        let host_vteps: Vec<String> =
            sqlx::query_scalar("SELECT vtep_address FROM hosts WHERE vtep_address != ''")
                .fetch_all(&mut *tx)
                .await?;
        let used: std::collections::HashSet<u32> = used_allocs
            .into_iter()
            .chain(host_vteps)
            .filter_map(|s| s.parse::<Ipv4Addr>().ok().map(u32::from))
            .collect();

        let mut start = (range.start + (count - 1)) & !(count - 1); // align up
        let cidr = loop {
            let end = match start.checked_add(count - 1) {
                Some(e) if e <= range.end => e,
                _ => {
                    return Err(DbError::Exhausted(format!(
                        "no aligned /{prefix_len} block free in scope '{scope}' \
                         range [{}..={}]",
                        Ipv4Addr::from(range.start),
                        Ipv4Addr::from(range.end),
                    )));
                }
            };
            if (start..=end).all(|n| !used.contains(&n)) {
                for n in start..=end {
                    let ip = Ipv4Addr::from(n).to_string();
                    sqlx::query(
                        "INSERT INTO ip_allocations (ip_address, scope, vm_id, cluster_id)
                         VALUES (?, ?, NULL, ?)",
                    )
                    .bind(&ip)
                    .bind(scope)
                    .bind(cluster_id)
                    .execute(&mut *tx)
                    .await?;
                }
                break format!("{}/{prefix_len}", Ipv4Addr::from(start));
            }
            start = match start.checked_add(count) {
                Some(s) => s,
                None => {
                    return Err(DbError::Exhausted(format!(
                        "no aligned /{prefix_len} block free in scope '{scope}'"
                    )));
                }
            };
        };
        tx.commit().await?;
        Ok(cidr)
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
        // Exclude registered host underlay IPs from the candidate set:
        // when a LAN-routable pool (cell-public) overlaps the host
        // underlay range, the allocator could otherwise hand out an
        // address that's already a hypervisor's primary IP. The agent
        // would then install proxy-ARP on the leader host for that IP
        // and steal the LAN's frames for the real host. Empty
        // vtep_address (pre-VXLAN agent or just-registered host) is
        // ignored — the second condition guards that.
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
                    AND printf('%d.%d.%d.%d',
                               (n >> 24) & 255, (n >> 16) & 255,
                               (n >>  8) & 255,  n        & 255)
                        NOT IN (SELECT vtep_address FROM hosts
                                WHERE vtep_address != '')
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
            "INSERT INTO clusters (
                id, name, vni, cidr,
                bridge_range_start, bridge_range_end,
                vm_range_start, vm_range_end,
                prefix_len, control_plane_endpoint,
                apiserver_visibility, external_pool,
                service_block_cidr, trust_domain, created_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&cluster.id)
        .bind(&cluster.name)
        .bind(cluster.vni)
        .bind(&cluster.cidr)
        .bind(&cluster.bridge_range_start)
        .bind(&cluster.bridge_range_end)
        .bind(&cluster.vm_range_start)
        .bind(&cluster.vm_range_end)
        .bind(cluster.prefix_len)
        .bind(&cluster.control_plane_endpoint)
        .bind(cluster.apiserver_visibility)
        .bind(&cluster.external_pool)
        .bind(&cluster.service_block_cidr)
        .bind(&cluster.trust_domain)
        .bind(&cluster.created_at)
        .execute(&self.writer)
        .await
        .map_err(|e| match e {
            sqlx::Error::Database(db_err) if db_err.is_unique_violation() => {
                // Distinguish name dup (idempotent retry) from VNI/CIDR
                // dup (allocation race). SQLite includes the constraint
                // column in the message, e.g.
                // "UNIQUE constraint failed: clusters.vni".
                let msg = db_err.message();
                if msg.contains("clusters.vni") || msg.contains("clusters.cidr") {
                    DbError::AllocationRaced(msg.to_string())
                } else {
                    DbError::Conflict(format!("cluster '{}' already exists", cluster.name))
                }
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

    /// Overwrite a cluster's `trust_domain`. Idempotent CreateCluster
    /// calls this when the request's trust_domain disagrees with the
    /// stored value — the caller is the source of truth for the live
    /// CA, and a stored row that lags (DB carried over from an
    /// install with a different CA) silently places the cluster in
    /// the wrong VRF. Caller broadcasts reconcile so agents move the
    /// bridge to the new VRF on the next pass.
    pub async fn update_cluster_trust_domain(
        &self,
        cluster_id: &str,
        trust_domain: &str,
    ) -> Result<(), DbError> {
        sqlx::query("UPDATE clusters SET trust_domain = ? WHERE id = ?")
            .bind(trust_domain)
            .bind(cluster_id)
            .execute(&self.writer)
            .await?;
        Ok(())
    }

    /// Hard-delete a cluster row. Caller is responsible for ensuring
    /// every (host, cluster) row has already been tombstoned and acked
    /// — DeleteCluster's flow marks each host pending and waits for
    /// acks before invoking this. The FK on host_clusters fires
    /// noisily if you skip that step, which is the desired behaviour.
    pub async fn delete_cluster(&self, id: &str) -> Result<(), DbError> {
        // `cluster_lan_vip_owner` cascades via ON DELETE CASCADE.
        sqlx::query("DELETE FROM clusters WHERE id = ?")
            .bind(id)
            .execute(&self.writer)
            .await?;
        Ok(())
    }

    /// Resolve the sticky LAN-VIP owner for `cluster_id` against the
    /// current carrier set: keep the recorded owner if still active,
    /// else pick the lowest-id survivor and persist. Returns the
    /// elected owner (empty string if `carriers` is empty — a fresh
    /// cluster with no placement yet, in which case agents skip
    /// proxy-ARP for every entry).
    ///
    /// Idempotent across concurrent `build_reconcile_command` runs:
    /// every host computes the same election from the same input, so
    /// the `INSERT OR REPLACE` is a no-op write when the owner hasn't
    /// changed.
    ///
    /// Sticky on member-add (the bug fix): a worker placement adds a
    /// carrier, but `prev` is still in `carriers`, so the recorded
    /// owner stays. Re-election fires only when `prev` left
    /// `host_clusters` — which is the correct trigger for ownership
    /// transfer (drain the old owner first, then GARP from the new).
    pub async fn elect_lan_vip_owner(
        &self,
        cluster_id: &str,
        carriers: &[String],
    ) -> Result<String, DbError> {
        let prev: Option<String> =
            sqlx::query_scalar("SELECT host_id FROM cluster_lan_vip_owner WHERE cluster_id = ?")
                .bind(cluster_id)
                .fetch_optional(&self.reader)
                .await?;

        let prev_owner = prev.clone();
        if let Some(p) = prev {
            if carriers.iter().any(|h| h == &p) {
                return Ok(p);
            }
        }

        let new_owner = match carriers.first() {
            Some(h) => h.clone(),
            None => return Ok(String::new()),
        };
        let now = basis_common::time::now_rfc3339();
        sqlx::query(
            "INSERT OR REPLACE INTO cluster_lan_vip_owner
             (cluster_id, host_id, elected_at) VALUES (?, ?, ?)",
        )
        .bind(cluster_id)
        .bind(&new_owner)
        .bind(&now)
        .execute(&self.writer)
        .await?;
        trace!(
            cluster_id,
            prev_owner = ?prev_owner,
            new_owner = %new_owner,
            carriers = ?carriers,
            "lan-vip owner re-elected",
        );
        Ok(new_owner)
    }

    pub async fn list_clusters(&self) -> Result<Vec<ClusterRow>, DbError> {
        Ok(sqlx::query_as::<_, ClusterRow>("SELECT * FROM clusters")
            .fetch_all(&self.reader)
            .await?)
    }

    /// VTEP addresses of every host with an ACTIVE host_clusters row
    /// for this cluster. Drives the agent-side FDB reconcile (BUM
    /// destinations). PENDING_TEARDOWN hosts are excluded — they're
    /// in the process of releasing the cluster and shouldn't be
    /// flooded with new traffic.
    pub async fn list_cluster_vteps(&self, cluster_id: &str) -> Result<Vec<String>, DbError> {
        Ok(sqlx::query_scalar(
            "SELECT DISTINCT h.vtep_address
             FROM host_clusters hc
             JOIN hosts h ON h.id = hc.host_id
             WHERE hc.cluster_id = ? AND hc.state = 0
               AND h.vtep_address != ''",
        )
        .bind(cluster_id)
        .fetch_all(&self.reader)
        .await?)
    }

    /// Host IDs currently carrying VMs in this cluster.
    pub async fn list_hosts_in_cluster(&self, cluster_id: &str) -> Result<Vec<String>, DbError> {
        let rows: Vec<(String,)> =
            sqlx::query_as("SELECT DISTINCT host_id FROM vms WHERE cluster_id = ?")
                .bind(cluster_id)
                .fetch_all(&self.reader)
                .await?;
        Ok(rows.into_iter().map(|(h,)| h).collect())
    }

    /// Host IDs with an ACTIVE host_clusters row for this cluster AND
    /// a healthy host record, in deterministic order (lowest host_id
    /// first). Drives controller-side LAN-VIP owner election: the
    /// first entry is the host that owns proxy-ARP for the cluster's
    /// `cluster_vips` on every other host's reconcile pass.
    ///
    /// Joining on `hosts.healthy = 1` makes ownership reactive to the
    /// existing host health-check signal. Without the join, a hard-
    /// crashed host whose `host_clusters` rows haven't been cleaned up
    /// stays "elected" — the dead host doesn't proxy-ARP, no host
    /// does, and LAN ingress to the cluster's VIPs black-holes until
    /// the membership rows finally clear. Gating on healthy means the
    /// next reconcile after the heartbeat watchdog flips `healthy=0`
    /// re-elects to a live carrier.
    ///
    /// Stable identity → no ownership flap on reconciles that don't
    /// change the carrier set; deterministic across controllers
    /// (no random tiebreak) so a controller failover doesn't move
    /// ownership either. PENDING_TEARDOWN carriers are excluded — they
    /// shouldn't take ownership of anything they're about to release.
    pub async fn list_active_carriers(&self, cluster_id: &str) -> Result<Vec<String>, DbError> {
        Ok(sqlx::query_scalar(
            "SELECT hc.host_id FROM host_clusters hc
             JOIN hosts h ON h.id = hc.host_id
             WHERE hc.cluster_id = ? AND hc.state = 0 AND h.healthy = 1
             ORDER BY hc.host_id ASC",
        )
        .bind(cluster_id)
        .fetch_all(&self.reader)
        .await?)
    }

    // --- Hosts ---

    pub async fn upsert_host(&self, host: &HostRow) -> Result<(), DbError> {
        sqlx::query(
            "INSERT INTO hosts (
                id, hostname, total_cpu, total_memory_mib,
                rootfs_total_bytes, rootfs_free_bytes,
                rootfs_metadata_total_bytes, rootfs_metadata_free_bytes,
                data_total_bytes, data_free_bytes,
                gpu_inventory, vtep_address, last_heartbeat, healthy, rank, labels
             )
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(id) DO UPDATE SET
                hostname = excluded.hostname,
                total_cpu = excluded.total_cpu,
                total_memory_mib = excluded.total_memory_mib,
                rootfs_total_bytes = excluded.rootfs_total_bytes,
                rootfs_free_bytes = excluded.rootfs_free_bytes,
                rootfs_metadata_total_bytes = excluded.rootfs_metadata_total_bytes,
                rootfs_metadata_free_bytes = excluded.rootfs_metadata_free_bytes,
                data_total_bytes = excluded.data_total_bytes,
                data_free_bytes = excluded.data_free_bytes,
                gpu_inventory = excluded.gpu_inventory,
                vtep_address = excluded.vtep_address,
                last_heartbeat = excluded.last_heartbeat,
                healthy = excluded.healthy,
                rank = excluded.rank,
                labels = excluded.labels",
        )
        .bind(&host.id)
        .bind(&host.hostname)
        .bind(host.total_cpu)
        .bind(host.total_memory_mib)
        .bind(host.rootfs_total_bytes)
        .bind(host.rootfs_free_bytes)
        .bind(host.rootfs_metadata_total_bytes)
        .bind(host.rootfs_metadata_free_bytes)
        .bind(host.data_total_bytes)
        .bind(host.data_free_bytes)
        .bind(
            serde_json::to_string(&host.gpu_inventory)
                .expect("serializing Vec<GpuInfo> to JSON is infallible"),
        )
        .bind(&host.vtep_address)
        .bind(&host.last_heartbeat)
        .bind(host.healthy)
        .bind(host.rank)
        .bind(
            serde_json::to_string(&host.labels)
                .expect("serializing BTreeMap<String, String> to JSON is infallible"),
        )
        .execute(&self.writer)
        .await?;
        Ok(())
    }

    /// Apply a fresh storage capacity snapshot from a heartbeat. One
    /// call site so the column list lives in exactly one place. Also
    /// flips `healthy=1` and stamps `last_heartbeat` — heartbeats
    /// always carry capacity now, so collapsing the two updates avoids
    /// a separate UPDATE round-trip per tick.
    pub async fn record_heartbeat(
        &self,
        host_id: &str,
        now: &str,
        capacity: &StorageCapacityBytes,
    ) -> Result<(), DbError> {
        let result = sqlx::query(
            "UPDATE hosts SET
                last_heartbeat = ?,
                healthy = 1,
                rootfs_total_bytes = ?,
                rootfs_free_bytes = ?,
                rootfs_metadata_total_bytes = ?,
                rootfs_metadata_free_bytes = ?,
                data_total_bytes = ?,
                data_free_bytes = ?
             WHERE id = ?",
        )
        .bind(now)
        .bind(capacity.rootfs_total_bytes)
        .bind(capacity.rootfs_free_bytes)
        .bind(capacity.rootfs_metadata_total_bytes)
        .bind(capacity.rootfs_metadata_free_bytes)
        .bind(capacity.data_total_bytes)
        .bind(capacity.data_free_bytes)
        .bind(host_id)
        .execute(&self.writer)
        .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::NotFound(format!("host '{host_id}'")));
        }
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

        // Sum the new VM's data-disk footprint up front so the gate
        // doesn't have to json_each the bound JSON literal — the
        // host-side data total still uses json_each over `vms` to
        // capture every existing VM's extras.
        let data_gib_request: i64 = vm.extra_disks_sum_gib().map_err(|e| {
            DbError::Malformed(format!("vm '{}': extra_disk_gibs unparseable: {e}", vm.id))
        })?;
        let rootfs_bytes_request: i64 = vm.disk_gib.saturating_mul(GIB);
        let data_bytes_request: i64 = data_gib_request.saturating_mul(GIB);

        let affected = sqlx::query(
            "INSERT INTO vms (
                id, name, cluster_id, host_id, ip_address, state,
                cpu, memory_mib, disk_gib,
                extra_disk_gibs, image, error_message,
                created_at, updated_at)
             SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
             WHERE EXISTS (
                 SELECT 1 FROM hosts h
                 WHERE h.id = ? AND h.healthy = 1
                   AND CAST(h.total_cpu * ? AS INTEGER)
                       - COALESCE((SELECT SUM(cpu) FROM vms
                                   WHERE host_id = h.id), 0) >= ?
                   AND h.total_memory_mib
                       - COALESCE((SELECT SUM(memory_mib) FROM vms
                                   WHERE host_id = h.id), 0) >= ?
                   AND h.rootfs_total_bytes
                       - COALESCE((SELECT SUM(disk_gib) FROM vms
                                   WHERE host_id = h.id), 0) * ? >= ?
                   AND h.data_total_bytes
                       - COALESCE((SELECT SUM(je.value) FROM vms v,
                                          json_each(v.extra_disk_gibs) je
                                   WHERE v.host_id = h.id), 0) * ? >= ?
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
        .bind(&vm.extra_disk_gibs)
        .bind(&vm.image)
        .bind(&vm.error_message)
        .bind(&vm.created_at)
        .bind(&vm.updated_at)
        .bind(&vm.host_id)
        .bind(self.cpu_overcommit_ratio as f64)
        .bind(vm.cpu)
        .bind(vm.memory_mib)
        .bind(GIB)
        .bind(rootfs_bytes_request)
        .bind(GIB)
        .bind(data_bytes_request)
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

    /// Full `VmRow` for every VM the controller has on this host. Used
    /// by the register handler to diff the controller's view against
    /// the agent's reported `current_inventory` and reap rows the
    /// agent says it doesn't have anymore.
    pub async fn list_vms_on_host(&self, host_id: &str) -> Result<Vec<VmRow>, DbError> {
        Ok(
            sqlx::query_as::<_, VmRow>("SELECT * FROM vms WHERE host_id = ?")
                .bind(host_id)
                .fetch_all(&self.reader)
                .await?,
        )
    }

    /// Point-in-time, per-host view of everything the scheduler cares
    /// about: consumed cpu / memory / disk, and the set of GPU PCI
    /// addresses already claimed. Read from the reader pool in a
    /// handful of queries; the writer's `insert_vm` re-validates at
    /// commit time so a stale snapshot here never over-places.
    pub async fn host_usage_snapshot(&self) -> Result<HashMap<String, HostUsage>, DbError> {
        let mut out: HashMap<String, HostUsage> = HashMap::new();

        // Consumed cpu / mem / rootfs / data per host. The data column
        // sums each VM's `extra_disk_gibs` JSON via `json_each` —
        // single source of truth for per-disk sizes lives there.
        // `LEFT JOIN` so a VM with no extras still contributes its
        // rootfs to the rollup. COALESCE covers hosts with zero VMs
        // by leaving them absent; the caller fills in
        // `HostUsage::default()` when a host isn't in the map.
        let rows = sqlx::query_as::<_, (String, i64, i64, i64, i64)>(
            "SELECT v.host_id,
                    COALESCE(SUM(v.cpu), 0),
                    COALESCE(SUM(v.memory_mib), 0),
                    COALESCE(SUM(v.disk_gib), 0),
                    COALESCE((SELECT SUM(je.value)
                              FROM vms v2, json_each(v2.extra_disk_gibs) je
                              WHERE v2.host_id = v.host_id), 0)
             FROM vms v
             GROUP BY v.host_id",
        )
        .fetch_all(&self.reader)
        .await?;
        for (host_id, cpu, mem, rootfs_gib, data_gib) in rows {
            out.insert(
                host_id,
                HostUsage {
                    used_cpu: cpu,
                    used_memory_mib: mem,
                    used_rootfs_gib: rootfs_gib,
                    used_data_gib: data_gib,
                    assigned_pci: HashSet::new(),
                    vms_by_cluster: HashMap::new(),
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

        // Per-cluster VM counts per host — feeds the scheduler's soft
        // anti-affinity tie-break so a cluster's VMs prefer hosts that
        // don't already run a sibling.
        let cluster_counts = sqlx::query_as::<_, (String, String, i64)>(
            "SELECT host_id, cluster_id, COUNT(*) FROM vms GROUP BY host_id, cluster_id",
        )
        .fetch_all(&self.reader)
        .await?;
        for (host_id, cluster_id, count) in cluster_counts {
            out.entry(host_id)
                .or_default()
                .vms_by_cluster
                .insert(cluster_id, count as u32);
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

    /// Mark a VM as PENDING_TEARDOWN. The vms row is *not* deleted —
    /// it's kept so the controller's reconcile keeps re-emitting the
    /// `vm_tombstones[]` entry until the agent acks. On ack, the row
    /// is deleted via `ack_tombstones` and IP/GPU allocations are
    /// released.
    pub async fn mark_vm_pending_teardown(&self, vm_id: &str) -> Result<(), DbError> {
        let now = basis_common::time::now_rfc3339();
        let result = sqlx::query("UPDATE vms SET state = ?, updated_at = ? WHERE id = ?")
            .bind(VM_STATE_PENDING_TEARDOWN)
            .bind(&now)
            .bind(vm_id)
            .execute(&self.writer)
            .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::NotFound(format!("vm '{vm_id}'")));
        }
        Ok(())
    }

    /// VM IDs on `host_id` whose state is PENDING_TEARDOWN — drives
    /// `ReconcileHostCommand.vm_tombstones[]`.
    pub async fn list_pending_vm_tombstones(&self, host_id: &str) -> Result<Vec<String>, DbError> {
        Ok(
            sqlx::query_scalar("SELECT id FROM vms WHERE host_id = ? AND state = ?")
                .bind(host_id)
                .bind(VM_STATE_PENDING_TEARDOWN)
                .fetch_all(&self.reader)
                .await?,
        )
    }

    /// Apply a `TombstoneAck` from the agent atomically: drop the
    /// matching pending host_clusters and vms rows, releasing their
    /// IP/GPU allocations. Each list is filtered to actually-pending
    /// rows so a stale ack (e.g. from a re-emitted tombstone the
    /// controller already cleared) is a no-op rather than a corruption.
    pub async fn ack_tombstones(
        &self,
        host_id: &str,
        cluster_vnis: &[u32],
        vm_ids: &[String],
    ) -> Result<(), DbError> {
        if cluster_vnis.is_empty() && vm_ids.is_empty() {
            return Ok(());
        }
        let mut tx = self.writer.begin().await?;

        // VMs first: deleting the vm row before the host_cluster row
        // means the (host_cluster, vm) FK invariant ("a host_cluster
        // row cannot have live VMs") holds throughout. ip_allocations
        // entries scoped to the vm are released via release_vm_ips.
        for vm_id in vm_ids {
            sqlx::query("DELETE FROM ip_allocations WHERE vm_id = ?")
                .bind(vm_id)
                .execute(&mut *tx)
                .await?;
            sqlx::query("DELETE FROM vms WHERE id = ? AND host_id = ? AND state = ?")
                .bind(vm_id)
                .bind(host_id)
                .bind(VM_STATE_PENDING_TEARDOWN)
                .execute(&mut *tx)
                .await?;
        }

        // Cluster tombstones: delete only rows that are actually
        // PENDING_TEARDOWN — defends against an ack that races a
        // resurrection (the row went ACTIVE again before the ack
        // landed; we don't want to drop it). After the row is gone,
        // if the cluster has zero remaining host_clusters rows AND
        // zero vms rows, it's fully drained — release its VIP
        // allocations and delete the cluster row. This is the only
        // path that completes a DeleteCluster.
        let mut drained_clusters: Vec<String> = Vec::new();
        for vni in cluster_vnis {
            let cluster_id: Option<String> =
                sqlx::query_scalar("SELECT id FROM clusters WHERE vni = ?")
                    .bind(*vni as i64)
                    .fetch_optional(&mut *tx)
                    .await?;
            let Some(cluster_id) = cluster_id else {
                continue;
            };

            sqlx::query(
                "DELETE FROM host_clusters
                 WHERE host_id = ? AND state = 1 AND cluster_id = ?",
            )
            .bind(host_id)
            .bind(&cluster_id)
            .execute(&mut *tx)
            .await?;

            // Cascade: drop the cluster + its VIP allocations once
            // every host has released and every VM is gone.
            let remaining: i64 = sqlx::query_scalar(
                "SELECT
                    (SELECT COUNT(*) FROM host_clusters WHERE cluster_id = ?) +
                    (SELECT COUNT(*) FROM vms WHERE cluster_id = ?)",
            )
            .bind(&cluster_id)
            .bind(&cluster_id)
            .fetch_one(&mut *tx)
            .await?;
            if remaining == 0 {
                sqlx::query("DELETE FROM ip_allocations WHERE cluster_id = ?")
                    .bind(&cluster_id)
                    .execute(&mut *tx)
                    .await?;
                sqlx::query("DELETE FROM clusters WHERE id = ?")
                    .bind(&cluster_id)
                    .execute(&mut *tx)
                    .await?;
                drained_clusters.push(cluster_id);
            }
        }

        tx.commit().await?;
        if !drained_clusters.is_empty() {
            tracing::info!(
                clusters = ?drained_clusters,
                "tombstone ack drained final host_clusters rows; cluster + VIP allocations released",
            );
        }
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
    // is what cluster-CIDR carving hits.
    a.contains(&b.network()) || b.contains(&a.network())
}

// --- Row types ---

/// Network identity assigned to a fresh cluster. Returned by
/// [`Db::allocate_cluster_network`]; the caller writes the cluster
/// row via `insert_cluster`. Derivative addresses (apiserver VIP when
/// `APISERVER_PRIVATE`) come from `private_apiserver_ip`.
#[derive(Debug, Clone, Copy)]
pub struct ClusterNetwork {
    pub vni: u32,
    pub cidr: ipnet::Ipv4Net,
    pub prefix_len: u8,
    /// Per-host bridge IP range — the bottom slice of `cidr`,
    /// `bridge_reserve` addresses wide.
    pub bridge_start: Ipv4Addr,
    pub bridge_end: Ipv4Addr,
    /// VM IP range — everything between `bridge_end` and the broadcast
    /// (less the apiserver VIP slot when `APISERVER_PRIVATE`, but the
    /// allocator skips taken IPs at allocate time so the range itself
    /// doesn't shrink).
    pub vm_start: Ipv4Addr,
    pub vm_end: Ipv4Addr,
}

impl ClusterNetwork {
    fn carve(vni: u32, cidr: ipnet::Ipv4Net, prefix_len: u8, bridge_reserve: u32) -> Self {
        let net = u32::from(cidr.network());
        let bcast = u32::from(cidr.broadcast());
        let bridge_start = net + 1;
        let bridge_end = net + bridge_reserve;
        let vm_start = bridge_end + 1;
        let vm_end = bcast - 1;
        Self {
            vni,
            cidr,
            prefix_len,
            bridge_start: Ipv4Addr::from(bridge_start),
            bridge_end: Ipv4Addr::from(bridge_end),
            vm_start: Ipv4Addr::from(vm_start),
            vm_end: Ipv4Addr::from(vm_end),
        }
    }

    /// Last usable address — the cluster's apiserver VIP when
    /// `APISERVER_PRIVATE`. Sits at the very top of the CIDR (the VM
    /// range stops one short, so vm + apiserver never collide).
    pub fn private_apiserver_ip(&self) -> Ipv4Addr {
        Ipv4Addr::from(u32::from(self.cidr.broadcast()) - 1)
    }
}

/// Bytes everywhere — the GiB conversion happens at scheduler /
/// metrics consumption sites, never in storage. `metadata_*` apply
/// only to the rootfs thin pool; the data VG reports zero.
#[derive(Debug, Clone, Copy, Default)]
pub struct StorageCapacityBytes {
    pub rootfs_total_bytes: i64,
    pub rootfs_free_bytes: i64,
    pub rootfs_metadata_total_bytes: i64,
    pub rootfs_metadata_free_bytes: i64,
    pub data_total_bytes: i64,
    pub data_free_bytes: i64,
}

impl StorageCapacityBytes {
    /// Build from the agent-side proto. Single boundary conversion;
    /// callers don't need to know the field shape on either side.
    pub fn from_proto(c: &basis_proto::StorageCapacity) -> Self {
        let rootfs = c.rootfs.as_ref();
        let data = c.data.as_ref();
        Self {
            rootfs_total_bytes: rootfs.map(|p| p.total_bytes as i64).unwrap_or(0),
            rootfs_free_bytes: rootfs.map(|p| p.free_bytes as i64).unwrap_or(0),
            rootfs_metadata_total_bytes: rootfs.map(|p| p.metadata_total_bytes as i64).unwrap_or(0),
            rootfs_metadata_free_bytes: rootfs.map(|p| p.metadata_free_bytes as i64).unwrap_or(0),
            data_total_bytes: data.map(|p| p.total_bytes as i64).unwrap_or(0),
            data_free_bytes: data.map(|p| p.free_bytes as i64).unwrap_or(0),
        }
    }
}

/// One gibibyte in bytes. Used wherever the agent reports bytes but
/// the scheduler / VM accounting works in GiB.
pub const GIB: i64 = 1 << 30;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct HostRow {
    pub id: String,
    pub hostname: String,
    pub total_cpu: i64,
    pub total_memory_mib: i64,
    /// Rootfs thin pool capacity (bytes). `_total_*` is set on
    /// register; `_free_*` is heartbeat-fresh. `metadata_*` is the
    /// thin pool's separate metadata extent.
    pub rootfs_total_bytes: i64,
    pub rootfs_free_bytes: i64,
    pub rootfs_metadata_total_bytes: i64,
    pub rootfs_metadata_free_bytes: i64,
    /// Data VG capacity (bytes). Plain VG, no thin metadata.
    pub data_total_bytes: i64,
    pub data_free_bytes: i64,
    #[sqlx(json)]
    pub gpu_inventory: Vec<GpuInfo>,
    /// IP address the agent uses as the VXLAN src for outgoing tunneled
    /// frames. Reported on `RegisterHostRequest`; empty string means
    /// the agent is pre-VXLAN and cross-host traffic for any cluster
    /// overlay it carries won't reach its peers.
    pub vtep_address: String,
    pub last_heartbeat: String,
    pub healthy: bool,
    /// Operator-assigned placement preference, lower is preferred.
    /// Used by the scheduler as a tiebreaker after capacity + GPU
    /// topology + anti-affinity. Default 0 means "no preference";
    /// operators bump deprioritized hosts (e.g. consumer-disk boxes
    /// that shouldn't carry etcd) to a higher number.
    pub rank: i64,
    /// Operator-assigned labels (e.g. {"tier": "fast"}). Empty by
    /// default. Consulted by `PlacementSpec.requires` (hard filter)
    /// and `PlacementSpec.prefers` (soft tiebreak). Stored as JSON
    /// in the `labels` column — `BTreeMap` for deterministic ordering
    /// in logs, debug output, and snapshots. The schema doesn't need
    /// to know the label vocabulary up front.
    #[sqlx(json)]
    pub labels: BTreeMap<String, String>,
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
    /// Rootfs LV size in GiB. The data-disk footprint is the sum of
    /// [`Self::extra_disk_gibs`].
    pub disk_gib: i64,
    /// Per-extra-disk sizes, JSON-encoded `Vec<u32>` of gibibytes.
    /// Authoritative source of per-disk breakdown; the host-usage
    /// rollup sums via `json_each` so there's no denormalised total
    /// to drift from this.
    pub extra_disk_gibs: String,
    pub image: String,
    pub error_message: String,
    pub created_at: String,
    pub updated_at: String,
}

impl VmRow {
    pub fn extra_disks(&self) -> serde_json::Result<Vec<u32>> {
        serde_json::from_str(&self.extra_disk_gibs)
    }

    /// Sum of [`Self::extra_disk_gibs`], in GiB. Computed on demand;
    /// no DB column duplicates this.
    pub fn extra_disks_sum_gib(&self) -> serde_json::Result<i64> {
        Ok(self.extra_disks()?.into_iter().map(|g| g as i64).sum())
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
///
/// Disk usage is split per-pool: rootfs LVs contribute to
/// `used_rootfs_gib`, data disks contribute to `used_data_gib`. The
/// scheduler charges each request against the matching pool — a
/// rootfs-only VM can't use up data-pool capacity and vice versa.
#[derive(Debug, Clone, Default)]
pub struct HostUsage {
    pub used_cpu: i64,
    pub used_memory_mib: i64,
    pub used_rootfs_gib: i64,
    pub used_data_gib: i64,
    pub assigned_pci: HashSet<String>,
    /// Number of VMs from each cluster currently on this host. Drives
    /// soft anti-affinity in the scheduler so a cluster's VMs spread
    /// across hosts before bin-packing decides where to land.
    pub vms_by_cluster: HashMap<String, u32>,
}

/// Wire-format value of `MachineState::PENDING_TEARDOWN` — the VM
/// row is being kept alive solely so the controller's reconcile
/// keeps re-emitting the matching `vm_tombstones[]` entry until the
/// agent acks. Centralised so SQL filters and proto conversions
/// agree on a single number.
pub const VM_STATE_PENDING_TEARDOWN: i64 = basis_proto::MachineState::PendingTeardown as i64;

/// Active host_clusters row joined with the cluster — drives
/// `ReconcileHostCommand.clusters[]`. The `bridge_ip` is this host's
/// gateway IP for the cluster's overlay (carved from `bridge_range`).
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct HostClusterMembership {
    pub id: String,
    pub name: String,
    pub vni: i64,
    pub cidr: String,
    pub bridge_range_start: String,
    pub bridge_range_end: String,
    pub vm_range_start: String,
    pub vm_range_end: String,
    pub prefix_len: i64,
    pub control_plane_endpoint: String,
    pub apiserver_visibility: i64,
    pub external_pool: String,
    pub service_block_cidr: String,
    pub trust_domain: String,
    pub created_at: String,
    pub bridge_ip: String,
}

impl HostClusterMembership {
    pub fn cluster(&self) -> ClusterRow {
        ClusterRow {
            id: self.id.clone(),
            name: self.name.clone(),
            vni: self.vni,
            cidr: self.cidr.clone(),
            bridge_range_start: self.bridge_range_start.clone(),
            bridge_range_end: self.bridge_range_end.clone(),
            vm_range_start: self.vm_range_start.clone(),
            vm_range_end: self.vm_range_end.clone(),
            prefix_len: self.prefix_len,
            control_plane_endpoint: self.control_plane_endpoint.clone(),
            apiserver_visibility: self.apiserver_visibility,
            external_pool: self.external_pool.clone(),
            service_block_cidr: self.service_block_cidr.clone(),
            trust_domain: self.trust_domain.clone(),
            created_at: self.created_at.clone(),
        }
    }
}

/// Pending-teardown projection — the controller emits one of these
/// per row in `ReconcileHostCommand.cluster_tombstones[]`.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct PendingClusterTombstone {
    pub cluster_id: String,
    pub vni: i64,
    pub cidr: String,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ClusterRow {
    pub id: String,
    pub name: String,
    pub vni: i64,
    pub cidr: String,
    pub bridge_range_start: String,
    pub bridge_range_end: String,
    pub vm_range_start: String,
    pub vm_range_end: String,
    pub prefix_len: i64,
    pub control_plane_endpoint: String,
    /// `0` = `APISERVER_PUBLIC` (apiserver VIP from `external_pool`,
    /// BGP-advertised cell-wide), `1` = `APISERVER_PRIVATE`
    /// (apiserver VIP = last usable in `cidr`, never advertised).
    /// Stored as i64 to mirror the proto enum; predicate helpers
    /// (`is_apiserver_public`) hide the magic at the boundary.
    pub apiserver_visibility: i64,
    /// Pool name the cluster's external IPs were carved from at
    /// CreateCluster. Always set — every cluster needs a pool for at
    /// least its LB Service block; the apiserver VIP additionally
    /// comes from this pool when `apiserver_visibility = PUBLIC`.
    pub external_pool: String,
    /// CIDR of this cluster's LoadBalancer Service block. Empty when
    /// the cluster asked for 0 service IPs.
    pub service_block_cidr: String,
    /// Trust-domain label that scopes the controller's eager-bootstrap
    /// fan-out for tree-scoped pool VIPs (see
    /// `build_reconcile_command`). Empty is its own group.
    pub trust_domain: String,
    pub created_at: String,
}

/// Identity + intent the caller has already settled on by the time
/// `ClusterRow::from_network` runs. Bundling these into a struct
/// keeps the constructor below from sprouting an unwieldy positional
/// argument list (the same data still flows through, just grouped by
/// "what the caller knows" vs "what the allocator allocated").
pub struct ClusterIdentity {
    pub id: String,
    pub name: String,
    pub control_plane_endpoint: String,
    pub apiserver_visibility: i64,
    pub external_pool: String,
    pub service_block_cidr: String,
    pub trust_domain: String,
    pub created_at: String,
}

impl ClusterRow {
    /// True iff the cluster's apiserver VIP is allocated from the
    /// external pool and BGP-advertised cell-wide (proto enum value
    /// `APISERVER_PUBLIC`). Predicate hides the i64-vs-enum boundary.
    pub fn is_apiserver_public(&self) -> bool {
        self.apiserver_visibility == 0
    }

    /// Build a row from the allocator's output and the caller's
    /// pre-decided identity, ready to write via `insert_cluster`.
    /// Centralises the conversion so server.rs doesn't reach into
    /// the allocator's fields directly.
    pub fn from_network(identity: ClusterIdentity, network: ClusterNetwork) -> Self {
        Self {
            id: identity.id,
            name: identity.name,
            vni: network.vni as i64,
            cidr: network.cidr.to_string(),
            bridge_range_start: network.bridge_start.to_string(),
            bridge_range_end: network.bridge_end.to_string(),
            vm_range_start: network.vm_start.to_string(),
            vm_range_end: network.vm_end.to_string(),
            prefix_len: network.prefix_len as i64,
            control_plane_endpoint: identity.control_plane_endpoint,
            apiserver_visibility: identity.apiserver_visibility,
            external_pool: identity.external_pool,
            service_block_cidr: identity.service_block_cidr,
            trust_domain: identity.trust_domain,
            created_at: identity.created_at,
        }
    }

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
}

/// Inclusive IPv4 range expressed as host-order `u32`s.
#[derive(Debug, Clone, Copy)]
pub struct ParsedRange {
    pub start: u32,
    pub end: u32,
}

impl ParsedRange {
    fn parse(start: &str, end: &str, cluster_id: &str, kind: &str) -> Result<Self, DbError> {
        let s: Ipv4Addr = start.parse().map_err(|e| {
            DbError::Malformed(format!(
                "cluster {cluster_id} {kind}_range_start '{start}': {e}"
            ))
        })?;
        let e: Ipv4Addr = end.parse().map_err(|e| {
            DbError::Malformed(format!(
                "cluster {cluster_id} {kind}_range_end '{end}': {e}"
            ))
        })?;
        Ok(Self {
            start: u32::from(s),
            end: u32::from(e),
        })
    }

    /// Derive an inclusive allocatable range from a pool's CIDR:
    /// `[network+1, broadcast-1]` for /N<31. /30 yields the two host
    /// addresses; smaller pools are rejected upstream by `Pool::validate`.
    pub fn parse_pool_range(pool: &Pool) -> Result<Self, DbError> {
        let net: ipnet::Ipv4Net = pool
            .cidr
            .parse()
            .map_err(|e| DbError::Malformed(format!("pool '{}' cidr: {e}", pool.name)))?;
        let net_addr = u32::from(net.network());
        let bcast = u32::from(net.broadcast());
        Ok(Self {
            start: net_addr + 1,
            end: bcast - 1,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{NetworkConfig, Pool, PoolScope, VniRange};

    async fn test_db() -> Db {
        Db::open(":memory:".as_ref(), 1.0).await.unwrap()
    }

    fn make_net_config() -> NetworkConfig {
        NetworkConfig {
            cluster_supernet: "10.0.0.0/8".to_string(),
            cluster_prefix: 24,
            bridge_reserve: 32,
            default_external_service_ips: 16,
            vni_range: VniRange {
                start: 10_000,
                end: 10_010,
            },
            pools: vec![Pool {
                name: "cell-internal".to_string(),
                cidr: "192.168.100.0/27".to_string(),
                scope: PoolScope::Lan,
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
            rootfs_total_bytes: 200 * GIB,
            rootfs_free_bytes: 200 * GIB,
            rootfs_metadata_total_bytes: 0,
            rootfs_metadata_free_bytes: 0,
            data_total_bytes: 800 * GIB,
            data_free_bytes: 800 * GIB,
            gpu_inventory: Vec::new(),
            vtep_address: format!("10.100.0.{}", id.bytes().last().unwrap_or(b'1')),
            last_heartbeat: "2025-01-01T00:00:00Z".to_string(),
            healthy: true,
            rank: 0,
            labels: BTreeMap::new(),
        }
    }

    /// Build a `ClusterRow` from an allocated `ClusterNetwork`.
    /// Default `apiserver_visibility = PUBLIC`; tests override when
    /// they want to exercise the private path.
    fn make_cluster(id: &str, name: &str, network: ClusterNetwork, endpoint: &str) -> ClusterRow {
        ClusterRow::from_network(
            ClusterIdentity {
                id: id.to_string(),
                name: name.to_string(),
                control_plane_endpoint: endpoint.to_string(),
                apiserver_visibility: 0,
                external_pool: "cell-internal".to_string(),
                service_block_cidr: String::new(),
                trust_domain: String::new(),
                created_at: "2025-01-01T00:00:00Z".to_string(),
            },
            network,
        )
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
            extra_disk_gibs: "[]".to_string(),
            image: "test:latest".to_string(),
            error_message: String::new(),
            created_at: "2025-01-01T00:00:00Z".to_string(),
            updated_at: "2025-01-01T00:00:00Z".to_string(),
        }
    }

    #[tokio::test]
    async fn allocate_cluster_network_picks_sequential_vni_and_cidr() {
        let db = test_db().await;
        let net = make_net_config();

        // Need actual cluster rows for the next allocation to see VNIs
        // as taken — `allocate_cluster_network` reads from `clusters`.
        let n1 = db.allocate_cluster_network(&net).await.unwrap();
        db.insert_cluster(&make_cluster("c1", "c1", n1, "unused"))
            .await
            .unwrap();
        let n2 = db.allocate_cluster_network(&net).await.unwrap();
        db.insert_cluster(&make_cluster("c2", "c2", n2, "unused"))
            .await
            .unwrap();

        assert_eq!(n1.vni, 10_000);
        assert_eq!(n2.vni, 10_001);
        assert_ne!(n1.cidr, n2.cidr);
        assert_eq!(n1.prefix_len, 24);
    }

    #[tokio::test]
    async fn cluster_network_carve_layout_is_sane() {
        let db = test_db().await;
        let net = make_net_config();
        let n = db.allocate_cluster_network(&net).await.unwrap();

        // bridge_range = bottom `bridge_reserve` after the network
        // address; vm_range = the rest minus the broadcast; private
        // apiserver = last usable.
        assert_eq!(u32::from(n.bridge_start), u32::from(n.cidr.network()) + 1);
        assert_eq!(
            u32::from(n.bridge_end) - u32::from(n.bridge_start),
            net.bridge_reserve - 1
        );
        assert_eq!(u32::from(n.vm_start), u32::from(n.bridge_end) + 1);
        assert_eq!(u32::from(n.vm_end), u32::from(n.cidr.broadcast()) - 1);
        assert_eq!(
            n.private_apiserver_ip(),
            Ipv4Addr::from(u32::from(n.cidr.broadcast()) - 1)
        );
    }

    #[tokio::test]
    async fn deleting_cluster_frees_its_vni() {
        let db = test_db().await;
        let mut net = make_net_config();
        net.vni_range.end = 10_000; // single VNI

        let n = db.allocate_cluster_network(&net).await.unwrap();
        db.insert_cluster(&make_cluster("c1", "c1", n, "unused"))
            .await
            .unwrap();

        // Pool exhausted — VNI 10_000 already taken.
        let next = db.allocate_cluster_network(&net).await;
        assert!(matches!(next, Err(DbError::Exhausted(_))));

        db.delete_cluster("c1").await.unwrap();
        let n2 = db.allocate_cluster_network(&net).await.unwrap();
        assert_eq!(n2.vni, 10_000);
    }

    #[tokio::test]
    async fn allocate_cluster_vm_ip_starts_above_bridge_reserve() {
        let db = test_db().await;
        let net = make_net_config();
        let n = db.allocate_cluster_network(&net).await.unwrap();
        let cluster = make_cluster("c1", "c1", n, "unused");
        db.insert_cluster(&cluster).await.unwrap();

        // First VM IP = bridge_end + 1.
        let ip = db.allocate_cluster_vm_ip(&cluster, "vm1").await.unwrap();
        assert_eq!(ip, n.vm_start.to_string());
    }

    /// Find the bridge IP `host_id` was given for `cluster_id`, looking
    /// at active host_clusters rows only — the same path
    /// `build_reconcile_command` takes. Returns `None` when there's no
    /// row (or the row is in PENDING_TEARDOWN, which is exposed via a
    /// separate query in production).
    async fn active_bridge_ip(db: &Db, cluster_id: &str, host_id: &str) -> Option<String> {
        db.list_active_host_clusters(host_id)
            .await
            .unwrap()
            .into_iter()
            .find(|m| m.id == cluster_id)
            .map(|m| m.bridge_ip)
    }

    #[tokio::test]
    async fn host_bridge_ip_is_unique_and_idempotent() {
        let db = test_db().await;
        let mut h1 = make_host("h1", "node-1");
        h1.vtep_address = "10.100.0.1".to_string();
        db.upsert_host(&h1).await.unwrap();
        let mut h2 = make_host("h2", "node-2");
        h2.vtep_address = "10.100.0.2".to_string();
        db.upsert_host(&h2).await.unwrap();
        let net = make_net_config();
        let n = db.allocate_cluster_network(&net).await.unwrap();
        let cluster = make_cluster("c1", "c1", n, "unused");
        db.insert_cluster(&cluster).await.unwrap();

        let ip_h1 = db.ensure_host_cluster_active(&cluster, "h1").await.unwrap();
        let ip_h2 = db.ensure_host_cluster_active(&cluster, "h2").await.unwrap();
        assert_ne!(ip_h1, ip_h2);
        assert_eq!(ip_h1, n.bridge_start.to_string());

        // Idempotent for the same host.
        let ip_h1_again = db.ensure_host_cluster_active(&cluster, "h1").await.unwrap();
        assert_eq!(ip_h1, ip_h1_again);

        assert_eq!(active_bridge_ip(&db, "c1", "h1").await, Some(ip_h1.clone()));
        assert_eq!(active_bridge_ip(&db, "c1", "missing").await, None);
    }

    #[tokio::test]
    async fn host_cluster_pending_teardown_skips_when_live_vms_remain() {
        let db = test_db().await;
        let mut host = make_host("h1", "node-1");
        host.vtep_address = "10.100.0.1".to_string();
        db.upsert_host(&host).await.unwrap();

        let net = make_net_config();
        let n = db.allocate_cluster_network(&net).await.unwrap();
        let cluster = make_cluster("c1", "c1", n, "unused");
        db.insert_cluster(&cluster).await.unwrap();

        let ip = db.ensure_host_cluster_active(&cluster, "h1").await.unwrap();
        let vm_ip = db.allocate_cluster_vm_ip(&cluster, "v1").await.unwrap();
        db.insert_vm(&make_vm("v1", "h1", "c1", &vm_ip), &[])
            .await
            .unwrap();

        // Live VM remains → mark_pending_teardown is a no-op (state
        // stays ACTIVE), so list_active still surfaces this row.
        db.mark_host_cluster_pending_teardown("c1", "h1")
            .await
            .unwrap();
        assert_eq!(active_bridge_ip(&db, "c1", "h1").await, Some(ip.clone()));

        // Tombstoning the VM detaches the live-VM gate, so the next
        // mark_pending_teardown flips state to PENDING_TEARDOWN —
        // active query now skips, pending query surfaces it.
        db.mark_vm_pending_teardown("v1").await.unwrap();
        db.mark_host_cluster_pending_teardown("c1", "h1")
            .await
            .unwrap();
        assert_eq!(active_bridge_ip(&db, "c1", "h1").await, None);
        let pending = db.list_pending_cluster_tombstones("h1").await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].cluster_id, "c1");

        // Ack drops both the vm row and the host_clusters row, releasing
        // the IP for reuse.
        db.ack_tombstones("h1", &[n.vni], &["v1".to_string()])
            .await
            .unwrap();
        // The cluster row is also gone after the last host released —
        // ack_tombstones cascades.
        assert!(db.get_cluster("c1").await.is_err());
    }

    #[tokio::test]
    async fn ensure_host_cluster_active_resurrects_pending_teardown() {
        let db = test_db().await;
        let mut host = make_host("h1", "node-1");
        host.vtep_address = "10.100.0.1".to_string();
        db.upsert_host(&host).await.unwrap();

        let net = make_net_config();
        let n = db.allocate_cluster_network(&net).await.unwrap();
        let cluster = make_cluster("c1", "c1", n, "unused");
        db.insert_cluster(&cluster).await.unwrap();

        // First VM lands → row ACTIVE.
        let original_ip = db.ensure_host_cluster_active(&cluster, "h1").await.unwrap();
        let vm1_ip = db.allocate_cluster_vm_ip(&cluster, "v1").await.unwrap();
        db.insert_vm(&make_vm("v1", "h1", "c1", &vm1_ip), &[])
            .await
            .unwrap();

        // VM goes pending → host_cluster also goes pending.
        db.mark_vm_pending_teardown("v1").await.unwrap();
        db.mark_host_cluster_pending_teardown("c1", "h1")
            .await
            .unwrap();
        assert!(db
            .list_pending_cluster_tombstones("h1")
            .await
            .unwrap()
            .iter()
            .any(|t| t.cluster_id == "c1"));

        // CAPI delete-then-recreate races: a new VM lands on the same
        // (host, cluster) before the agent acks the tombstone. The
        // resurrection-cancels-tombstone path flips the row back to
        // ACTIVE without ever emitting a tombstone.
        let resurrected_ip = db.ensure_host_cluster_active(&cluster, "h1").await.unwrap();
        assert_eq!(
            resurrected_ip, original_ip,
            "resurrection keeps the same bridge IP — no flap",
        );
        assert!(db
            .list_pending_cluster_tombstones("h1")
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            active_bridge_ip(&db, "c1", "h1").await,
            Some(original_ip),
            "row is back in ACTIVE",
        );
    }

    #[tokio::test]
    async fn vm_ip_release_frees_for_reuse() {
        let db = test_db().await;
        let net = make_net_config();
        let n = db.allocate_cluster_network(&net).await.unwrap();
        let cluster = make_cluster("c1", "c1", n, "unused");
        db.insert_cluster(&cluster).await.unwrap();

        let ip1 = db.allocate_cluster_vm_ip(&cluster, "vm1").await.unwrap();
        db.release_vm_ips("vm1").await.unwrap();
        let ip2 = db.allocate_cluster_vm_ip(&cluster, "vm2").await.unwrap();
        assert_eq!(ip1, ip2, "released VM IP must be reusable");
    }

    #[tokio::test]
    async fn pool_vip_released_by_explicit_call() {
        let db = test_db().await;
        let net = make_net_config();
        let n = db.allocate_cluster_network(&net).await.unwrap();
        db.insert_cluster(&make_cluster("c1", "c1", n, "unused"))
            .await
            .unwrap();

        let vip = db
            .allocate_pool_vip(pool(&net, "cell-internal"), "c1")
            .await
            .unwrap();
        assert!(vip.starts_with("192.168.100."));

        db.release_cluster_ips("c1").await.unwrap();
        db.delete_cluster("c1").await.unwrap();

        let n2 = db.allocate_cluster_network(&net).await.unwrap();
        db.insert_cluster(&make_cluster("c2", "c2", n2, "unused"))
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
    async fn list_cluster_vteps_tracks_active_host_clusters() {
        let db = test_db().await;
        let mut h1 = make_host("h1", "node-1");
        h1.vtep_address = "10.100.0.1".to_string();
        let mut h2 = make_host("h2", "node-2");
        h2.vtep_address = "10.100.0.2".to_string();
        db.upsert_host(&h1).await.unwrap();
        db.upsert_host(&h2).await.unwrap();

        let net = make_net_config();
        let n = db.allocate_cluster_network(&net).await.unwrap();
        let cluster = make_cluster("c1", "c1", n, "unused-endpoint");
        db.insert_cluster(&cluster).await.unwrap();

        assert!(db.list_cluster_vteps("c1").await.unwrap().is_empty());

        // Each host transitions to ACTIVE for the cluster on first
        // VM placement — that's when it joins the BUM destination set.
        let vm_ip_1 = db.allocate_cluster_vm_ip(&cluster, "v1").await.unwrap();
        db.insert_vm(&make_vm("v1", "h1", "c1", &vm_ip_1), &[])
            .await
            .unwrap();
        db.ensure_host_cluster_active(&cluster, "h1").await.unwrap();
        assert_eq!(
            db.list_cluster_vteps("c1").await.unwrap(),
            vec!["10.100.0.1".to_string()]
        );

        let vm_ip_2 = db.allocate_cluster_vm_ip(&cluster, "v2").await.unwrap();
        db.insert_vm(&make_vm("v2", "h2", "c1", &vm_ip_2), &[])
            .await
            .unwrap();
        db.ensure_host_cluster_active(&cluster, "h2").await.unwrap();
        let mut vteps = db.list_cluster_vteps("c1").await.unwrap();
        vteps.sort();
        assert_eq!(
            vteps,
            vec!["10.100.0.1".to_string(), "10.100.0.2".to_string()]
        );

        // Mark VM pending then mark host_cluster pending — same path
        // initiate_vm_teardown takes. Pending hosts drop out of the
        // BUM set so peer FDBs stop flooding them.
        db.mark_vm_pending_teardown("v1").await.unwrap();
        db.mark_host_cluster_pending_teardown("c1", "h1")
            .await
            .unwrap();
        assert_eq!(
            db.list_cluster_vteps("c1").await.unwrap(),
            vec!["10.100.0.2".to_string()]
        );
    }

    /// Seed a `(host, cluster)` pair the VM-race tests can aim
    /// placements at. Extracted so the three optimistic-concurrency
    /// tests below don't repeat the same 10 lines.
    async fn seed_single_host_cluster(db: &Db) -> (HostRow, ClusterRow) {
        let mut host = make_host("h1", "node-1");
        host.vtep_address = "10.100.0.1".to_string();
        db.upsert_host(&host).await.unwrap();
        let net = make_net_config();
        let n = db.allocate_cluster_network(&net).await.unwrap();
        let cluster = make_cluster("c1", "c1", n, "unused-endpoint");
        db.insert_cluster(&cluster).await.unwrap();
        (host, cluster)
    }

    /// The atomic capacity gate in `insert_vm` refuses the second
    /// placement if a concurrent winner has already consumed the
    /// cpu slice we targeted — the writer's single-connection
    /// serialization makes this a faithful stand-in for the real
    /// race in `create_machine`.
    #[tokio::test]
    async fn insert_vm_rejects_cpu_capacity_race() {
        let db = test_db().await;
        let (_, _) = seed_single_host_cluster(&db).await;

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
        // Ack-driven teardown drops the vm row + cascades vm_gpus
        // via FK ON DELETE CASCADE.
        db.mark_vm_pending_teardown("v1").await.unwrap();
        db.ack_tombstones("h1", &[], &["v1".to_string()])
            .await
            .unwrap();
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
        v1.extra_disk_gibs = "[30, 20]".to_string();
        db.insert_vm(&v1, &[]).await.unwrap();

        let snapshot = db.host_usage_snapshot().await.unwrap();
        let u = snapshot.get("h1").expect("host in snapshot");
        assert_eq!(u.used_cpu, 4);
        assert_eq!(u.used_memory_mib, 8192);
        assert_eq!(u.used_rootfs_gib, 100, "rootfs usage = vms.disk_gib only");
        assert_eq!(
            u.used_data_gib, 50,
            "data usage = sum of extras (json_each over extra_disk_gibs)"
        );
        assert!(u.assigned_pci.is_empty());
        assert_eq!(
            u.vms_by_cluster.get("c1").copied(),
            Some(1),
            "per-cluster VM count populated from vms.cluster_id"
        );
    }

    /// Bring up a fresh DB with two healthy hosts and one cluster ready
    /// for `ensure_host_cluster_active`. Common preamble for the LAN-VIP
    /// election tests below.
    async fn lan_vip_owner_setup() -> (Db, ClusterRow) {
        let db = test_db().await;
        let net = make_net_config();
        let mut h1 = make_host("h1", "node-1");
        h1.vtep_address = "10.100.0.1".to_string();
        db.upsert_host(&h1).await.unwrap();
        let mut h2 = make_host("h2", "node-2");
        h2.vtep_address = "10.100.0.2".to_string();
        db.upsert_host(&h2).await.unwrap();
        let n = db.allocate_cluster_network(&net).await.unwrap();
        let cluster = make_cluster("c1", "c1", n, "10.100.0.1");
        db.insert_cluster(&cluster).await.unwrap();
        (db, cluster)
    }

    /// First election picks the lowest-id carrier and persists. The
    /// second carrier joining does NOT displace — sticky behaviour
    /// is the whole point of the election rule.
    #[tokio::test]
    async fn elect_lan_vip_owner_is_sticky_on_member_add() {
        let (db, cluster) = lan_vip_owner_setup().await;

        db.ensure_host_cluster_active(&cluster, "h1").await.unwrap();
        let carriers = db.list_active_carriers(&cluster.id).await.unwrap();
        let owner1 = db
            .elect_lan_vip_owner(&cluster.id, &carriers)
            .await
            .unwrap();
        assert_eq!(owner1, "h1");

        // Second carrier joins. Ownership must NOT flip — h1 is still
        // active and the recorded owner.
        db.ensure_host_cluster_active(&cluster, "h2").await.unwrap();
        let carriers = db.list_active_carriers(&cluster.id).await.unwrap();
        let owner2 = db
            .elect_lan_vip_owner(&cluster.id, &carriers)
            .await
            .unwrap();
        assert_eq!(owner2, "h1", "sticky: member-add must not displace owner");
    }

    /// When the recorded owner leaves the carrier set, election picks a
    /// surviving carrier and persists. This is the only path that
    /// changes ownership.
    #[tokio::test]
    async fn elect_lan_vip_owner_re_elects_when_owner_leaves() {
        let (db, cluster) = lan_vip_owner_setup().await;

        db.ensure_host_cluster_active(&cluster, "h1").await.unwrap();
        db.ensure_host_cluster_active(&cluster, "h2").await.unwrap();
        let carriers = db.list_active_carriers(&cluster.id).await.unwrap();
        assert_eq!(
            db.elect_lan_vip_owner(&cluster.id, &carriers)
                .await
                .unwrap(),
            "h1"
        );

        // h1 transitions to PENDING_TEARDOWN — drops out of the active
        // carrier set. Re-election must pick the survivor.
        db.mark_host_cluster_pending_teardown(&cluster.id, "h1")
            .await
            .unwrap();
        let carriers = db.list_active_carriers(&cluster.id).await.unwrap();
        assert_eq!(
            db.elect_lan_vip_owner(&cluster.id, &carriers)
                .await
                .unwrap(),
            "h2",
            "owner left → re-elect to surviving carrier"
        );
    }

    /// `list_active_carriers` joins on `hosts.healthy = 1`. A carrier
    /// whose host went unhealthy must NOT win or retain ownership —
    /// otherwise nobody answers ARP for the cluster's LAN VIPs and
    /// LAN ingress black-holes until membership rows clear.
    #[tokio::test]
    async fn elect_lan_vip_owner_skips_unhealthy_carrier() {
        let (db, cluster) = lan_vip_owner_setup().await;

        db.ensure_host_cluster_active(&cluster, "h1").await.unwrap();
        db.ensure_host_cluster_active(&cluster, "h2").await.unwrap();

        // Owner h1 is elected first.
        let carriers = db.list_active_carriers(&cluster.id).await.unwrap();
        assert_eq!(
            db.elect_lan_vip_owner(&cluster.id, &carriers)
                .await
                .unwrap(),
            "h1"
        );

        // h1 goes unhealthy (heartbeat watchdog flips the bit). The
        // join in `list_active_carriers` excludes h1; election re-runs
        // because the recorded owner is no longer in the carrier set.
        sqlx::query("UPDATE hosts SET healthy = 0 WHERE id = ?")
            .bind("h1")
            .execute(&db.writer)
            .await
            .unwrap();
        let carriers = db.list_active_carriers(&cluster.id).await.unwrap();
        assert_eq!(
            carriers,
            vec!["h2".to_string()],
            "unhealthy carrier excluded from list"
        );
        assert_eq!(
            db.elect_lan_vip_owner(&cluster.id, &carriers)
                .await
                .unwrap(),
            "h2",
            "election re-runs to surviving healthy carrier"
        );
    }

    /// No carriers at all → empty owner string. Agents skip proxy-ARP
    /// for every entry; routes still install. This is the cold-start
    /// shape between cluster create and first placement.
    #[tokio::test]
    async fn elect_lan_vip_owner_no_carriers_returns_empty() {
        let (db, cluster) = lan_vip_owner_setup().await;
        let carriers = db.list_active_carriers(&cluster.id).await.unwrap();
        assert!(carriers.is_empty());
        let owner = db
            .elect_lan_vip_owner(&cluster.id, &carriers)
            .await
            .unwrap();
        assert_eq!(owner, "");
    }

    /// `update_cluster_trust_domain` overwrites the stored value so the
    /// next agent reconcile pass picks up the new VRF assignment.
    #[tokio::test]
    async fn update_cluster_trust_domain_overwrites_stored_value() {
        let db = test_db().await;
        let net = db
            .allocate_cluster_network(&make_net_config())
            .await
            .unwrap();
        let mut cluster = make_cluster("c1", "c1", net, "10.100.0.1");
        cluster.trust_domain = "old-td".to_string();
        db.insert_cluster(&cluster).await.unwrap();

        db.update_cluster_trust_domain(&cluster.id, "new-td")
            .await
            .unwrap();

        let refreshed = db.get_cluster(&cluster.id).await.unwrap();
        assert_eq!(refreshed.trust_domain, "new-td");
    }
}
