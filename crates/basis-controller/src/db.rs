use std::path::Path;

use std::str::FromStr;

use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};

use crate::config::IpPool;

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
}

/// Every IP allocation is owned by exactly one thing. Two kinds today —
/// a VM's address, or a cluster's control-plane VIP. Adding a new owner
/// kind in the future means adding a variant here.
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
    pool: SqlitePool,
}

impl Db {
    pub async fn open(path: &Path) -> Result<Self, DbError> {
        let options = if path.to_string_lossy() == ":memory:" {
            SqliteConnectOptions::from_str("sqlite::memory:")?
        } else {
            SqliteConnectOptions::new()
                .filename(path)
                .create_if_missing(true)
                .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
                .busy_timeout(std::time::Duration::from_secs(5))
        };

        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await?;

        let db = Self { pool };
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
                last_heartbeat TEXT NOT NULL,
                healthy INTEGER NOT NULL DEFAULT 1
            )",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS clusters (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                ip_pool TEXT NOT NULL,
                control_plane_endpoint TEXT NOT NULL,
                created_at TEXT NOT NULL
            )",
        )
        .execute(&self.pool)
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
                gpu_assignments TEXT NOT NULL DEFAULT '[]',
                image TEXT NOT NULL,
                error_message TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )",
        )
        .execute(&self.pool)
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
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS ip_pools (
                name TEXT PRIMARY KEY,
                cidr TEXT NOT NULL,
                gateway TEXT NOT NULL,
                range_start TEXT NOT NULL,
                range_end TEXT NOT NULL
            )",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS ip_allocations (
                ip_address TEXT PRIMARY KEY,
                pool_name TEXT NOT NULL,
                owner_id TEXT NOT NULL,
                owner_kind TEXT NOT NULL
            )",
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    // --- IP Pools ---

    /// Upsert every configured IP pool. Called once on controller startup so
    /// the DB matches controller.toml after any config changes.
    pub async fn seed_ip_pools(&self, pools: &[IpPool]) -> Result<(), DbError> {
        for pool in pools {
            self.upsert_ip_pool(pool).await?;
        }
        Ok(())
    }

    pub async fn upsert_ip_pool(&self, pool: &IpPool) -> Result<(), DbError> {
        sqlx::query(
            "INSERT INTO ip_pools (name, cidr, gateway, range_start, range_end)
             VALUES (?, ?, ?, ?, ?)
             ON CONFLICT(name) DO UPDATE SET
                cidr = excluded.cidr,
                gateway = excluded.gateway,
                range_start = excluded.range_start,
                range_end = excluded.range_end",
        )
        .bind(&pool.name)
        .bind(&pool.cidr)
        .bind(&pool.gateway)
        .bind(&pool.range_start)
        .bind(&pool.range_end)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_ip_pool(&self, name: &str) -> Result<IpPoolRow, DbError> {
        sqlx::query_as::<_, IpPoolRow>("SELECT * FROM ip_pools WHERE name = ?")
            .bind(name)
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| DbError::NotFound(format!("ip pool '{name}'")))
    }

    /// Allocate the next available IP from `pool_name`, recording the
    /// supplied owner as its owner.
    pub async fn allocate_ip(
        &self,
        pool_name: &str,
        owner: IpOwner<'_>,
    ) -> Result<String, DbError> {
        let pool = self.get_ip_pool(pool_name).await?;

        let start: std::net::Ipv4Addr = pool
            .range_start
            .parse()
            .map_err(|e| DbError::Conflict(format!("bad range_start: {e}")))?;
        let end: std::net::Ipv4Addr = pool
            .range_end
            .parse()
            .map_err(|e| DbError::Conflict(format!("bad range_end: {e}")))?;

        let start_u32 = u32::from(start);
        let end_u32 = u32::from(end);

        let allocated: Vec<String> =
            sqlx::query_scalar("SELECT ip_address FROM ip_allocations WHERE pool_name = ?")
                .bind(pool_name)
                .fetch_all(&self.pool)
                .await?;

        let allocated_set: std::collections::HashSet<String> = allocated.into_iter().collect();

        for ip_u32 in start_u32..=end_u32 {
            let candidate = std::net::Ipv4Addr::from(ip_u32).to_string();
            if !allocated_set.contains(&candidate) {
                sqlx::query(
                    "INSERT INTO ip_allocations (ip_address, pool_name, owner_id, owner_kind)
                     VALUES (?, ?, ?, ?)",
                )
                .bind(&candidate)
                .bind(pool_name)
                .bind(owner.id())
                .bind(owner.kind())
                .execute(&self.pool)
                .await?;
                return Ok(candidate);
            }
        }

        Err(DbError::Conflict(format!(
            "no available IPs in pool '{pool_name}'"
        )))
    }

    /// Release every IP held by this owner.
    pub async fn release_ips(&self, owner: IpOwner<'_>) -> Result<(), DbError> {
        sqlx::query("DELETE FROM ip_allocations WHERE owner_id = ? AND owner_kind = ?")
            .bind(owner.id())
            .bind(owner.kind())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    // --- Clusters ---

    pub async fn insert_cluster(&self, cluster: &ClusterRow) -> Result<(), DbError> {
        sqlx::query(
            "INSERT INTO clusters (id, name, ip_pool, control_plane_endpoint, created_at)
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(&cluster.id)
        .bind(&cluster.name)
        .bind(&cluster.ip_pool)
        .bind(&cluster.control_plane_endpoint)
        .bind(&cluster.created_at)
        .execute(&self.pool)
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
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| DbError::NotFound(format!("cluster '{id}'")))
    }

    pub async fn get_cluster_by_name(&self, name: &str) -> Result<Option<ClusterRow>, DbError> {
        Ok(
            sqlx::query_as::<_, ClusterRow>("SELECT * FROM clusters WHERE name = ?")
                .bind(name)
                .fetch_optional(&self.pool)
                .await?,
        )
    }

    pub async fn delete_cluster(&self, id: &str) -> Result<(), DbError> {
        sqlx::query("DELETE FROM clusters WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn list_clusters(&self) -> Result<Vec<ClusterRow>, DbError> {
        Ok(sqlx::query_as::<_, ClusterRow>("SELECT * FROM clusters")
            .fetch_all(&self.pool)
            .await?)
    }

    // --- Hosts ---

    pub async fn upsert_host(&self, host: &HostRow) -> Result<(), DbError> {
        sqlx::query(
            "INSERT INTO hosts (id, hostname, total_cpu, total_memory_mib, total_disk_gib,
                gpu_inventory, last_heartbeat, healthy)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(id) DO UPDATE SET
                hostname = excluded.hostname,
                total_cpu = excluded.total_cpu,
                total_memory_mib = excluded.total_memory_mib,
                total_disk_gib = excluded.total_disk_gib,
                gpu_inventory = excluded.gpu_inventory,
                last_heartbeat = excluded.last_heartbeat,
                healthy = excluded.healthy",
        )
        .bind(&host.id)
        .bind(&host.hostname)
        .bind(host.total_cpu)
        .bind(host.total_memory_mib)
        .bind(host.total_disk_gib)
        .bind(&host.gpu_inventory)
        .bind(&host.last_heartbeat)
        .bind(host.healthy)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_host(&self, id: &str) -> Result<HostRow, DbError> {
        sqlx::query_as::<_, HostRow>("SELECT * FROM hosts WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| DbError::NotFound(format!("host '{id}'")))
    }

    pub async fn get_host_by_hostname(&self, hostname: &str) -> Result<Option<HostRow>, DbError> {
        Ok(
            sqlx::query_as::<_, HostRow>("SELECT * FROM hosts WHERE hostname = ?")
                .bind(hostname)
                .fetch_optional(&self.pool)
                .await?,
        )
    }

    pub async fn list_healthy_hosts(&self) -> Result<Vec<HostRow>, DbError> {
        Ok(
            sqlx::query_as::<_, HostRow>("SELECT * FROM hosts WHERE healthy = 1")
                .fetch_all(&self.pool)
                .await?,
        )
    }

    pub async fn list_hosts(&self) -> Result<Vec<HostRow>, DbError> {
        Ok(
            sqlx::query_as::<_, HostRow>("SELECT * FROM hosts")
                .fetch_all(&self.pool)
                .await?,
        )
    }

    /// Refresh `last_heartbeat` and flip the host back to healthy. Capacity
    /// isn't stored per-host — scheduler computes it from VM allocations.
    pub async fn update_host_heartbeat(&self, host_id: &str, now: &str) -> Result<(), DbError> {
        let result = sqlx::query(
            "UPDATE hosts SET last_heartbeat = ?, healthy = 1 WHERE id = ?",
        )
        .bind(now)
        .bind(host_id)
        .execute(&self.pool)
        .await?;

        if result.rows_affected() == 0 {
            return Err(DbError::NotFound(format!("host '{host_id}'")));
        }
        Ok(())
    }

    pub async fn mark_host_unhealthy(&self, host_id: &str) -> Result<(), DbError> {
        sqlx::query("UPDATE hosts SET healthy = 0 WHERE id = ?")
            .bind(host_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    // --- VMs ---

    pub async fn insert_vm(&self, vm: &VmRow) -> Result<(), DbError> {
        sqlx::query(
            "INSERT INTO vms (id, name, cluster_id, host_id, ip_address, state, cpu, memory_mib, disk_gib,
                gpu_assignments, image, error_message, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
        .bind(&vm.gpu_assignments)
        .bind(&vm.image)
        .bind(&vm.error_message)
        .bind(&vm.created_at)
        .bind(&vm.updated_at)
        .execute(&self.pool)
        .await
        .map_err(|e| match e {
            sqlx::Error::Database(db_err) if db_err.is_unique_violation() => DbError::Conflict(
                format!("vm '{}' already exists in cluster '{}'", vm.name, vm.cluster_id),
            ),
            other => DbError::Sqlx(other),
        })?;
        Ok(())
    }

    pub async fn get_vm(&self, id: &str) -> Result<VmRow, DbError> {
        sqlx::query_as::<_, VmRow>("SELECT * FROM vms WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| DbError::NotFound(format!("vm '{id}'")))
    }

    pub async fn get_vm_by_name(
        &self,
        cluster_id: &str,
        name: &str,
    ) -> Result<Option<VmRow>, DbError> {
        Ok(sqlx::query_as::<_, VmRow>(
            "SELECT * FROM vms WHERE cluster_id = ? AND name = ?",
        )
        .bind(cluster_id)
        .bind(name)
        .fetch_optional(&self.pool)
        .await?)
    }

    pub async fn list_vms(&self, cluster_id: Option<&str>) -> Result<Vec<VmRow>, DbError> {
        match cluster_id {
            Some(c) => Ok(
                sqlx::query_as::<_, VmRow>("SELECT * FROM vms WHERE cluster_id = ?")
                    .bind(c)
                    .fetch_all(&self.pool)
                    .await?,
            ),
            None => Ok(sqlx::query_as::<_, VmRow>("SELECT * FROM vms")
                .fetch_all(&self.pool)
                .await?),
        }
    }

    pub async fn list_vms_on_host(&self, host_id: &str) -> Result<Vec<VmRow>, DbError> {
        Ok(
            sqlx::query_as::<_, VmRow>("SELECT * FROM vms WHERE host_id = ?")
                .bind(host_id)
                .fetch_all(&self.pool)
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
        let result = sqlx::query(
            "UPDATE vms SET state = ?, error_message = ?, updated_at = ? WHERE id = ?",
        )
        .bind(state)
        .bind(error_message)
        .bind(now)
        .bind(vm_id)
        .execute(&self.pool)
        .await?;

        if result.rows_affected() == 0 {
            return Err(DbError::NotFound(format!("vm '{vm_id}'")));
        }
        Ok(())
    }

    pub async fn delete_vm(&self, id: &str) -> Result<(), DbError> {
        sqlx::query("DELETE FROM vms WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Mark hosts as unhealthy if their last heartbeat is older than the cutoff.
    pub async fn mark_stale_hosts_unhealthy(&self, cutoff: &str) -> Result<Vec<String>, DbError> {
        let stale: Vec<String> = sqlx::query_scalar(
            "SELECT id FROM hosts WHERE healthy = 1 AND last_heartbeat < ?",
        )
        .bind(cutoff)
        .fetch_all(&self.pool)
        .await?;

        if !stale.is_empty() {
            sqlx::query("UPDATE hosts SET healthy = 0 WHERE healthy = 1 AND last_heartbeat < ?")
                .bind(cutoff)
                .execute(&self.pool)
                .await?;
        }

        Ok(stale)
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
    pub gpu_assignments: String,
    pub image: String,
    pub error_message: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ClusterRow {
    pub id: String,
    pub name: String,
    pub ip_pool: String,
    pub control_plane_endpoint: String,
    pub created_at: String,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct IpPoolRow {
    pub name: String,
    pub cidr: String,
    pub gateway: String,
    pub range_start: String,
    pub range_end: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::IpPool;

    async fn test_db() -> Db {
        Db::open(":memory:".as_ref()).await.unwrap()
    }

    fn make_host(id: &str, hostname: &str) -> HostRow {
        HostRow {
            id: id.to_string(),
            hostname: hostname.to_string(),
            total_cpu: 16,
            total_memory_mib: 65536,
            total_disk_gib: 1000,
            gpu_inventory: "[]".to_string(),
            last_heartbeat: "2025-01-01T00:00:00Z".to_string(),
            healthy: true,
        }
    }

    fn make_cluster(id: &str, name: &str) -> ClusterRow {
        ClusterRow {
            id: id.to_string(),
            name: name.to_string(),
            ip_pool: "default".to_string(),
            control_plane_endpoint: "10.0.10.10".to_string(),
            created_at: "2025-01-01T00:00:00Z".to_string(),
        }
    }

    fn make_vm(id: &str, host_id: &str, cluster_id: &str) -> VmRow {
        VmRow {
            id: id.to_string(),
            name: format!("vm-{id}"),
            cluster_id: cluster_id.to_string(),
            host_id: host_id.to_string(),
            ip_address: "10.0.10.42".to_string(),
            state: 2, // RUNNING
            cpu: 4,
            memory_mib: 8192,
            disk_gib: 100,
            gpu_assignments: "[]".to_string(),
            image: "test-image:latest".to_string(),
            error_message: String::new(),
            created_at: "2025-01-01T00:00:00Z".to_string(),
            updated_at: "2025-01-01T00:00:00Z".to_string(),
        }
    }

    fn make_pool(name: &str) -> IpPool {
        IpPool {
            name: name.to_string(),
            cidr: "10.0.10.0/24".to_string(),
            gateway: "10.0.10.1".to_string(),
            range_start: "10.0.10.10".to_string(),
            range_end: "10.0.10.15".to_string(),
        }
    }

    // --- Host CRUD tests ---

    #[tokio::test]
    async fn test_host_insert_and_get() {
        let db = test_db().await;
        let host = make_host("h1", "node-1");
        db.upsert_host(&host).await.unwrap();

        let fetched = db.get_host("h1").await.unwrap();
        assert_eq!(fetched.hostname, "node-1");
        assert_eq!(fetched.total_cpu, 16);
        assert!(fetched.healthy);
    }

    #[tokio::test]
    async fn test_host_get_not_found() {
        let db = test_db().await;
        let result = db.get_host("nonexistent").await;
        assert!(matches!(result, Err(DbError::NotFound(_))));
    }

    #[tokio::test]
    async fn test_host_upsert_updates_existing() {
        let db = test_db().await;
        let mut host = make_host("h1", "node-1");
        db.upsert_host(&host).await.unwrap();

        host.total_cpu = 32;
        host.total_memory_mib = 131072;
        db.upsert_host(&host).await.unwrap();

        let fetched = db.get_host("h1").await.unwrap();
        assert_eq!(fetched.total_cpu, 32);
        assert_eq!(fetched.total_memory_mib, 131072);
    }

    #[tokio::test]
    async fn test_host_get_by_hostname() {
        let db = test_db().await;
        db.upsert_host(&make_host("h1", "node-1")).await.unwrap();
        db.upsert_host(&make_host("h2", "node-2")).await.unwrap();

        let found = db.get_host_by_hostname("node-2").await.unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().id, "h2");

        let not_found = db.get_host_by_hostname("node-99").await.unwrap();
        assert!(not_found.is_none());
    }

    #[tokio::test]
    async fn test_list_healthy_hosts() {
        let db = test_db().await;
        db.upsert_host(&make_host("h1", "node-1")).await.unwrap();

        let mut unhealthy = make_host("h2", "node-2");
        unhealthy.healthy = false;
        db.upsert_host(&unhealthy).await.unwrap();

        let healthy = db.list_healthy_hosts().await.unwrap();
        assert_eq!(healthy.len(), 1);
        assert_eq!(healthy[0].id, "h1");
    }

    #[tokio::test]
    async fn test_heartbeat_refreshes_timestamp_and_health() {
        let db = test_db().await;
        let mut h = make_host("h1", "node-1");
        h.healthy = false;
        db.upsert_host(&h).await.unwrap();

        db.update_host_heartbeat("h1", "2025-01-01T01:00:00Z").await.unwrap();

        let host = db.get_host("h1").await.unwrap();
        assert_eq!(host.last_heartbeat, "2025-01-01T01:00:00Z");
        assert!(host.healthy, "heartbeat must flip unhealthy → healthy");
    }

    #[tokio::test]
    async fn test_heartbeat_unknown_host_fails() {
        let db = test_db().await;
        let result = db
            .update_host_heartbeat("nonexistent", "2025-01-01T00:00:00Z")
            .await;
        assert!(matches!(result, Err(DbError::NotFound(_))));
    }

    #[tokio::test]
    async fn test_mark_stale_hosts_unhealthy() {
        let db = test_db().await;
        let mut old_host = make_host("h1", "node-1");
        old_host.last_heartbeat = "2025-01-01T00:00:00Z".to_string();
        db.upsert_host(&old_host).await.unwrap();

        let mut fresh_host = make_host("h2", "node-2");
        fresh_host.last_heartbeat = "2025-01-01T02:00:00Z".to_string();
        db.upsert_host(&fresh_host).await.unwrap();

        let stale = db
            .mark_stale_hosts_unhealthy("2025-01-01T01:00:00Z")
            .await
            .unwrap();
        assert_eq!(stale, vec!["h1"]);

        let h1 = db.get_host("h1").await.unwrap();
        assert!(!h1.healthy);

        let h2 = db.get_host("h2").await.unwrap();
        assert!(h2.healthy);
    }

    // --- Cluster tests ---

    #[tokio::test]
    async fn test_cluster_insert_and_get() {
        let db = test_db().await;
        db.insert_cluster(&make_cluster("c1", "cluster-a")).await.unwrap();
        let c = db.get_cluster("c1").await.unwrap();
        assert_eq!(c.name, "cluster-a");
        assert_eq!(c.ip_pool, "default");
    }

    #[tokio::test]
    async fn test_cluster_duplicate_name_is_conflict() {
        let db = test_db().await;
        db.insert_cluster(&make_cluster("c1", "cluster-a")).await.unwrap();
        let err = db
            .insert_cluster(&make_cluster("c2", "cluster-a"))
            .await
            .unwrap_err();
        assert!(matches!(err, DbError::Conflict(_)));
    }

    #[tokio::test]
    async fn test_cluster_delete() {
        let db = test_db().await;
        db.insert_cluster(&make_cluster("c1", "cluster-a")).await.unwrap();
        db.delete_cluster("c1").await.unwrap();
        assert!(matches!(
            db.get_cluster("c1").await,
            Err(DbError::NotFound(_))
        ));
    }

    // --- VM CRUD tests ---

    #[tokio::test]
    async fn test_vm_insert_and_get() {
        let db = test_db().await;
        db.upsert_host(&make_host("h1", "node-1")).await.unwrap();
        db.insert_cluster(&make_cluster("c1", "cluster-a")).await.unwrap();

        let vm = make_vm("vm1", "h1", "c1");
        db.insert_vm(&vm).await.unwrap();

        let fetched = db.get_vm("vm1").await.unwrap();
        assert_eq!(fetched.name, "vm-vm1");
        assert_eq!(fetched.cluster_id, "c1");
        assert_eq!(fetched.host_id, "h1");
        assert_eq!(fetched.cpu, 4);
    }

    #[tokio::test]
    async fn test_vm_get_not_found() {
        let db = test_db().await;
        let result = db.get_vm("nonexistent").await;
        assert!(matches!(result, Err(DbError::NotFound(_))));
    }

    #[tokio::test]
    async fn test_list_vms_by_cluster() {
        let db = test_db().await;
        db.upsert_host(&make_host("h1", "node-1")).await.unwrap();
        db.insert_cluster(&make_cluster("ca", "cluster-a")).await.unwrap();
        db.insert_cluster(&make_cluster("cb", "cluster-b")).await.unwrap();

        db.insert_vm(&make_vm("vm1", "h1", "ca")).await.unwrap();
        db.insert_vm(&make_vm("vm2", "h1", "ca")).await.unwrap();
        db.insert_vm(&make_vm("vm3", "h1", "cb")).await.unwrap();

        let all = db.list_vms(None).await.unwrap();
        assert_eq!(all.len(), 3);

        let cluster_a = db.list_vms(Some("ca")).await.unwrap();
        assert_eq!(cluster_a.len(), 2);

        let cluster_b = db.list_vms(Some("cb")).await.unwrap();
        assert_eq!(cluster_b.len(), 1);

        let cluster_c = db.list_vms(Some("cc")).await.unwrap();
        assert!(cluster_c.is_empty());
    }

    #[tokio::test]
    async fn test_list_vms_on_host() {
        let db = test_db().await;
        db.upsert_host(&make_host("h1", "node-1")).await.unwrap();
        db.upsert_host(&make_host("h2", "node-2")).await.unwrap();
        db.insert_cluster(&make_cluster("c1", "cluster-a")).await.unwrap();

        db.insert_vm(&make_vm("vm1", "h1", "c1")).await.unwrap();
        db.insert_vm(&make_vm("vm2", "h1", "c1")).await.unwrap();
        db.insert_vm(&make_vm("vm3", "h2", "c1")).await.unwrap();

        let on_h1 = db.list_vms_on_host("h1").await.unwrap();
        assert_eq!(on_h1.len(), 2);

        let on_h2 = db.list_vms_on_host("h2").await.unwrap();
        assert_eq!(on_h2.len(), 1);
    }

    #[tokio::test]
    async fn test_update_vm_state() {
        let db = test_db().await;
        db.upsert_host(&make_host("h1", "node-1")).await.unwrap();
        db.insert_cluster(&make_cluster("c1", "cluster-a")).await.unwrap();
        db.insert_vm(&make_vm("vm1", "h1", "c1")).await.unwrap();

        db.update_vm_state(
            "vm1",
            basis_proto::MachineState::Failed as i64,
            "disk error",
            "2025-01-02T00:00:00Z",
        )
        .await
        .unwrap();

        let vm = db.get_vm("vm1").await.unwrap();
        assert_eq!(vm.state, basis_proto::MachineState::Failed as i64);
        assert_eq!(vm.error_message, "disk error");
        assert_eq!(vm.updated_at, "2025-01-02T00:00:00Z");
    }

    #[tokio::test]
    async fn test_update_vm_state_not_found() {
        let db = test_db().await;
        let result = db
            .update_vm_state(
                "nonexistent",
                basis_proto::MachineState::Running as i64,
                "",
                "2025-01-01T00:00:00Z",
            )
            .await;
        assert!(matches!(result, Err(DbError::NotFound(_))));
    }

    #[tokio::test]
    async fn test_delete_vm() {
        let db = test_db().await;
        db.upsert_host(&make_host("h1", "node-1")).await.unwrap();
        db.insert_cluster(&make_cluster("c1", "cluster-a")).await.unwrap();
        db.insert_vm(&make_vm("vm1", "h1", "c1")).await.unwrap();

        db.delete_vm("vm1").await.unwrap();
        let result = db.get_vm("vm1").await;
        assert!(matches!(result, Err(DbError::NotFound(_))));
    }

    // --- IP allocation tests ---

    #[tokio::test]
    async fn test_ip_pool_upsert_and_get() {
        let db = test_db().await;
        db.upsert_ip_pool(&make_pool("default")).await.unwrap();

        let pool = db.get_ip_pool("default").await.unwrap();
        assert_eq!(pool.gateway, "10.0.10.1");
        assert_eq!(pool.range_start, "10.0.10.10");
    }

    #[tokio::test]
    async fn test_ip_pool_not_found() {
        let db = test_db().await;
        let result = db.get_ip_pool("nonexistent").await;
        assert!(matches!(result, Err(DbError::NotFound(_))));
    }

    #[tokio::test]
    async fn test_allocate_ip_sequential() {
        let db = test_db().await;
        db.upsert_ip_pool(&make_pool("default")).await.unwrap();

        let ip1 = db.allocate_ip("default", IpOwner::Vm("vm1")).await.unwrap();
        assert_eq!(ip1, "10.0.10.10");

        let ip2 = db.allocate_ip("default", IpOwner::Vm("vm2")).await.unwrap();
        assert_eq!(ip2, "10.0.10.11");

        let ip3 = db.allocate_ip("default", IpOwner::Vm("vm3")).await.unwrap();
        assert_eq!(ip3, "10.0.10.12");
    }

    #[tokio::test]
    async fn test_allocate_ip_fills_gaps() {
        let db = test_db().await;
        db.upsert_ip_pool(&make_pool("default")).await.unwrap();

        db.allocate_ip("default", IpOwner::Vm("vm1")).await.unwrap();
        db.allocate_ip("default", IpOwner::Vm("vm2")).await.unwrap();
        db.allocate_ip("default", IpOwner::Vm("vm3")).await.unwrap();

        db.release_ips(IpOwner::Vm("vm2")).await.unwrap();

        let ip4 = db.allocate_ip("default", IpOwner::Vm("vm4")).await.unwrap();
        assert_eq!(ip4, "10.0.10.11");
    }

    #[tokio::test]
    async fn test_allocate_ip_pool_exhaustion() {
        let db = test_db().await;
        db.upsert_ip_pool(&make_pool("small")).await.unwrap();

        for i in 0..6 {
            db.allocate_ip("small", IpOwner::Vm(&format!("vm{i}")))
                .await
                .unwrap();
        }

        let result = db.allocate_ip("small", IpOwner::Vm("vm6")).await;
        assert!(matches!(result, Err(DbError::Conflict(_))));
    }

    #[tokio::test]
    async fn test_release_vm_ip() {
        let db = test_db().await;
        db.upsert_ip_pool(&make_pool("default")).await.unwrap();

        let ip = db.allocate_ip("default", IpOwner::Vm("vm1")).await.unwrap();
        assert_eq!(ip, "10.0.10.10");

        db.release_ips(IpOwner::Vm("vm1")).await.unwrap();

        let ip2 = db.allocate_ip("default", IpOwner::Vm("vm2")).await.unwrap();
        assert_eq!(ip2, "10.0.10.10");
    }

    #[tokio::test]
    async fn test_cluster_vip_is_separate_allocation() {
        let db = test_db().await;
        db.upsert_ip_pool(&make_pool("default")).await.unwrap();

        let vip = db
            .allocate_ip("default", IpOwner::ClusterVip("c1"))
            .await
            .unwrap();
        assert_eq!(vip, "10.0.10.10");

        // Subsequent VM allocations take the NEXT free IP, not the VIP.
        let vm_ip = db
            .allocate_ip("default", IpOwner::Vm("vm1"))
            .await
            .unwrap();
        assert_eq!(vm_ip, "10.0.10.11");

        // Releasing the VM does not release the VIP.
        db.release_ips(IpOwner::Vm("vm1")).await.unwrap();
        let vm_ip2 = db
            .allocate_ip("default", IpOwner::Vm("vm2"))
            .await
            .unwrap();
        assert_eq!(vm_ip2, "10.0.10.11");

        // Releasing the VIP makes its IP available again.
        db.release_ips(IpOwner::ClusterVip("c1")).await.unwrap();
        let reused = db
            .allocate_ip("default", IpOwner::Vm("vm3"))
            .await
            .unwrap();
        assert_eq!(reused, "10.0.10.10");
    }
}
