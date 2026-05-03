//! Local agent state: identity, live VMs, image cache, and the LVM
//! reservation ledger.
//!
//! The reservation ledger lives next to `local_vms` so the same SQLite
//! file is the agent's complete local truth — a backup of one file
//! captures both the VM record and the disk-allocation state needed to
//! reconcile it.

use std::path::Path;
use std::str::FromStr;

use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};

#[derive(Debug, thiserror::Error)]
pub enum AgentDbError {
    #[error("database error: {0}")]
    Sqlx(#[from] sqlx::Error),
}

#[derive(Debug, Clone)]
pub struct AgentDb {
    pool: SqlitePool,
}

impl AgentDb {
    pub async fn open(path: &Path) -> Result<Self, AgentDbError> {
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
            .max_connections(2)
            .connect_with(options)
            .await?;

        let db = Self { pool };
        db.migrate().await?;
        Ok(db)
    }

    pub fn raw_pool(&self) -> SqlitePool {
        self.pool.clone()
    }

    async fn migrate(&self) -> Result<(), AgentDbError> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS agent_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS local_vms (
                vm_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                unit_name TEXT NOT NULL,
                ip_address TEXT NOT NULL,
                cpu INTEGER NOT NULL,
                memory_mib INTEGER NOT NULL,
                disk_gib INTEGER NOT NULL,
                gpu_pci_addresses TEXT NOT NULL DEFAULT '[]',
                -- JSON-encoded Vec<LocalStorageDisk>: {assignment_id, pool, device_id,
                -- disk_index, size_gib, purpose}. Replaces the M0 extra_disk_gibs
                -- field; carries enough state to drive reconcile without joining
                -- against lvm_reservation.
                storage_disks TEXT NOT NULL DEFAULT '[]',
                image TEXT NOT NULL,
                vni INTEGER NOT NULL DEFAULT 0,
                cluster_id TEXT NOT NULL,
                created_at TEXT NOT NULL
            )",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS cached_images (
                image_ref TEXT PRIMARY KEY,
                local_path TEXT NOT NULL,
                size_bytes INTEGER NOT NULL DEFAULT 0,
                pulled_at TEXT NOT NULL
            )",
        )
        .execute(&self.pool)
        .await?;

        // LVM data-disk reservation ledger. The owner key is
        // (vm_id, disk_index); the hardware key is (vg, lv_name). The
        // assignment_id is the controller's idempotency key — same id
        // resends are no-ops, different id for the same (vm, disk) is a
        // hard conflict the agent surfaces.
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS lvm_reservation (
                assignment_id TEXT PRIMARY KEY,
                pool          TEXT NOT NULL,
                device_id     TEXT NOT NULL,
                vg            TEXT NOT NULL,
                lv_name       TEXT NOT NULL,
                vm_id         TEXT NOT NULL,
                disk_index    INTEGER NOT NULL,
                size_gib      INTEGER NOT NULL,
                state         TEXT NOT NULL,    -- Creating | Ready | Deleting
                reserved_at   TEXT NOT NULL,
                UNIQUE (vm_id, disk_index),
                UNIQUE (vg, lv_name)
            )",
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    // --- Agent identity ---

    pub async fn get_host_id(&self) -> Result<Option<String>, AgentDbError> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT value FROM agent_state WHERE key = 'host_id'")
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.map(|r| r.0))
    }

    pub async fn set_host_id(&self, host_id: &str) -> Result<(), AgentDbError> {
        sqlx::query(
            "INSERT INTO agent_state (key, value) VALUES ('host_id', ?)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        )
        .bind(host_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    // --- Local VM tracking ---

    pub async fn insert_vm(&self, vm: &LocalVmRow) -> Result<(), AgentDbError> {
        sqlx::query(
            "INSERT INTO local_vms (vm_id, name, unit_name, ip_address, cpu, memory_mib,
                disk_gib, gpu_pci_addresses, storage_disks, image, vni, cluster_id, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&vm.vm_id)
        .bind(&vm.name)
        .bind(&vm.unit_name)
        .bind(&vm.ip_address)
        .bind(vm.cpu)
        .bind(vm.memory_mib)
        .bind(vm.disk_gib)
        .bind(&vm.gpu_pci_addresses)
        .bind(&vm.storage_disks)
        .bind(&vm.image)
        .bind(vm.vni)
        .bind(&vm.cluster_id)
        .bind(&vm.created_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn delete_vm(&self, vm_id: &str) -> Result<(), AgentDbError> {
        sqlx::query("DELETE FROM local_vms WHERE vm_id = ?")
            .bind(vm_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn list_vms(&self) -> Result<Vec<LocalVmRow>, AgentDbError> {
        Ok(sqlx::query_as::<_, LocalVmRow>("SELECT * FROM local_vms")
            .fetch_all(&self.pool)
            .await?)
    }

    pub async fn get_vm(&self, vm_id: &str) -> Result<Option<LocalVmRow>, AgentDbError> {
        Ok(
            sqlx::query_as::<_, LocalVmRow>("SELECT * FROM local_vms WHERE vm_id = ?")
                .bind(vm_id)
                .fetch_optional(&self.pool)
                .await?,
        )
    }

    // --- Image cache tracking ---

    pub async fn record_cached_image(
        &self,
        image_ref: &str,
        local_path: &str,
        size_bytes: i64,
        now: &str,
    ) -> Result<(), AgentDbError> {
        sqlx::query(
            "INSERT INTO cached_images (image_ref, local_path, size_bytes, pulled_at)
             VALUES (?, ?, ?, ?)
             ON CONFLICT(image_ref) DO UPDATE SET
                local_path = excluded.local_path,
                size_bytes = excluded.size_bytes,
                pulled_at = excluded.pulled_at",
        )
        .bind(image_ref)
        .bind(local_path)
        .bind(size_bytes)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct LocalVmRow {
    pub vm_id: String,
    pub name: String,
    pub unit_name: String,
    pub ip_address: String,
    pub cpu: i64,
    pub memory_mib: i64,
    pub disk_gib: i64,
    pub gpu_pci_addresses: String,
    pub image: String,
    pub vni: i64,
    pub cluster_id: String,
    pub created_at: String,
    /// JSON-encoded `Vec<LocalStorageDisk>`. One entry per data disk
    /// the controller commanded; carries the assignment_id, pool, and
    /// device_id needed to drive release/reconcile without consulting
    /// the lvm_reservation ledger.
    pub storage_disks: String,
}

impl LocalVmRow {
    pub fn gpus(&self) -> serde_json::Result<Vec<String>> {
        serde_json::from_str(&self.gpu_pci_addresses)
    }

    pub fn parsed_storage_disks(&self) -> serde_json::Result<Vec<LocalStorageDisk>> {
        serde_json::from_str(&self.storage_disks)
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LocalStorageDisk {
    pub assignment_id: String,
    pub pool: String,
    pub device_id: String,
    pub disk_index: u32,
    pub size_gib: u64,
    pub purpose: String,
    /// Block device path the backend allocated (`/dev/<vg>/<lv>` for
    /// lvm-linear, `/dev/disk/by-id/<id>` for raw-disk, etc). Persisted
    /// here so post-reboot restart can pass it straight to cloud-
    /// hypervisor without re-querying the backend.
    pub device_path: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn test_db() -> AgentDb {
        AgentDb::open(":memory:".as_ref()).await.unwrap()
    }

    fn make_vm(vm_id: &str) -> LocalVmRow {
        LocalVmRow {
            vm_id: vm_id.to_string(),
            name: format!("vm-{vm_id}"),
            unit_name: format!("basis-vm-{vm_id}.service"),
            ip_address: "10.0.10.42".to_string(),
            cpu: 4,
            memory_mib: 8192,
            disk_gib: 100,
            gpu_pci_addresses: "[]".to_string(),
            image: "test-image:latest".to_string(),
            vni: 10_000,
            cluster_id: "cluster-x".to_string(),
            created_at: "2025-01-01T00:00:00Z".to_string(),
            storage_disks: "[]".to_string(),
        }
    }

    #[tokio::test]
    async fn host_id_roundtrip() {
        let db = test_db().await;
        assert!(db.get_host_id().await.unwrap().is_none());
        db.set_host_id("host-abc-123").await.unwrap();
        assert_eq!(db.get_host_id().await.unwrap().unwrap(), "host-abc-123");
        db.set_host_id("host-xyz-456").await.unwrap();
        assert_eq!(db.get_host_id().await.unwrap().unwrap(), "host-xyz-456");
    }

    #[tokio::test]
    async fn vm_insert_and_list() {
        let db = test_db().await;
        db.insert_vm(&make_vm("vm1")).await.unwrap();
        db.insert_vm(&make_vm("vm2")).await.unwrap();
        assert_eq!(db.list_vms().await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn vm_get() {
        let db = test_db().await;
        db.insert_vm(&make_vm("vm1")).await.unwrap();
        let vm = db.get_vm("vm1").await.unwrap().unwrap();
        assert_eq!(vm.cluster_id, "cluster-x");
        assert!(db.get_vm("nonexistent").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn vm_delete() {
        let db = test_db().await;
        db.insert_vm(&make_vm("vm1")).await.unwrap();
        db.insert_vm(&make_vm("vm2")).await.unwrap();
        db.delete_vm("vm1").await.unwrap();
        let vms = db.list_vms().await.unwrap();
        assert_eq!(vms.len(), 1);
        assert_eq!(vms[0].vm_id, "vm2");
        // Idempotent.
        db.delete_vm("nonexistent").await.unwrap();
    }

    #[tokio::test]
    async fn cached_image_upsert() {
        let db = test_db().await;
        db.record_cached_image("img:v1", "/p/a", 1, "t1")
            .await
            .unwrap();
        db.record_cached_image("img:v1", "/p/b", 2, "t2")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn storage_disks_roundtrip() {
        let db = test_db().await;
        let mut vm = make_vm("v");
        let disks = vec![LocalStorageDisk {
            assignment_id: "a1".into(),
            pool: "fast".into(),
            device_id: "nvme-X".into(),
            disk_index: 0,
            size_gib: 175,
            purpose: "replicated".into(),
            device_path: "/dev/basis-fast-X/basis-data-v-0".into(),
        }];
        vm.storage_disks = serde_json::to_string(&disks).unwrap();
        db.insert_vm(&vm).await.unwrap();
        let parsed = db
            .get_vm("v")
            .await
            .unwrap()
            .unwrap()
            .parsed_storage_disks()
            .unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].pool, "fast");
    }
}
