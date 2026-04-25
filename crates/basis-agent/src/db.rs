use std::path::Path;
use std::str::FromStr;

use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};

#[derive(Debug, thiserror::Error)]
pub enum AgentDbError {
    #[error("database error: {0}")]
    Sqlx(#[from] sqlx::Error),
}

/// Local agent state database. Survives agent restarts so we can reconcile
/// running VMs, cached images, and our controller-assigned host_id.
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
                extra_disk_gibs TEXT NOT NULL DEFAULT '[]',
                image TEXT NOT NULL,
                vni INTEGER NOT NULL DEFAULT 0,
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
            "INSERT INTO local_vms (vm_id, name, unit_name, ip_address, cpu, memory_mib, disk_gib,
                gpu_pci_addresses, image, vni, created_at, extra_disk_gibs)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&vm.vm_id)
        .bind(&vm.name)
        .bind(&vm.unit_name)
        .bind(&vm.ip_address)
        .bind(vm.cpu)
        .bind(vm.memory_mib)
        .bind(vm.disk_gib)
        .bind(&vm.gpu_pci_addresses)
        .bind(&vm.image)
        .bind(vm.vni)
        .bind(&vm.created_at)
        .bind(&vm.extra_disk_gibs)
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
    /// VXLAN Network Identifier of the tree this VM's primary TAP
    /// attaches to. Persisted so a post-reboot restart knows which
    /// `brt<vni>` bridge to re-attach the TAP to.
    pub vni: i64,
    pub created_at: String,
    /// JSON-encoded `Vec<u32>` of extra data-disk sizes in GiB, in the
    /// same order the guest enumerates them as `/dev/vdc`, `/dev/vdd`, …
    pub extra_disk_gibs: String,
}

impl LocalVmRow {
    /// Parsed PCI addresses of every GPU assigned to this VM. One place
    /// for the `Vec<String>` decode so a schema tweak can't drift
    /// between `handlers.rs` and `reconcile.rs`.
    pub fn gpus(&self) -> Vec<String> {
        basis_common::json::parse_owned_json(&self.gpu_pci_addresses, "local_vms.gpu_pci_addresses")
    }

    /// Parsed per-extra-disk sizes, in the same order the guest sees
    /// them on the virtio bus.
    pub fn extra_disks(&self) -> Vec<u32> {
        basis_common::json::parse_owned_json(&self.extra_disk_gibs, "local_vms.extra_disk_gibs")
    }
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
            created_at: "2025-01-01T00:00:00Z".to_string(),
            extra_disk_gibs: "[]".to_string(),
        }
    }

    #[tokio::test]
    async fn test_host_id_roundtrip() {
        let db = test_db().await;

        assert!(db.get_host_id().await.unwrap().is_none());

        db.set_host_id("host-abc-123").await.unwrap();
        assert_eq!(db.get_host_id().await.unwrap().unwrap(), "host-abc-123");

        // Overwrite
        db.set_host_id("host-xyz-456").await.unwrap();
        assert_eq!(db.get_host_id().await.unwrap().unwrap(), "host-xyz-456");
    }

    #[tokio::test]
    async fn test_vm_insert_and_list() {
        let db = test_db().await;
        db.insert_vm(&make_vm("vm1")).await.unwrap();
        db.insert_vm(&make_vm("vm2")).await.unwrap();

        let vms = db.list_vms().await.unwrap();
        assert_eq!(vms.len(), 2);
    }

    #[tokio::test]
    async fn test_vm_get() {
        let db = test_db().await;
        db.insert_vm(&make_vm("vm1")).await.unwrap();

        let vm = db.get_vm("vm1").await.unwrap();
        assert!(vm.is_some());
        let vm = vm.unwrap();
        assert_eq!(vm.name, "vm-vm1");
        assert_eq!(vm.cpu, 4);
        assert_eq!(vm.memory_mib, 8192);

        let none = db.get_vm("nonexistent").await.unwrap();
        assert!(none.is_none());
    }

    #[tokio::test]
    async fn test_vm_delete() {
        let db = test_db().await;
        db.insert_vm(&make_vm("vm1")).await.unwrap();
        db.insert_vm(&make_vm("vm2")).await.unwrap();

        db.delete_vm("vm1").await.unwrap();

        let vms = db.list_vms().await.unwrap();
        assert_eq!(vms.len(), 1);
        assert_eq!(vms[0].vm_id, "vm2");
    }

    #[tokio::test]
    async fn test_vm_delete_nonexistent_is_ok() {
        let db = test_db().await;
        // Should not error
        db.delete_vm("nonexistent").await.unwrap();
    }

    #[tokio::test]
    async fn test_cached_image_recording() {
        let db = test_db().await;
        db.record_cached_image(
            "ghcr.io/evan-hines-js/node:v1.32",
            "/var/lib/basis/images/node_v1_32.qcow2",
            1073741824,
            "2025-01-01T00:00:00Z",
        )
        .await
        .unwrap();

        // Upsert same image with new path (e.g., re-pull)
        db.record_cached_image(
            "ghcr.io/evan-hines-js/node:v1.32",
            "/var/lib/basis/images/node_v1_32_new.qcow2",
            2147483648,
            "2025-01-02T00:00:00Z",
        )
        .await
        .unwrap();

        // Should not error (upsert semantics)
    }

    #[tokio::test]
    async fn test_vm_with_gpu_assignments() {
        let db = test_db().await;
        let mut vm = make_vm("gpu-vm");
        vm.gpu_pci_addresses =
            serde_json::to_string(&vec!["0000:41:00.0", "0000:42:00.0"]).unwrap();

        db.insert_vm(&vm).await.unwrap();

        let fetched = db.get_vm("gpu-vm").await.unwrap().unwrap();
        let addrs: Vec<String> = serde_json::from_str(&fetched.gpu_pci_addresses).unwrap();
        assert_eq!(addrs.len(), 2);
        assert_eq!(addrs[0], "0000:41:00.0");
    }
}
