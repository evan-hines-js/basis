//! Host resource discovery: CPU count, total memory, VM-disk capacity.
//!
//! Captured once at agent startup and reported on registration. The
//! controller's `available_*` accounting is derived from running VMs, not
//! re-polled here.

pub struct HostResources {
    pub total_cpu: u32,
    pub total_memory_mib: u64,
    pub total_disk_gib: u64,
}

impl HostResources {
    /// `thin_pool_data_total_bytes` comes from `lvm::validate_pool` at
    /// startup. VMs don't live on a filesystem anymore — df on data_dir
    /// would report the OS disk's free space, which is irrelevant to
    /// VM-disk capacity. Pool data total is the authoritative number.
    pub fn discover(thin_pool_data_total_bytes: u64) -> Self {
        Self {
            total_cpu: num_cpus(),
            total_memory_mib: total_memory_mib(),
            total_disk_gib: thin_pool_data_total_bytes / (1 << 30),
        }
    }
}

fn num_cpus() -> u32 {
    std::thread::available_parallelism()
        .map(|n| n.get() as u32)
        .unwrap_or(1)
}

/// Parse MemTotal from `/proc/meminfo`. Returns 0 on non-Linux or if the
/// file is unreadable — the controller sees `total_memory_mib=0` for this
/// host, which makes scheduling correctly refuse to place VMs on it.
fn total_memory_mib() -> u64 {
    let Ok(contents) = std::fs::read_to_string("/proc/meminfo") else {
        return 0;
    };
    for line in contents.lines() {
        let Some(rest) = line.strip_prefix("MemTotal:") else {
            continue;
        };
        if let Some(kb_str) = rest.split_whitespace().next() {
            if let Ok(kb) = kb_str.parse::<u64>() {
                return kb / 1024;
            }
        }
    }
    0
}
