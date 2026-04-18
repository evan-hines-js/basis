//! Host resource discovery: CPU count, total memory, available disk.
//!
//! Captured once at agent startup and reported on registration. The
//! controller's `available_*` accounting is derived from running VMs, not
//! re-polled here.

use std::path::Path;

use tracing::warn;

pub struct HostResources {
    pub total_cpu: u32,
    pub total_memory_mib: u64,
    pub total_disk_gib: u64,
}

impl HostResources {
    pub fn discover(data_dir: &Path) -> Self {
        Self {
            total_cpu: num_cpus(),
            total_memory_mib: total_memory_mib(),
            total_disk_gib: disk_space_gib(data_dir),
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

/// Available disk space for the agent's data directory, in GiB.
///
/// Uses `df --output=avail -B1` so we don't need a platform-specific
/// statvfs binding. Returns 0 if df is unavailable or errors.
fn disk_space_gib(path: &Path) -> u64 {
    let output = std::process::Command::new("df")
        .args(["--output=avail", "-B1", &path.to_string_lossy()])
        .output();

    let output = match output {
        Ok(o) if o.status.success() => o,
        Ok(_) | Err(_) => {
            warn!("df failed; reporting 0 disk available");
            return 0;
        }
    };

    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout
        .lines()
        .nth(1)
        .and_then(|line| line.trim().parse::<u64>().ok())
        .map(|bytes| bytes / (1024 * 1024 * 1024))
        .unwrap_or(0)
}
