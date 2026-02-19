use actix_web::{web, HttpResponse};
use chrono::Utc;
use serde::Serialize;
use std::sync::Arc;
use sysinfo::{Pid, System};
use tokio::sync::Mutex;

/// Cached process metrics collector.
/// Keeps a `System` instance alive between calls so CPU usage deltas are meaningful.
pub struct ProcessMetricsCollector {
    inner: Mutex<CollectorInner>,
    start_time: chrono::DateTime<Utc>,
}

struct CollectorInner {
    sys: System,
    pid: Pid,
}

/// JSON response for GET /api/metrics/process
#[derive(Debug, Clone, Serialize)]
pub struct ProcessMetrics {
    pub cpu_usage_percent: f32,
    pub memory_rss_bytes: u64,
    pub memory_rss_mb: f64,
    pub virtual_memory_bytes: u64,
    pub uptime_seconds: i64,
    pub timestamp: chrono::DateTime<Utc>,
}

impl ProcessMetricsCollector {
    pub fn new() -> Self {
        let pid = Pid::from_u32(std::process::id());
        let mut sys = System::new();
        // Initial refresh so the first call has a baseline for CPU delta
        sys.refresh_processes_specifics(
            sysinfo::ProcessesToUpdate::Some(&[pid]),
            true,
            sysinfo::ProcessRefreshKind::everything(),
        );
        Self {
            inner: Mutex::new(CollectorInner { sys, pid }),
            start_time: Utc::now(),
        }
    }

    pub async fn snapshot(&self) -> ProcessMetrics {
        let mut inner = self.inner.lock().await;
        let pid = inner.pid;
        // Refresh only our own process — very cheap
        inner.sys.refresh_processes_specifics(
            sysinfo::ProcessesToUpdate::Some(&[pid]),
            true,
            sysinfo::ProcessRefreshKind::everything(),
        );

        let now = Utc::now();
        let uptime = (now - self.start_time).num_seconds();

        match inner.sys.process(pid) {
            Some(proc) => ProcessMetrics {
                cpu_usage_percent: proc.cpu_usage(),
                memory_rss_bytes: proc.memory(),
                memory_rss_mb: proc.memory() as f64 / (1024.0 * 1024.0),
                virtual_memory_bytes: proc.virtual_memory(),
                uptime_seconds: uptime,
                timestamp: now,
            },
            None => ProcessMetrics {
                cpu_usage_percent: 0.0,
                memory_rss_bytes: 0,
                memory_rss_mb: 0.0,
                virtual_memory_bytes: 0,
                uptime_seconds: uptime,
                timestamp: now,
            },
        }
    }
}

/// GET /api/metrics/process — current process CPU / memory / uptime.
pub async fn get_process_metrics(
    collector: web::Data<Arc<ProcessMetricsCollector>>,
) -> HttpResponse {
    let metrics = collector.snapshot().await;
    HttpResponse::Ok().json(metrics)
}
