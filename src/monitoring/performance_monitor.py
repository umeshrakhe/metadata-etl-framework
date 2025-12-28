import logging
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
import uuid

# Optional imports
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    psutil = None
    PSUTIL_AVAILABLE = False

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    pd = None
    PANDAS_AVAILABLE = False

logger = logging.getLogger("PerformanceMonitor")


@dataclass
class SystemMetrics:
    """Container for system performance metrics."""
    timestamp: datetime
    cpu_percent: float
    cpu_per_core: List[float]
    memory_used_mb: float
    memory_available_mb: float
    memory_percent: float
    disk_read_mb: float
    disk_write_mb: float
    disk_read_iops: int
    disk_write_iops: int
    network_recv_mb: float
    network_sent_mb: float
    process_cpu_percent: Optional[float] = None
    process_memory_mb: Optional[float] = None
    process_memory_percent: Optional[float] = None


@dataclass
class PerformanceAnalysis:
    """Container for performance analysis results."""
    run_id: str
    avg_cpu_percent: float
    peak_cpu_percent: float
    avg_memory_percent: float
    peak_memory_percent: float
    total_disk_read_mb: float
    total_disk_write_mb: float
    total_network_recv_mb: float
    total_network_sent_mb: float
    duration_seconds: float
    bottlenecks: List[str]
    recommendations: List[str]
    overall_score: float


class PerformanceMonitor:
    """
    Comprehensive performance monitoring for ETL pipelines.
    Tracks system resources, identifies bottlenecks, and provides optimization recommendations.
    """

    def __init__(self, db_connection: Any = None, monitoring_interval: float = 1.0):
        self.db = db_connection
        self.monitoring_interval = monitoring_interval
        self.logger = logging.getLogger(self.__class__.__name__)
        self.active_monitors: Dict[str, threading.Thread] = {}
        self.monitoring_data: Dict[str, List[SystemMetrics]] = {}
        self.baseline_metrics: Dict[str, SystemMetrics] = {}

        if not PSUTIL_AVAILABLE:
            self.logger.warning("psutil not available - performance monitoring will be limited")

    def _check_psutil(self) -> None:
        """Check if psutil is available."""
        if not PSUTIL_AVAILABLE:
            raise ImportError("psutil required for performance monitoring")

    def _get_process_info(self) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        """Get current process CPU and memory usage."""
        try:
            process = psutil.Process()
            cpu_percent = process.cpu_percent(interval=0.1)
            memory_info = process.memory_info()
            memory_mb = memory_info.rss / 1024 / 1024  # Convert to MB
            memory_percent = process.memory_percent()
            return cpu_percent, memory_mb, memory_percent
        except Exception:
            return None, None, None

    def collect_cpu_metrics(self) -> Dict[str, Any]:
        """Collect CPU usage metrics."""
        self._check_psutil()

        try:
            cpu_percent = psutil.cpu_percent(interval=0.1)
            cpu_per_core = psutil.cpu_percent(percpu=True, interval=0.1)
            cpu_freq = psutil.cpu_freq()

            return {
                "total_percent": cpu_percent,
                "per_core": cpu_per_core,
                "frequency_mhz": cpu_freq.current if cpu_freq else None,
                "cores": psutil.cpu_count(),
                "logical_cores": psutil.cpu_count(logical=True)
            }
        except Exception as e:
            self.logger.exception(f"Failed to collect CPU metrics: {e}")
            return {}

    def collect_memory_metrics(self) -> Dict[str, Any]:
        """Collect memory usage metrics."""
        self._check_psutil()

        try:
            memory = psutil.virtual_memory()
            swap = psutil.swap_memory()

            return {
                "total_mb": memory.total / 1024 / 1024,
                "used_mb": memory.used / 1024 / 1024,
                "available_mb": memory.available / 1024 / 1024,
                "percent": memory.percent,
                "swap_total_mb": swap.total / 1024 / 1024,
                "swap_used_mb": swap.used / 1024 / 1024,
                "swap_percent": swap.percent
            }
        except Exception as e:
            self.logger.exception(f"Failed to collect memory metrics: {e}")
            return {}

    def collect_disk_io_metrics(self) -> Dict[str, Any]:
        """Collect disk I/O metrics."""
        self._check_psutil()

        try:
            disk_io = psutil.disk_io_counters()
            if disk_io:
                return {
                    "read_bytes": disk_io.read_bytes,
                    "write_bytes": disk_io.write_bytes,
                    "read_count": disk_io.read_count,
                    "write_count": disk_io.write_count,
                    "read_time_ms": disk_io.read_time,
                    "write_time_ms": disk_io.write_time
                }
            return {}
        except Exception as e:
            self.logger.exception(f"Failed to collect disk I/O metrics: {e}")
            return {}

    def collect_network_io_metrics(self) -> Dict[str, Any]:
        """Collect network I/O metrics."""
        self._check_psutil()

        try:
            network_io = psutil.net_io_counters()
            if network_io:
                return {
                    "bytes_recv": network_io.bytes_recv,
                    "bytes_sent": network_io.bytes_sent,
                    "packets_recv": network_io.packets_recv,
                    "packets_sent": network_io.packets_sent,
                    "errin": network_io.errin,
                    "errout": network_io.errout,
                    "dropin": network_io.dropin,
                    "dropout": network_io.dropout
                }
            return {}
        except Exception as e:
            self.logger.exception(f"Failed to collect network I/O metrics: {e}")
            return {}

    def get_current_metrics(self) -> SystemMetrics:
        """Get current system metrics snapshot."""
        self._check_psutil()

        try:
            timestamp = datetime.utcnow()

            # CPU metrics
            cpu_percent = psutil.cpu_percent(interval=0.1)
            cpu_per_core = psutil.cpu_percent(percpu=True, interval=0.1)

            # Memory metrics
            memory = psutil.virtual_memory()
            memory_used_mb = memory.used / 1024 / 1024
            memory_available_mb = memory.available / 1024 / 1024
            memory_percent = memory.percent

            # Disk I/O metrics (per second rates would need previous measurement)
            disk_io = psutil.disk_io_counters()
            disk_read_mb = disk_io.read_bytes / 1024 / 1024 if disk_io else 0
            disk_write_mb = disk_io.write_bytes / 1024 / 1024 if disk_io else 0
            disk_read_iops = disk_io.read_count if disk_io else 0
            disk_write_iops = disk_io.write_count if disk_io else 0

            # Network I/O metrics
            network_io = psutil.net_io_counters()
            network_recv_mb = network_io.bytes_recv / 1024 / 1024 if network_io else 0
            network_sent_mb = network_io.bytes_sent / 1024 / 1024 if network_io else 0

            # Process-specific metrics
            process_cpu, process_memory_mb, process_memory_percent = self._get_process_info()

            return SystemMetrics(
                timestamp=timestamp,
                cpu_percent=cpu_percent,
                cpu_per_core=cpu_per_core,
                memory_used_mb=memory_used_mb,
                memory_available_mb=memory_available_mb,
                memory_percent=memory_percent,
                disk_read_mb=disk_read_mb,
                disk_write_mb=disk_write_mb,
                disk_read_iops=disk_read_iops,
                disk_write_iops=disk_write_iops,
                network_recv_mb=network_recv_mb,
                network_sent_mb=network_sent_mb,
                process_cpu_percent=process_cpu,
                process_memory_mb=process_memory_mb,
                process_memory_percent=process_memory_percent
            )

        except Exception as e:
            self.logger.exception(f"Failed to get current metrics: {e}")
            raise

    def record_metrics(self, run_id: str, metrics: SystemMetrics) -> None:
        """Record metrics in PERFORMANCE_METRICS table."""
        if not self.db:
            self.logger.warning("No database connection - metrics not persisted")
            return

        try:
            sql = """
                INSERT INTO PERFORMANCE_METRICS (
                    metric_id, run_id, timestamp, cpu_percent, memory_used_mb,
                    memory_available_mb, memory_percent, disk_read_mb, disk_write_mb,
                    disk_read_iops, disk_write_iops, network_recv_mb, network_sent_mb,
                    process_cpu_percent, process_memory_mb, process_memory_percent,
                    created_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """

            metric_id = str(uuid.uuid4())
            params = (
                metric_id,
                run_id,
                metrics.timestamp,
                metrics.cpu_percent,
                metrics.memory_used_mb,
                metrics.memory_available_mb,
                metrics.memory_percent,
                metrics.disk_read_mb,
                metrics.disk_write_mb,
                metrics.disk_read_iops,
                metrics.disk_write_iops,
                metrics.network_recv_mb,
                metrics.network_sent_mb,
                metrics.process_cpu_percent,
                metrics.process_memory_mb,
                metrics.process_memory_percent,
                datetime.utcnow()
            )

            self.db.begin()
            self.db.execute(sql, params)
            self.db.commit()

            self.logger.debug(f"Recorded metrics for run {run_id}")

        except Exception as e:
            self.db.rollback()
            self.logger.exception(f"Failed to record metrics: {e}")

    def _monitoring_worker(self, run_id: str, stop_event: threading.Event) -> None:
        """Background worker for continuous monitoring."""
        self.monitoring_data[run_id] = []

        while not stop_event.is_set():
            try:
                metrics = self.get_current_metrics()
                self.monitoring_data[run_id].append(metrics)
                self.record_metrics(run_id, metrics)
                time.sleep(self.monitoring_interval)
            except Exception as e:
                self.logger.exception(f"Monitoring worker error: {e}")
                break

    def start_monitoring(self, run_id: str) -> None:
        """Start monitoring for a pipeline run."""
        if run_id in self.active_monitors:
            self.logger.warning(f"Monitoring already active for run {run_id}")
            return

        stop_event = threading.Event()
        worker_thread = threading.Thread(
            target=self._monitoring_worker,
            args=(run_id, stop_event),
            daemon=True
        )

        self.active_monitors[run_id] = {
            "thread": worker_thread,
            "stop_event": stop_event,
            "start_time": datetime.utcnow()
        }

        worker_thread.start()
        self.logger.info(f"Started monitoring for run {run_id}")

    def stop_monitoring(self, run_id: str) -> None:
        """Stop monitoring for a pipeline run."""
        if run_id not in self.active_monitors:
            self.logger.warning(f"No active monitoring for run {run_id}")
            return

        monitor_info = self.active_monitors[run_id]
        monitor_info["stop_event"].set()
        monitor_info["thread"].join(timeout=5.0)
        monitor_info["end_time"] = datetime.utcnow()

        # Calculate final metrics
        if run_id in self.monitoring_data and self.monitoring_data[run_id]:
            analysis = self.calculate_resource_utilization(run_id)
            self.logger.info(f"Monitoring stopped for run {run_id}. "
                           f"Duration: {analysis.duration_seconds:.2f}s, "
                           f"Avg CPU: {analysis.avg_cpu_percent:.1f}%, "
                           f"Peak CPU: {analysis.peak_cpu_percent:.1f}%")

        del self.active_monitors[run_id]

    def calculate_resource_utilization(self, run_id: str) -> PerformanceAnalysis:
        """Analyze resource utilization for a run."""
        if run_id not in self.monitoring_data or not self.monitoring_data[run_id]:
            raise ValueError(f"No monitoring data available for run {run_id}")

        metrics_list = self.monitoring_data[run_id]

        # Calculate averages and peaks
        cpu_percents = [m.cpu_percent for m in metrics_list]
        memory_percents = [m.memory_percent for m in metrics_list]

        avg_cpu = sum(cpu_percents) / len(cpu_percents)
        peak_cpu = max(cpu_percents)
        avg_memory = sum(memory_percents) / len(memory_percents)
        peak_memory = max(memory_percents)

        # Calculate totals
        total_disk_read = sum(m.disk_read_mb for m in metrics_list)
        total_disk_write = sum(m.disk_write_mb for m in metrics_list)
        total_network_recv = sum(m.network_recv_mb for m in metrics_list)
        total_network_sent = sum(m.network_sent_mb for m in metrics_list)

        # Calculate duration
        start_time = metrics_list[0].timestamp
        end_time = metrics_list[-1].timestamp
        duration = (end_time - start_time).total_seconds()

        # Identify bottlenecks
        bottlenecks = []
        if peak_cpu > 90:
            bottlenecks.append("High CPU usage (>90%)")
        if peak_memory > 90:
            bottlenecks.append("High memory usage (>90%)")
        if avg_cpu > 70:
            bottlenecks.append("Sustained high CPU usage (>70%)")
        if avg_memory > 80:
            bottlenecks.append("High average memory usage (>80%)")

        # Generate recommendations
        recommendations = []
        if peak_cpu > 80:
            recommendations.append("Consider increasing CPU resources or optimizing CPU-intensive operations")
        if peak_memory > 85:
            recommendations.append("Consider increasing memory resources or implementing data chunking")
        if total_disk_read > 1000:  # MB
            recommendations.append("High disk I/O - consider SSD storage or data caching")
        if total_network_recv + total_network_sent > 500:  # MB
            recommendations.append("High network traffic - consider data compression or local processing")

        # Calculate overall score (0-100, higher is better)
        cpu_score = max(0, 100 - avg_cpu)
        memory_score = max(0, 100 - avg_memory)
        overall_score = (cpu_score + memory_score) / 2

        return PerformanceAnalysis(
            run_id=run_id,
            avg_cpu_percent=avg_cpu,
            peak_cpu_percent=peak_cpu,
            avg_memory_percent=avg_memory,
            peak_memory_percent=peak_memory,
            total_disk_read_mb=total_disk_read,
            total_disk_write_mb=total_disk_write,
            total_network_recv_mb=total_network_recv,
            total_network_sent_mb=total_network_sent,
            duration_seconds=duration,
            bottlenecks=bottlenecks,
            recommendations=recommendations,
            overall_score=overall_score
        )

    def identify_bottlenecks(self, run_id: str) -> List[str]:
        """Identify performance bottlenecks for a run."""
        analysis = self.calculate_resource_utilization(run_id)
        return analysis.bottlenecks

    def compare_with_baseline(self, run_id: str, baseline_run_id: str) -> Dict[str, Any]:
        """Compare performance with a baseline run."""
        if not self.db:
            raise ValueError("Database connection required for baseline comparison")

        try:
            # Get current run analysis
            current_analysis = self.calculate_resource_utilization(run_id)

            # Get baseline metrics from database
            sql = """
                SELECT AVG(cpu_percent) as avg_cpu, MAX(cpu_percent) as peak_cpu,
                       AVG(memory_percent) as avg_memory, MAX(memory_percent) as peak_memory,
                       SUM(disk_read_mb) as total_disk_read, SUM(disk_write_mb) as total_disk_write,
                       SUM(network_recv_mb) as total_network_recv, SUM(network_sent_mb) as total_network_sent,
                       COUNT(*) as metric_count
                FROM PERFORMANCE_METRICS
                WHERE run_id = %s
            """

            self.db.execute(sql, (baseline_run_id,))
            baseline_row = self.db.fetchone()

            if not baseline_row:
                raise ValueError(f"No baseline data found for run {baseline_run_id}")

            baseline_avg_cpu = baseline_row[0] or 0
            baseline_peak_cpu = baseline_row[1] or 0
            baseline_avg_memory = baseline_row[2] or 0
            baseline_peak_memory = baseline_row[3] or 0
            baseline_disk_read = baseline_row[4] or 0
            baseline_disk_write = baseline_row[5] or 0
            baseline_network_recv = baseline_row[6] or 0
            baseline_network_sent = baseline_row[7] or 0

            # Calculate comparisons
            comparisons = {
                "cpu_avg_change": current_analysis.avg_cpu_percent - baseline_avg_cpu,
                "cpu_peak_change": current_analysis.peak_cpu_percent - baseline_peak_cpu,
                "memory_avg_change": current_analysis.avg_memory_percent - baseline_avg_memory,
                "memory_peak_change": current_analysis.peak_memory_percent - baseline_peak_memory,
                "disk_read_change": current_analysis.total_disk_read_mb - baseline_disk_read,
                "disk_write_change": current_analysis.total_disk_write_mb - baseline_disk_write,
                "network_recv_change": current_analysis.total_network_recv_mb - baseline_network_recv,
                "network_sent_change": current_analysis.total_network_sent_mb - baseline_network_sent,
                "current_analysis": current_analysis,
                "baseline_metrics": {
                    "avg_cpu": baseline_avg_cpu,
                    "peak_cpu": baseline_peak_cpu,
                    "avg_memory": baseline_avg_memory,
                    "peak_memory": baseline_peak_memory,
                    "total_disk_read": baseline_disk_read,
                    "total_disk_write": baseline_disk_write,
                    "total_network_recv": baseline_network_recv,
                    "total_network_sent": baseline_network_sent
                }
            }

            return comparisons

        except Exception as e:
            self.logger.exception(f"Failed to compare with baseline: {e}")
            raise

    def generate_performance_report(self, run_id: str) -> Dict[str, Any]:
        """Generate detailed performance report."""
        if run_id not in self.monitoring_data or not self.monitoring_data[run_id]:
            raise ValueError(f"No monitoring data available for run {run_id}")

        analysis = self.calculate_resource_utilization(run_id)
        metrics_list = self.monitoring_data[run_id]

        # Generate time series data
        timestamps = [m.timestamp.isoformat() for m in metrics_list]
        cpu_data = [m.cpu_percent for m in metrics_list]
        memory_data = [m.memory_percent for m in metrics_list]

        report = {
            "run_id": run_id,
            "analysis": {
                "avg_cpu_percent": analysis.avg_cpu_percent,
                "peak_cpu_percent": analysis.peak_cpu_percent,
                "avg_memory_percent": analysis.avg_memory_percent,
                "peak_memory_percent": analysis.peak_memory_percent,
                "total_disk_read_mb": analysis.total_disk_read_mb,
                "total_disk_write_mb": analysis.total_disk_write_mb,
                "total_network_recv_mb": analysis.total_network_recv_mb,
                "total_network_sent_mb": analysis.total_network_sent_mb,
                "duration_seconds": analysis.duration_seconds,
                "overall_score": analysis.overall_score
            },
            "bottlenecks": analysis.bottlenecks,
            "recommendations": analysis.recommendations,
            "time_series": {
                "timestamps": timestamps,
                "cpu_percent": cpu_data,
                "memory_percent": memory_data
            },
            "summary": {
                "total_metrics_collected": len(metrics_list),
                "monitoring_interval_seconds": self.monitoring_interval,
                "generated_at": datetime.utcnow().isoformat()
            }
        }

        return report

    def get_optimization_recommendations(self, analysis: PerformanceAnalysis) -> List[str]:
        """Get optimization recommendations based on analysis."""
        return analysis.recommendations

    def track_query_performance(self, query: str, duration: float, run_id: str = None) -> None:
        """Track individual query performance."""
        if not self.db:
            self.logger.warning("No database connection - query performance not tracked")
            return

        try:
            sql = """
                INSERT INTO QUERY_PERFORMANCE (
                    query_id, run_id, query_text, execution_time_seconds, executed_at
                ) VALUES (%s, %s, %s, %s, %s)
            """

            query_id = str(uuid.uuid4())
            params = (
                query_id,
                run_id,
                query[:1000],  # Truncate long queries
                duration,
                datetime.utcnow()
            )

            self.db.begin()
            self.db.execute(sql, params)
            self.db.commit()

            self.logger.debug(f"Tracked query performance: {duration:.3f}s")

        except Exception as e:
            self.db.rollback()
            self.logger.exception(f"Failed to track query performance: {e}")

    def get_performance_trends(self, pipeline_id: str, days: int = 30) -> Dict[str, Any]:
        """Get performance trends for a pipeline over time."""
        if not self.db:
            raise ValueError("Database connection required for trend analysis")

        try:
            cutoff_date = datetime.utcnow() - timedelta(days=days)

            sql = """
                SELECT
                    DATE(pm.timestamp) as date,
                    AVG(pm.cpu_percent) as avg_cpu,
                    MAX(pm.cpu_percent) as peak_cpu,
                    AVG(pm.memory_percent) as avg_memory,
                    MAX(pm.memory_percent) as peak_memory,
                    SUM(pm.disk_read_mb) as total_disk_read,
                    SUM(pm.disk_write_mb) as total_disk_write,
                    COUNT(*) as metric_count
                FROM PERFORMANCE_METRICS pm
                JOIN ETL_EXECUTION ee ON pm.run_id = ee.run_id
                WHERE ee.pipeline_id = %s
                  AND pm.timestamp >= %s
                GROUP BY DATE(pm.timestamp)
                ORDER BY date
            """

            self.db.execute(sql, (pipeline_id, cutoff_date))
            rows = self.db.fetchall()

            trends = {
                "pipeline_id": pipeline_id,
                "days_analyzed": days,
                "daily_metrics": []
            }

            for row in rows:
                trends["daily_metrics"].append({
                    "date": row[0].isoformat() if row[0] else None,
                    "avg_cpu_percent": float(row[1] or 0),
                    "peak_cpu_percent": float(row[2] or 0),
                    "avg_memory_percent": float(row[3] or 0),
                    "peak_memory_percent": float(row[4] or 0),
                    "total_disk_read_mb": float(row[5] or 0),
                    "total_disk_write_mb": float(row[6] or 0),
                    "metric_count": int(row[7] or 0)
                })

            return trends

        except Exception as e:
            self.logger.exception(f"Failed to get performance trends: {e}")
            raise

    def get_dashboard_data(self, run_id: str = None) -> Dict[str, Any]:
        """Get real-time dashboard data."""
        current_metrics = self.get_current_metrics()

        dashboard_data = {
            "current_metrics": {
                "cpu_percent": current_metrics.cpu_percent,
                "memory_percent": current_metrics.memory_percent,
                "memory_used_mb": current_metrics.memory_used_mb,
                "memory_available_mb": current_metrics.memory_available_mb,
                "disk_read_mb": current_metrics.disk_read_mb,
                "disk_write_mb": current_metrics.disk_write_mb,
                "network_recv_mb": current_metrics.network_recv_mb,
                "network_sent_mb": current_metrics.network_sent_mb,
                "timestamp": current_metrics.timestamp.isoformat()
            },
            "active_runs": list(self.active_monitors.keys()),
            "alerts": []
        }

        # Check for resource alerts
        if current_metrics.cpu_percent > 90:
            dashboard_data["alerts"].append({
                "type": "warning",
                "message": f"High CPU usage: {current_metrics.cpu_percent:.1f}%",
                "timestamp": current_metrics.timestamp.isoformat()
            })

        if current_metrics.memory_percent > 90:
            dashboard_data["alerts"].append({
                "type": "critical",
                "message": f"High memory usage: {current_metrics.memory_percent:.1f}%",
                "timestamp": current_metrics.timestamp.isoformat()
            })

        # Add current run data if monitoring
        if run_id and run_id in self.monitoring_data:
            metrics_list = self.monitoring_data[run_id]
            if metrics_list:
                latest = metrics_list[-1]
                dashboard_data["current_run"] = {
                    "run_id": run_id,
                    "latest_cpu": latest.cpu_percent,
                    "latest_memory": latest.memory_percent,
                    "metrics_collected": len(metrics_list),
                    "duration_seconds": (latest.timestamp - metrics_list[0].timestamp).total_seconds()
                }

        return dashboard_data

    def set_baseline(self, run_id: str) -> None:
        """Set a run as baseline for future comparisons."""
        if run_id not in self.monitoring_data or not self.monitoring_data[run_id]:
            raise ValueError(f"No monitoring data available for run {run_id}")

        # Store baseline metrics (could be persisted to database)
        metrics_list = self.monitoring_data[run_id]
        avg_cpu = sum(m.cpu_percent for m in metrics_list) / len(metrics_list)
        avg_memory = sum(m.memory_percent for m in metrics_list) / len(metrics_list)

        self.baseline_metrics[run_id] = {
            "avg_cpu_percent": avg_cpu,
            "avg_memory_percent": avg_memory,
            "timestamp": datetime.utcnow()
        }

        self.logger.info(f"Set baseline for run {run_id}: CPU {avg_cpu:.1f}%, Memory {avg_memory:.1f}%")