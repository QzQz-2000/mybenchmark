"""System monitoring utilities adapted from my-benchmark project."""

import time
import psutil
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from threading import Thread, Event
import logging

logger = logging.getLogger(__name__)


@dataclass
class SystemMetrics:
    """System metrics data point."""
    timestamp: float
    cpu_percent: float
    memory_percent: float
    network_sent_bytes: int
    network_recv_bytes: int
    disk_read_bytes: int
    disk_write_bytes: int


@dataclass
class SystemStats:
    """Aggregated system statistics."""
    cpu: Dict[str, float] = field(default_factory=dict)
    memory: Dict[str, float] = field(default_factory=dict)
    network: Dict[str, float] = field(default_factory=dict)
    disk: Dict[str, float] = field(default_factory=dict)


class SystemMonitor:
    """Enhanced system resource monitor based on my-benchmark implementation."""

    def __init__(self, interval: float = 1.0):
        """Initialize system monitor.

        Args:
            interval: Monitoring interval in seconds
        """
        self.interval = interval
        self.metrics: List[SystemMetrics] = []
        self._running = False
        self._stop_event = Event()
        self._monitor_thread: Optional[Thread] = None
        self._initial_net_io = None
        self._initial_disk_io = None

    def start(self) -> None:
        """Start system monitoring."""
        if self._running:
            logger.warning("System monitor is already running")
            return

        self._running = True
        self._stop_event.clear()
        self.metrics.clear()

        # Get initial I/O counters
        self._initial_net_io = psutil.net_io_counters()
        self._initial_disk_io = psutil.disk_io_counters()

        self._monitor_thread = Thread(target=self._monitor_loop, daemon=True)
        self._monitor_thread.start()
        logger.info("System monitoring started")

    def stop(self) -> None:
        """Stop system monitoring."""
        if not self._running:
            return

        self._running = False
        self._stop_event.set()

        if self._monitor_thread and self._monitor_thread.is_alive():
            self._monitor_thread.join(timeout=5.0)

        logger.info(f"System monitoring stopped. Collected {len(self.metrics)} samples")

    def _monitor_loop(self) -> None:
        """Main monitoring loop."""
        start_time = time.time()
        prev_net = self._initial_net_io
        prev_disk = self._initial_disk_io

        while not self._stop_event.wait(self.interval):
            try:
                current_time = time.time()

                # CPU and Memory
                cpu_percent = psutil.cpu_percent(interval=None)
                memory = psutil.virtual_memory()

                # Network I/O
                net_io = psutil.net_io_counters()
                net_sent = net_io.bytes_sent - (prev_net.bytes_sent if prev_net else 0)
                net_recv = net_io.bytes_recv - (prev_net.bytes_recv if prev_net else 0)
                prev_net = net_io

                # Disk I/O
                disk_io = psutil.disk_io_counters()
                if disk_io and prev_disk:
                    disk_read = disk_io.read_bytes - prev_disk.read_bytes
                    disk_write = disk_io.write_bytes - prev_disk.write_bytes
                    prev_disk = disk_io
                else:
                    disk_read = disk_write = 0

                # Store metrics
                metric = SystemMetrics(
                    timestamp=current_time - start_time,
                    cpu_percent=cpu_percent,
                    memory_percent=memory.percent,
                    network_sent_bytes=net_sent,
                    network_recv_bytes=net_recv,
                    disk_read_bytes=disk_read,
                    disk_write_bytes=disk_write
                )
                self.metrics.append(metric)

            except Exception as e:
                logger.error(f"Error collecting system metrics: {e}")

    def get_stats(self) -> SystemStats:
        """Get aggregated system statistics."""
        if not self.metrics:
            return SystemStats()

        # Extract values
        cpu_values = [m.cpu_percent for m in self.metrics]
        memory_values = [m.memory_percent for m in self.metrics]
        net_sent_values = [m.network_sent_bytes / (1024 * 1024) for m in self.metrics]  # MB
        net_recv_values = [m.network_recv_bytes / (1024 * 1024) for m in self.metrics]  # MB
        disk_read_values = [m.disk_read_bytes / (1024 * 1024) for m in self.metrics]  # MB
        disk_write_values = [m.disk_write_bytes / (1024 * 1024) for m in self.metrics]  # MB

        return SystemStats(
            cpu={
                'avg': np.mean(cpu_values),
                'max': np.max(cpu_values),
                'min': np.min(cpu_values),
                'p95': np.percentile(cpu_values, 95) if len(cpu_values) > 0 else 0,
                'p99': np.percentile(cpu_values, 99) if len(cpu_values) > 0 else 0,
            },
            memory={
                'avg': np.mean(memory_values),
                'max': np.max(memory_values),
                'min': np.min(memory_values),
            },
            network={
                'avg_sent_mbps': np.mean(net_sent_values) if net_sent_values else 0,
                'avg_recv_mbps': np.mean(net_recv_values) if net_recv_values else 0,
                'max_sent_mbps': np.max(net_sent_values) if net_sent_values else 0,
                'max_recv_mbps': np.max(net_recv_values) if net_recv_values else 0,
                'total_sent_mb': sum(net_sent_values),
                'total_recv_mb': sum(net_recv_values),
            },
            disk={
                'avg_read_mbps': np.mean(disk_read_values) if disk_read_values else 0,
                'avg_write_mbps': np.mean(disk_write_values) if disk_write_values else 0,
                'max_read_mbps': np.max(disk_read_values) if disk_read_values else 0,
                'max_write_mbps': np.max(disk_write_values) if disk_write_values else 0,
                'total_read_mb': sum(disk_read_values),
                'total_write_mb': sum(disk_write_values),
            }
        )

    def plot_metrics(self, save_path: str = 'system_metrics.png') -> str:
        """Plot system metrics."""
        if not self.metrics:
            logger.warning("No metrics data available for plotting")
            return save_path

        # Setup plot style
        plt.style.use('seaborn-v0_8')
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 10))

        timestamps = [m.timestamp for m in self.metrics]
        cpu_values = [m.cpu_percent for m in self.metrics]
        memory_values = [m.memory_percent for m in self.metrics]
        net_sent_values = [m.network_sent_bytes / (1024 * 1024) for m in self.metrics]
        net_recv_values = [m.network_recv_bytes / (1024 * 1024) for m in self.metrics]
        disk_read_values = [m.disk_read_bytes / (1024 * 1024) for m in self.metrics]
        disk_write_values = [m.disk_write_bytes / (1024 * 1024) for m in self.metrics]

        # CPU Usage
        ax1.plot(timestamps, cpu_values, 'b-', linewidth=2, label='CPU')
        ax1.set_ylabel('CPU Usage (%)')
        ax1.set_title('CPU Usage Over Time')
        ax1.grid(True, alpha=0.3)
        ax1.set_ylim(0, 100)

        # Memory Usage
        ax2.plot(timestamps, memory_values, 'r-', linewidth=2, label='Memory')
        ax2.set_ylabel('Memory Usage (%)')
        ax2.set_title('Memory Usage Over Time')
        ax2.grid(True, alpha=0.3)
        ax2.set_ylim(0, 100)

        # Network I/O
        ax3.plot(timestamps, net_sent_values, 'g-', label='Sent', linewidth=2)
        ax3.plot(timestamps, net_recv_values, 'orange', label='Received', linewidth=2)
        ax3.set_ylabel('Network Rate (MB/s)')
        ax3.set_title('Network I/O Rate')
        ax3.legend()
        ax3.grid(True, alpha=0.3)

        # Disk I/O
        ax4.plot(timestamps, disk_read_values, 'purple', label='Read', linewidth=2)
        ax4.plot(timestamps, disk_write_values, 'brown', label='Write', linewidth=2)
        ax4.set_ylabel('Disk Rate (MB/s)')
        ax4.set_xlabel('Time (seconds)')
        ax4.set_title('Disk I/O Rate')
        ax4.legend()
        ax4.grid(True, alpha=0.3)

        plt.tight_layout()
        plt.savefig(save_path, dpi=150, bbox_inches='tight')
        plt.close()

        logger.info(f"System metrics plot saved to: {save_path}")
        return save_path

    def export_raw_data(self, file_path: str = 'system_metrics.csv') -> str:
        """Export raw metrics data to CSV."""
        if not self.metrics:
            logger.warning("No metrics data available for export")
            return file_path

        import pandas as pd

        df = pd.DataFrame([
            {
                'timestamp': m.timestamp,
                'cpu_percent': m.cpu_percent,
                'memory_percent': m.memory_percent,
                'network_sent_mbps': m.network_sent_bytes / (1024 * 1024),
                'network_recv_mbps': m.network_recv_bytes / (1024 * 1024),
                'disk_read_mbps': m.disk_read_bytes / (1024 * 1024),
                'disk_write_mbps': m.disk_write_bytes / (1024 * 1024),
            }
            for m in self.metrics
        ])

        df.to_csv(file_path, index=False)
        logger.info(f"Raw metrics data exported to: {file_path}")
        return file_path

    def __enter__(self):
        """Context manager entry."""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.stop()