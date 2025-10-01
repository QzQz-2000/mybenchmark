"""Worker CLI for running benchmark workers."""

import asyncio
from pathlib import Path
import click
from rich.console import Console

from benchmark.api.worker_api import run_worker_server
from benchmark.core.config import ConfigLoader
from benchmark.utils.logging import setup_logging
from .kafka_worker import KafkaWorker

console = Console()


@click.group()
@click.option('--log-level', default='INFO', help='Logging level')
@click.option('--log-file', help='Log file path')
@click.pass_context
def cli(ctx, log_level, log_file):
    """Python OpenMessaging Benchmark Worker CLI."""
    ctx.ensure_object(dict)
    ctx.obj['log_level'] = log_level
    ctx.obj['log_file'] = log_file

    # Setup logging
    setup_logging(level=log_level, log_file=log_file, component="worker")


@cli.command()
@click.option('--worker-id', '-i', required=True, help='Unique worker identifier')
@click.option('--driver-config', '-d', required=True, help='Driver configuration file')
@click.option('--client-type', '-c', default='kafka-python',
              type=click.Choice(['kafka-python', 'confluent-kafka', 'aiokafka']),
              help='Kafka client type to use')
@click.option('--host', default='0.0.0.0', help='Worker host address')
@click.option('--port', default=8080, help='Worker port')
def kafka(worker_id, driver_config, client_type, host, port):
    """Start a Kafka worker."""
    asyncio.run(_start_kafka_worker(
        worker_id=worker_id,
        driver_config=driver_config,
        client_type=client_type,
        host=host,
        port=port
    ))


async def _start_kafka_worker(
    worker_id: str,
    driver_config: str,
    client_type: str,
    host: str,
    port: int
):
    """Start Kafka worker implementation."""
    try:
        console.print(f"[bold blue]Starting Kafka worker...[/bold blue]")

        # Load driver configuration
        config = ConfigLoader.load_driver(driver_config)
        console.print(f"[green]✓[/green] Loaded driver config: {config.name}")

        # Create Kafka worker
        worker = KafkaWorker(
            worker_id=worker_id,
            driver_config=config,
            client_type=client_type
        )

        console.print(f"[green]✓[/green] Created Kafka worker: {worker_id}")
        console.print(f"[green]✓[/green] Client type: {client_type}")
        console.print(f"[green]✓[/green] Listening on: {host}:{port}")

        # Start worker server
        await run_worker_server(worker, host, port)

    except KeyboardInterrupt:
        console.print(f"\n[yellow]Received interrupt signal, shutting down...[/yellow]")
    except Exception as e:
        console.print(f"[red]✗ Worker failed: {e}[/red]")
        raise


@cli.command()
@click.option('--worker-urls', '-w', multiple=True, required=True, help='Worker URLs to check')
def health_check(worker_urls):
    """Check health of workers."""
    asyncio.run(_check_workers_health(list(worker_urls)))


async def _check_workers_health(worker_urls):
    """Check workers health implementation."""
    import aiohttp
    from rich.table import Table

    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Worker URL", style="cyan")
    table.add_column("Status", style="white")
    table.add_column("Worker ID", style="green")
    table.add_column("Running Tasks", style="yellow")

    async with aiohttp.ClientSession() as session:
        for url in worker_urls:
            try:
                health_url = f"{url}/health"
                async with session.get(health_url, timeout=aiohttp.ClientTimeout(total=5)) as response:
                    if response.status == 200:
                        health_data = await response.json()
                        table.add_row(
                            url,
                            "[green]✓ Healthy[/green]",
                            health_data.get('worker_id', 'unknown'),
                            str(health_data.get('running_tasks', 0))
                        )
                    else:
                        table.add_row(url, f"[red]✗ HTTP {response.status}[/red]", "N/A", "N/A")

            except asyncio.TimeoutError:
                table.add_row(url, "[red]✗ Timeout[/red]", "N/A", "N/A")
            except Exception as e:
                table.add_row(url, f"[red]✗ {str(e)}[/red]", "N/A", "N/A")

    console.print(table)


@cli.command()
@click.option('--worker-url', '-w', required=True, help='Worker URL')
def stats(worker_url):
    """Get system statistics from worker."""
    asyncio.run(_get_worker_stats(worker_url))


async def _get_worker_stats(worker_url):
    """Get worker statistics implementation."""
    import aiohttp
    from rich.table import Table

    try:
        async with aiohttp.ClientSession() as session:
            stats_url = f"{worker_url}/system/stats"
            async with session.get(stats_url, timeout=aiohttp.ClientTimeout(total=5)) as response:
                response.raise_for_status()
                stats_data = await response.json()

                # Display CPU stats
                if 'cpu' in stats_data:
                    cpu_table = Table(title="CPU Statistics", show_header=True, header_style="bold magenta")
                    cpu_table.add_column("Metric", style="cyan")
                    cpu_table.add_column("Value", style="green")

                    cpu = stats_data['cpu']
                    cpu_table.add_row("Average", f"{cpu.get('avg', 0):.1f}%")
                    cpu_table.add_row("Maximum", f"{cpu.get('max', 0):.1f}%")
                    cpu_table.add_row("P95", f"{cpu.get('p95', 0):.1f}%")

                    console.print(cpu_table)

                # Display Memory stats
                if 'memory' in stats_data:
                    mem_table = Table(title="Memory Statistics", show_header=True, header_style="bold magenta")
                    mem_table.add_column("Metric", style="cyan")
                    mem_table.add_column("Value", style="green")

                    memory = stats_data['memory']
                    mem_table.add_row("Average", f"{memory.get('avg', 0):.1f}%")
                    mem_table.add_row("Maximum", f"{memory.get('max', 0):.1f}%")

                    console.print(mem_table)

                # Display Network stats
                if 'network' in stats_data:
                    net_table = Table(title="Network Statistics", show_header=True, header_style="bold magenta")
                    net_table.add_column("Metric", style="cyan")
                    net_table.add_column("Value", style="green")

                    network = stats_data['network']
                    net_table.add_row("Avg Sent", f"{network.get('avg_sent_mbps', 0):.2f} MB/s")
                    net_table.add_row("Avg Received", f"{network.get('avg_recv_mbps', 0):.2f} MB/s")
                    net_table.add_row("Total Sent", f"{network.get('total_sent_mb', 0):.2f} MB")
                    net_table.add_row("Total Received", f"{network.get('total_recv_mb', 0):.2f} MB")

                    console.print(net_table)

    except Exception as e:
        console.print(f"[red]✗ Failed to get worker stats: {e}[/red]")


def worker_main():
    """Entry point for worker CLI."""
    cli()


if __name__ == '__main__':
    worker_main()