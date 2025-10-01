"""Command line interface for the benchmark framework."""

import asyncio
import sys
from pathlib import Path
from typing import List, Optional
import click
from rich.console import Console
from rich.table import Table

from .core.config import ConfigLoader, BenchmarkConfig
from .core.coordinator import BenchmarkCoordinator
from .utils.logging import setup_logging


console = Console()


@click.group()
@click.option('--log-level', default='INFO', help='Logging level')
@click.option('--log-file', help='Log file path')
@click.pass_context
def cli(ctx, log_level, log_file):
    """Python OpenMessaging Benchmark CLI."""
    ctx.ensure_object(dict)
    ctx.obj['log_level'] = log_level
    ctx.obj['log_file'] = log_file

    # Setup logging
    setup_logging(level=log_level, log_file=log_file, component="cli")


@cli.command()
@click.option('--workload', '-w', required=True, help='Workload configuration file')
@click.option('--driver', '-d', required=True, help='Driver configuration file')
@click.option('--workers', '-W', multiple=True, help='Worker URLs')
@click.option('--client-type', '-c', default='kafka-python',
              type=click.Choice(['kafka-python', 'confluent-kafka', 'aiokafka']),
              help='Kafka client type to use')
@click.option('--output-dir', '-o', default='results', help='Output directory for results')
@click.option('--results-prefix', default='benchmark', help='Results file prefix')
@click.option('--enable-monitoring/--disable-monitoring', default=True, help='Enable system monitoring')
@click.pass_context
def run(ctx, workload, driver, workers, client_type, output_dir, results_prefix, enable_monitoring):
    """Run a benchmark test."""
    asyncio.run(_run_benchmark(
        workload=workload,
        driver=driver,
        workers=list(workers),
        client_type=client_type,
        output_dir=output_dir,
        results_prefix=results_prefix,
        enable_monitoring=enable_monitoring
    ))


async def _run_benchmark(
    workload: str,
    driver: str,
    workers: List[str],
    client_type: str,
    output_dir: str,
    results_prefix: str,
    enable_monitoring: bool
):
    """Run benchmark implementation."""
    try:
        # Load configurations
        console.print(f"[bold blue]Loading configurations...[/bold blue]")

        workload_config = ConfigLoader.load_workload(workload)
        driver_config = ConfigLoader.load_driver(driver)

        # Create benchmark config
        benchmark_config = BenchmarkConfig(
            workers=workers,
            results_dir=output_dir,
            results_file_prefix=results_prefix,
            enable_monitoring=enable_monitoring
        )

        console.print(f"[green]✓[/green] Workload: {workload_config.name}")
        console.print(f"[green]✓[/green] Driver: {driver_config.name}")
        console.print(f"[green]✓[/green] Client: {client_type}")
        console.print(f"[green]✓[/green] Workers: {len(workers)}")

        # Create coordinator and run benchmark
        console.print(f"\n[bold blue]Starting benchmark...[/bold blue]")

        async with BenchmarkCoordinator(benchmark_config) as coordinator:
            result = await coordinator.run_benchmark(
                workload_config=workload_config,
                driver_config=driver_config
            )

            # Save results
            output_path = Path(output_dir)
            output_path.mkdir(parents=True, exist_ok=True)

            results_file = output_path / f"{results_prefix}_{result.test_id}.json"
            coordinator.result_collector.save_result(result, results_file)

            csv_file = output_path / f"{results_prefix}_{result.test_id}.csv"
            coordinator.result_collector.export_csv(result, csv_file)

            # Display results summary
            _display_results_summary(result)

            console.print(f"\n[green]✓ Benchmark completed successfully![/green]")
            console.print(f"Results saved to: {results_file}")

    except Exception as e:
        console.print(f"[red]✗ Benchmark failed: {e}[/red]")
        sys.exit(1)


def _display_results_summary(result):
    """Display benchmark results summary."""
    console.print(f"\n[bold]Benchmark Results Summary[/bold]")

    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green")

    # Basic info
    table.add_row("Test Name", result.test_name)
    table.add_row("Duration", f"{result.duration_seconds:.1f}s")
    table.add_row("Producers", str(result.total_producers))
    table.add_row("Consumers", str(result.total_consumers))

    # Producer stats
    if result.producer_stats:
        table.add_row("Messages Sent", f"{result.producer_stats.total_messages:,}")
        table.add_row("Producer Throughput", f"{result.producer_stats.messages_per_second:.0f} msg/s")
        table.add_row("Producer Bandwidth", f"{result.producer_stats.mb_per_second:.2f} MB/s")

    # Consumer stats
    if result.consumer_stats:
        table.add_row("Messages Consumed", f"{result.consumer_stats.total_messages:,}")
        table.add_row("Consumer Throughput", f"{result.consumer_stats.messages_per_second:.0f} msg/s")
        table.add_row("Consumer Bandwidth", f"{result.consumer_stats.mb_per_second:.2f} MB/s")

    # Latency stats
    if result.latency_stats:
        table.add_row("Avg Latency", f"{result.latency_stats.mean_ms:.2f}ms")
        table.add_row("P95 Latency", f"{result.latency_stats.p95_ms:.2f}ms")
        table.add_row("P99 Latency", f"{result.latency_stats.p99_ms:.2f}ms")

    # System stats
    if result.system_stats:
        table.add_row("Avg CPU", f"{result.system_stats.cpu.get('avg', 0):.1f}%")
        table.add_row("Max CPU", f"{result.system_stats.cpu.get('max', 0):.1f}%")
        table.add_row("Avg Memory", f"{result.system_stats.memory.get('avg', 0):.1f}%")

    console.print(table)


@cli.command()
@click.option('--workload', '-w', help='Workload configuration file to validate')
@click.option('--driver', '-d', help='Driver configuration file to validate')
def validate(workload, driver):
    """Validate configuration files."""
    try:
        if workload:
            console.print(f"[blue]Validating workload: {workload}[/blue]")
            config = ConfigLoader.load_workload(workload)
            console.print(f"[green]✓ Valid workload: {config.name}[/green]")

            # Display config summary
            table = Table(show_header=True, header_style="bold magenta")
            table.add_column("Property", style="cyan")
            table.add_column("Value", style="green")

            table.add_row("Topics", str(config.topics))
            table.add_row("Partitions per Topic", str(config.partitions_per_topic))
            table.add_row("Message Size", f"{config.message_size} bytes")
            table.add_row("Producers per Topic", str(config.producers_per_topic))
            table.add_row("Producer Rate", f"{config.producer_rate} msg/s")
            table.add_row("Consumers per Subscription", str(config.consumer_per_subscription))
            table.add_row("Test Duration", f"{config.test_duration_minutes} minutes")

            console.print(table)

        if driver:
            console.print(f"\n[blue]Validating driver: {driver}[/blue]")
            config = ConfigLoader.load_driver(driver)
            console.print(f"[green]✓ Valid driver: {config.name}[/green]")

            # Display config summary
            table = Table(show_header=True, header_style="bold magenta")
            table.add_column("Section", style="cyan")
            table.add_column("Properties", style="green")

            table.add_row("Common", str(len(config.common_config)))
            table.add_row("Producer", str(len(config.producer_config)))
            table.add_row("Consumer", str(len(config.consumer_config)))
            table.add_row("Topic", str(len(config.topic_config)))

            console.print(table)

    except Exception as e:
        console.print(f"[red]✗ Validation failed: {e}[/red]")
        sys.exit(1)


@cli.command()
@click.option('--client-type', '-c',
              type=click.Choice(['kafka-python', 'confluent-kafka', 'aiokafka']),
              help='Check specific client availability')
def check_clients(client_type):
    """Check availability of Kafka Python clients."""
    clients = {
        'kafka-python': 'kafka',
        'confluent-kafka': 'confluent_kafka',
        'aiokafka': 'aiokafka'
    }

    if client_type:
        clients = {client_type: clients[client_type]}

    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Client", style="cyan")
    table.add_column("Status", style="white")
    table.add_column("Version", style="green")

    for name, module in clients.items():
        try:
            imported = __import__(module)
            version = getattr(imported, '__version__', 'unknown')
            table.add_row(name, "[green]✓ Available[/green]", version)
        except ImportError:
            table.add_row(name, "[red]✗ Not available[/red]", "N/A")

    console.print(table)


def coordinator_main():
    """Entry point for coordinator CLI."""
    cli()


if __name__ == '__main__':
    coordinator_main()