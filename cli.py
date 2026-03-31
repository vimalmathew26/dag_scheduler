import click
import asyncio
import sys
from pathlib import Path
from typing import Optional
import httpx
import shutil

from .config import JOBS_DIR, API_HOST, API_PORT

API_BASE_URL = f"http://{API_HOST}:{API_PORT}"

@click.group()
def cli():
    """DAG Scheduler CLI"""
    pass

@cli.command()
@click.argument('path', type=click.Path(exists=True))
def load(path):
    """Load/reload a definition file"""
    try:
        src_path = Path(path)
        dest_path = JOBS_DIR / src_path.name
        
        # Copy the file to JOBS_DIR
        shutil.copy2(src_path, dest_path)
        click.echo(f"File copied to {dest_path} - daemon will hot-reload within 500ms.")
    except Exception as e:
        click.echo(f"Error loading file: {e}", err=True)
        sys.exit(1)

@cli.command()
def status():
    """Show all jobs and their states"""
    try:
        response = httpx.get(f"{API_BASE_URL}/jobs")
        response.raise_for_status()
        jobs = response.json()
        
        click.echo("Job Status:")
        for job in jobs:
            click.echo(f"  {job['name']:30s} {job['state']}")
    except httpx.RequestError as e:
        click.echo("Daemon not running (connection refused)", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error retrieving status: {e}", err=True)
        sys.exit(1)

@cli.command()
@click.argument('name')
def trigger(name):
    """Force-queue a job"""
    try:
        response = httpx.post(f"{API_BASE_URL}/jobs/{name}/trigger")
        response.raise_for_status()
        result = response.json()
        click.echo(result.get('message', 'Job triggered successfully'))
    except httpx.RequestError:
        click.echo("Daemon not running (connection refused)", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error triggering job: {e}", err=True)
        sys.exit(1)

@cli.command()
@click.argument('name')
def logs(name):
    """Tail logs for most recent run of job"""
    try:
        # First get the runs to find the most recent run_id
        runs_response = httpx.get(f"{API_BASE_URL}/jobs/{name}/runs")
        runs_response.raise_for_status()
        runs = runs_response.json()
        
        if not runs:
            click.echo(f"No runs found for job '{name}'")
            return
        
        # Get the most recent run (assuming it's the last one)
        latest_run = runs[-1]
        run_id = latest_run['run_id']
        
        # Now get the logs for that run
        logs_response = httpx.get(f"{API_BASE_URL}/jobs/{name}/runs/{run_id}/logs")
        logs_response.raise_for_status()
        logs_data = logs_response.json()
        
        # Sort logs by timestamp and print them
        all_logs = []
        for log_entry in logs_data:
            timestamp = log_entry['timestamp']
            stream = log_entry['stream']
            chunk = log_entry['chunk']
            # Split chunk into lines to handle multi-line chunks
            lines = chunk.splitlines()
            for line in lines:
                all_logs.append((timestamp, f"[{stream}] {line}"))
        
        # Sort by timestamp
        all_logs.sort(key=lambda x: x[0])
        
        # Print the sorted logs
        for _, log_line in all_logs:
            click.echo(log_line)
            
    except httpx.RequestError:
        click.echo("Daemon not running (connection refused)", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error retrieving logs: {e}", err=True)
        sys.exit(1)

@cli.command()
@click.argument('name')
def runs(name):
    """Show run history for a job"""
    try:
        response = httpx.get(f"{API_BASE_URL}/jobs/{name}/runs")
        response.raise_for_status()
        runs = response.json()
        
        click.echo(f"Run history for job: {name}")
        for run in runs:
            run_id_short = run['run_id'][:8]
            attempt = run.get('attempt', 'N/A')
            state = run.get('state', 'N/A')
            exit_code = run.get('exit_code', 'N/A')
            start_time = run.get('start_time', 'N/A')
            click.echo(f"  {run_id_short}  attempt={attempt}  state={state}  exit={exit_code}  start={start_time}")
    except httpx.RequestError:
        click.echo("Daemon not running (connection refused)", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error retrieving runs: {e}", err=True)
        sys.exit(1)

@cli.command()
def stats():
    """Show aggregate stats"""
    try:
        response = httpx.get(f"{API_BASE_URL}/stats")
        response.raise_for_status()
        stats_data = response.json()
        
        click.echo("Scheduler Statistics:")
        click.echo(f"  Total Runs: {stats_data.get('total_runs', 'N/A')}")
        click.echo(f"  Pass Rate: {stats_data.get('pass_rate', 'N/A'):.2%}")
        click.echo(f"  Average Duration: {stats_data.get('avg_duration_seconds', 'N/A'):.2f} seconds")
        click.echo("  Jobs by State:")
        for state, count in stats_data.get('jobs_by_state', {}).items():
            click.echo(f"    {state}: {count}")
    except httpx.RequestError:
        click.echo("Daemon not running (connection refused)", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error retrieving stats: {e}", err=True)
        sys.exit(1)

@cli.command()
@click.argument('name')
def cancel(name):
    """Cancel a queued or running job"""
    try:
        response = httpx.post(f"{API_BASE_URL}/jobs/{name}/cancel")
        response.raise_for_status()
        result = response.json()
        click.echo(result.get('message', 'Job cancellation requested'))
    except httpx.RequestError:
        click.echo("Daemon not running (connection refused)", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error cancelling job: {e}", err=True)
        sys.exit(1)

@cli.command()
@click.argument('name')
def reset(name):
    """Reset a job in a terminal state back to defined"""
    try:
        response = httpx.post(f"{API_BASE_URL}/jobs/{name}/reset")
        response.raise_for_status()
        result = response.json()
        click.echo(result.get('message', 'Job reset successfully'))
    except httpx.HTTPStatusError as e:
        detail = e.response.json().get('detail', str(e))
        click.echo(f"Error: {detail}", err=True)
        sys.exit(1)
    except httpx.RequestError:
        click.echo("Daemon not running (connection refused)", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error resetting job: {e}", err=True)
        sys.exit(1)

if __name__ == '__main__':
    cli()
