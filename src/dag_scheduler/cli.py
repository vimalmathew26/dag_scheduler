import asyncio
import os
import sys

import click
from pathlib import Path
from typing import Dict, Optional
from typing import Optional
import httpx
import shutil

from .config import JOBS_DIR, API_HOST, API_PORT

DEFAULT_API_URL = os.environ.get(
    "DAG_SCHEDULER_API_URL", f"http://{API_HOST}:{API_PORT}"
)
API_BASE_URL = DEFAULT_API_URL


def _auth_headers() -> Dict[str, str]:
    token = os.environ.get("DAG_SCHEDULER_TOKEN")
    return {"Authorization": f"Bearer {token}"} if token else {}


@click.group()
@click.option('--api-url', default=None,
              help='Base URL of the daemon API. Defaults to '
                   'DAG_SCHEDULER_API_URL or http://127.0.0.1:8000.')
def cli(api_url: Optional[str]) -> None:
    """DAG Scheduler CLI"""
    global API_BASE_URL
    if api_url:
        API_BASE_URL = api_url.rstrip('/')

@cli.command()
@click.option('--jobs-dir', type=click.Path(), default=None,
              help='Directory to read job definitions from.')
def daemon(jobs_dir: Optional[str]) -> None:
    """Run the scheduler daemon in the foreground."""
    from .__main__ import main as run_daemon
    if jobs_dir:
        import os
        os.environ['DAG_SCHEDULER_JOBS_DIR'] = str(Path(jobs_dir).resolve())
    run_daemon()


@cli.command()
@click.argument('path', type=click.Path(exists=True))
def load(path: str) -> None:
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
def status() -> None:
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
def trigger(name: str) -> None:
    """Force-queue a job"""
    try:
        response = httpx.post(f"{API_BASE_URL}/jobs/{name}/trigger", headers=_auth_headers())
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
def logs(name: str) -> None:
    """Tail logs for most recent run of job"""
    try:
        # First get the runs to find the most recent run_id
        runs_response = httpx.get(f"{API_BASE_URL}/jobs/{name}/runs")
        runs_response.raise_for_status()
        runs = runs_response.json()

        if not runs:
            click.echo(f"No runs found for job '{name}'")
            return

        # /jobs/{name}/runs is ordered newest first, so the most recent run
        # is the first element.  This used to take runs[-1], which is the
        # oldest, so `logs` showed the first run rather than the latest.
        latest_run = runs[0]
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
def runs(name: str) -> None:
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
def stats() -> None:
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
def cancel(name: str) -> None:
    """Cancel a queued or running job"""
    try:
        response = httpx.post(f"{API_BASE_URL}/jobs/{name}/cancel", headers=_auth_headers())
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
def reset(name: str) -> None:
    """Reset a job in a terminal state back to defined"""
    try:
        response = httpx.post(f"{API_BASE_URL}/jobs/{name}/reset", headers=_auth_headers())
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
