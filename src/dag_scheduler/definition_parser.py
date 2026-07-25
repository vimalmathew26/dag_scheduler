import yaml
import tomllib
import logging
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple
from .models import JobDefinition, DefinitionFile, RetryPolicy
from .dag import topological_sort, CycleError
from .config import DEFAULT_TIMEOUT, DEFAULT_RETRY, DEFAULT_BACKOFF_BASE, DEFAULT_JITTER, DEFAULT_RETRY_ON_EXIT_CODES

logger = logging.getLogger(__name__)

class ParseError(Exception):
    """Raised when a job definition file fails validation."""
    def __init__(self, message: str, file_path: Path = None, line: int = None):
        self.file_path = file_path
        self.line = line
        prefix = f"[{file_path}:{line}] " if file_path and line else f"[{file_path}] " if file_path else ""
        super().__init__(f"{prefix}{message}")

def load_file(path: Path) -> Dict[str, Any]:
    """Load a single YAML or TOML file."""
    try:
        with open(path, "rb") as f:
            if path.suffix == ".yaml" or path.suffix == ".yml":
                return yaml.safe_load(f) or {}
            elif path.suffix == ".toml":
                return tomllib.load(f)
            else:
                return {}
    except Exception as e:
        raise ParseError(f"Failed to read file: {e}", file_path=path)

class DefinitionParser:
    def __init__(self):
        self.all_jobs: Dict[str, JobDefinition] = {}
        self.job_to_file: Dict[str, Path] = {}
        self.parsed_files: List[Path] = []
        self.rejected: Dict[str, str] = {}

    def parse_directory(self, directory: Path) -> Dict[str, JobDefinition]:
        """
        Parses all .yaml and .toml files in a directory.
        Returns a flat namespace of JobDefinitions.
        Files with errors (bad syntax, duplicate names, cycles) are skipped
        with a warning so that valid jobs from other files still load.
        """
        # Sorted so the result never depends on filesystem enumeration
        # order.  It previously did, and decided which of two conflicting
        # files survived.
        files = sorted(
            list(directory.glob("*.yaml"))
            + list(directory.glob("*.yml"))
            + list(directory.glob("*.toml"))
        )
        self.parsed_files = list(files)

        # Pass 1: read every file and collect each declaration with the file
        # it came from, so a name declared more than once can be recognised
        # as a conflict rather than a race between files.
        raw_definitions: Dict[str, Dict[str, Any]] = {}
        declarations: Dict[str, List[Tuple[Path, Dict[str, Any]]]] = {}

        for path in files:
            try:
                data = load_file(path)
            except ParseError as e:
                logger.warning(f"Skipping file {path}: {e}")
                continue

            if "jobs" not in data:
                continue

            file_valid = True
            file_jobs: Dict[str, Dict[str, Any]] = {}
            for job_name, job_dict in data["jobs"].items():
                if not isinstance(job_dict, dict):
                    logger.warning(f"Skipping file {path}: job '{job_name}' definition must be a dictionary")
                    file_valid = False
                    break

                if "command" not in job_dict:
                    logger.warning(f"Skipping file {path}: job '{job_name}' missing required 'command' field")
                    file_valid = False
                    break

                file_jobs[job_name] = job_dict

            if not file_valid:
                continue

            for name, jdict in file_jobs.items():
                declarations.setdefault(name, []).append((path, jdict))

        # A name declared in more than one file is rejected on every side.
        # Keeping one of them would need an arbitrary precedence rule, and
        # the old rule was "whichever file the filesystem listed first",
        # which silently substituted the wrong command.  Rejecting both is
        # deterministic, and it keeps the blast radius equal to the scope of
        # the error: a name collision is an error about that name, not about
        # the files that happen to contain it.  This also makes pass 1
        # consistent with pass 3, which drops cycle participants per job
        # rather than discarding whole files.
        for name, declared in declarations.items():
            if len(declared) > 1:
                paths = ", ".join(str(path) for path, _ in declared)
                logger.warning(
                    f"Rejecting all {len(declared)} definitions of job '{name}': "
                    f"declared in multiple files ({paths}). Remove the duplicate "
                    f"and the job will load."
                )
                self.rejected[name] = (
                    f"declared in multiple files ({paths})"
                )
                continue

            path, jdict = declared[0]
            raw_definitions[name] = jdict
            self.job_to_file[name] = path

        # Pass 2: Validate dependency references and construct JobDefinition objects.
        # Jobs with invalid deps are removed, and removal cascades to their dependents.
        to_remove: Set[str] = set()
        for job_name, job_dict in raw_definitions.items():
            deps = job_dict.get("depends_on", [])
            if not isinstance(deps, list):
                logger.warning(f"Skipping job '{job_name}': 'depends_on' must be a list")
                to_remove.add(job_name)
                continue

            for dep in deps:
                if dep not in raw_definitions:
                    logger.warning(
                        f"Skipping job '{job_name}': depends on non-existent job '{dep}'"
                    )
                    to_remove.add(job_name)
                    break

        # Cascade: removing a job may invalidate other jobs' dependencies
        changed = True
        while changed:
            changed = False
            for job_name, job_dict in raw_definitions.items():
                if job_name in to_remove:
                    continue
                for dep in job_dict.get("depends_on", []):
                    if dep in to_remove:
                        logger.warning(f"Skipping job '{job_name}': dependency '{dep}' was removed")
                        to_remove.add(job_name)
                        changed = True
                        break

        for name in to_remove:
            raw_definitions.pop(name, None)
            self.job_to_file.pop(name, None)

        # Build JobDefinition objects for remaining valid jobs
        for job_name, job_dict in raw_definitions.items():
            deps = job_dict.get("depends_on", [])
            retry_dict = job_dict.get("retry", {})
            retry_policy = RetryPolicy(
                max_attempts=retry_dict.get("max_attempts", DEFAULT_RETRY),
                backoff_base=retry_dict.get("backoff_base", DEFAULT_BACKOFF_BASE),
                jitter=retry_dict.get("jitter", DEFAULT_JITTER),
                retry_on_exit_codes=retry_dict.get("retry_on_exit_codes", DEFAULT_RETRY_ON_EXIT_CODES)
            )

            self.all_jobs[job_name] = JobDefinition(
                command=job_dict["command"],
                depends_on=deps,
                tags=job_dict.get("tags", []),
                priority=job_dict.get("priority", 1),
                timeout=job_dict.get("timeout", DEFAULT_TIMEOUT),
                retry=retry_policy
            )

        # Pass 3: Cycle detection — iteratively remove cyclic jobs instead of
        # failing the entire load.  After removing cycle participants, cascade
        # removal of any jobs whose dependencies became unresolvable.
        while self.all_jobs:
            try:
                topological_sort(self.all_jobs)
                break  # no cycles
            except CycleError as e:
                cycle_jobs = set(e.cycle)
                if not cycle_jobs:
                    break  # safety valve
                for name in cycle_jobs:
                    if name in self.all_jobs:
                        logger.warning(
                            f"Removing job '{name}' ({self.job_to_file.get(name)}): "
                            f"part of dependency cycle {e.cycle}"
                        )
                        self.all_jobs.pop(name, None)
                        self.job_to_file.pop(name, None)
                # Cascade removal of dependents whose deps are now missing
                changed = True
                while changed:
                    changed = False
                    for name, defn in list(self.all_jobs.items()):
                        for dep in defn.depends_on:
                            if dep not in self.all_jobs:
                                logger.warning(
                                    f"Removing job '{name}': dependency '{dep}' was removed due to cycle"
                                )
                                self.all_jobs.pop(name, None)
                                self.job_to_file.pop(name, None)
                                changed = True
                                break

        return self.all_jobs
