from typing import List, Set, Dict, Tuple
from collections import deque
from .models import JobDefinition, JobState

class CycleError(Exception):
    """Raised when a cycle is detected in the dependency graph."""
    def __init__(self, cycle: List[str]):
        self.cycle = cycle
        super().__init__(f"Cycle detected: {' -> '.join(cycle)}")


def topological_sort(jobs: Dict[str, JobDefinition]) -> List[str]:
    """
    Perform topological sort using Kahn's algorithm.
    
    Args:
        jobs: A dictionary mapping job names to JobDefinition objects
        
    Returns:
        List of job names in topological order
        
    Raises:
        CycleError: If a cycle is detected in the dependency graph
    """
    # Build adjacency list and in-degree count
    adj: Dict[str, Set[str]] = {name: set() for name in jobs}
    in_degree: Dict[str, int] = {name: 0 for name in jobs}
    
    # Populate adjacency list and in-degrees
    for name, job in jobs.items():
        for dep in job.depends_on:
            if dep in jobs:  # Only consider dependencies that exist in the current snapshot
                adj[dep].add(name)
                in_degree[name] += 1
    
    # Initialize queue with nodes having zero in-degree
    queue: deque = deque([name for name, degree in in_degree.items() if degree == 0])
    result: List[str] = []
    visited: Set[str] = set()
    
    while queue:
        current = queue.popleft()
        result.append(current)
        visited.add(current)
        
        # Reduce in-degree of neighbors
        for neighbor in adj[current]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)
    
    # Check for cycles
    if len(result) != len(jobs):
        # Find the cycle
        unvisited = set(jobs.keys()) - visited
        if unvisited:
            # Start DFS from an unvisited node to find the cycle
            cycle = _find_cycle(jobs, unvisited.pop())
            raise CycleError(cycle)
    
    return result


def _find_cycle(jobs: Dict[str, JobDefinition], start_node: str) -> List[str]:
    """Helper function to find a cycle using DFS."""
    visited: Set[str] = set()
    rec_stack: Set[str] = set()
    path: List[str] = []
    
    def dfs(node: str) -> List[str] | None:
        visited.add(node)
        rec_stack.add(node)
        path.append(node)
        
        for dep in jobs[node].depends_on:
            if dep not in jobs:
                continue  # Skip external dependencies not in current snapshot
            if dep not in visited:
                cycle = dfs(dep)
                if cycle:
                    return cycle
            elif dep in rec_stack:
                # Found a cycle
                cycle_start = path.index(dep)
                return path[cycle_start:] + [dep]
        
        rec_stack.remove(node)
        path.pop()
        return None
    
    return dfs(start_node) or []


def get_dependents(job_name: str, jobs: Dict[str, JobDefinition]) -> Set[str]:
    """
    Get all jobs that depend on the given job.
    
    Args:
        job_name: The name of the job to find dependents for
        jobs: A dictionary mapping job names to JobDefinition objects
        
    Returns:
        Set of job names that depend on the given job
    """
    dependents = set()
    for name, job in jobs.items():
        if job_name in job.depends_on:
            dependents.add(name)
    return dependents


def get_ready_jobs(completed_job: str, jobs: Dict[str, JobDefinition], 
                   current_states: Dict[str, JobState]) -> List[str]:
    """
    Get jobs that are now ready to run because their dependencies are satisfied.
    
    Args:
        completed_job: The name of the job that just completed
        jobs: A dictionary mapping job names to JobDefinition objects
        current_states: A dictionary mapping job names to their current JobState
        
    Returns:
        List of job names that are now ready to run

    A job is ready only when every name in its depends_on is present in
    `jobs` and is DONE.  A dependency that is absent from the snapshot
    blocks its dependent: Persistence.revalidate_jobs takes the same view
    and moves such a job to BLOCKED_UNRESOLVABLE.
    """
    # Get direct dependents of the completed job
    dependents = get_dependents(completed_job, jobs)

    ready_jobs = []
    for dep_name in dependents:
        job = jobs[dep_name]
        ready = True
        for dep in job.depends_on:
            if dep not in jobs:
                # The dependency is not in the snapshot at all.  Its
                # condition is unsatisfiable, not satisfied.  This used to
                # be filtered out of the check, so a job whose only parent
                # had been deleted became instantly ready and ran.
                ready = False
                break
            if current_states.get(dep) is not JobState.DONE:
                ready = False
                break
        if ready:
            ready_jobs.append(dep_name)

    return ready_jobs
