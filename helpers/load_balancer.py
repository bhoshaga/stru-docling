"""
Load balancer for distributing work across workers.
"""
from typing import Dict, List, Optional, Any
from threading import Lock


class LoadBalancer:
    """
    Manages worker pools and selects workers for job distribution.

    Workers are organized into pools:
    - Dedicated pools: reserved for specific API keys
    - Shared pool: available to all API keys
    """

    def __init__(self, min_port: int = 5001, max_port: int = 5010):
        self.min_port = min_port
        self.max_port = max_port

        # Worker state tracking (shared with controller)
        self._workers: Dict[int, Dict] = {}  # port -> worker info
        self._lock = Lock()

        # Pool configuration
        # Default: 8 dedicated (ports 5001-5008), 2 shared (5009-5010)
        self._dedicated_ports = list(range(min_port, min_port + 8))
        self._shared_ports = list(range(min_port + 8, max_port + 1))

        # API key to dedicated pool mapping
        self._api_key_pools: Dict[str, List[int]] = {}

    def register_worker(self, port: int, worker_info: Dict):
        """Register a worker with its current state."""
        with self._lock:
            self._workers[port] = worker_info

    def update_worker(self, port: int, updates: Dict):
        """Update worker state."""
        with self._lock:
            if port in self._workers:
                self._workers[port].update(updates)

    def unregister_worker(self, port: int):
        """Remove a worker."""
        with self._lock:
            self._workers.pop(port, None)

    def get_available_workers(self, api_key: Optional[str] = None) -> List[int]:
        """
        Get list of available worker ports for an API key.

        Args:
            api_key: Optional API key for pool selection

        Returns:
            List of available worker ports (ready state, not processing)
        """
        with self._lock:
            # Determine which ports to consider
            if api_key and api_key in self._api_key_pools:
                candidate_ports = self._api_key_pools[api_key]
            else:
                # Use shared pool for unknown/default API keys
                candidate_ports = self._shared_ports + self._dedicated_ports

            # Filter to ready workers
            available = []
            for port in candidate_ports:
                worker = self._workers.get(port)
                if worker and worker.get("state") == "ready":
                    available.append(port)

            return available

    def select_workers(
        self,
        num_workers: int,
        api_key: Optional[str] = None,
    ) -> List[int]:
        """
        Select workers for a job, prioritizing least loaded.

        Args:
            num_workers: Number of workers needed
            api_key: Optional API key for pool selection

        Returns:
            List of selected worker ports
        """
        with self._lock:
            # Determine candidate pools based on API key
            if api_key and api_key in self._api_key_pools:
                candidate_ports = self._api_key_pools[api_key]
            else:
                # Unknown API key gets shared pool only
                candidate_ports = self._shared_ports

            # Get worker states and sort by load
            workers_with_load = []
            for port in candidate_ports:
                worker = self._workers.get(port)
                if worker:
                    state = worker.get("state", "unknown")
                    pages = worker.get("pages_processed", 0)
                    memory = worker.get("memory_mb", 0)

                    # Prefer ready workers, then by pages processed
                    if state == "ready":
                        priority = (0, pages, memory)
                    elif state == "processing":
                        priority = (1, pages, memory)
                    else:
                        priority = (2, pages, memory)

                    workers_with_load.append((port, priority))

            # Sort by priority (lower is better)
            workers_with_load.sort(key=lambda x: x[1])

            # Select top N workers
            selected = [port for port, _ in workers_with_load[:num_workers]]

            return selected

    def select_single_worker(self, api_key: Optional[str] = None) -> Optional[int]:
        """Select a single best worker for the given API key."""
        selected = self.select_workers(1, api_key)
        return selected[0] if selected else None

    def assign_api_key_pool(self, api_key: str, ports: List[int]):
        """Assign dedicated worker ports to an API key."""
        with self._lock:
            self._api_key_pools[api_key] = ports

    def get_pool_status(self) -> Dict:
        """Get status of all worker pools."""
        with self._lock:
            dedicated_status = []
            for port in self._dedicated_ports:
                worker = self._workers.get(port, {})
                dedicated_status.append({
                    "port": port,
                    "state": worker.get("state", "unknown"),
                    "pages_processed": worker.get("pages_processed", 0),
                    "memory_mb": worker.get("memory_mb", 0),
                })

            shared_status = []
            for port in self._shared_ports:
                worker = self._workers.get(port, {})
                shared_status.append({
                    "port": port,
                    "state": worker.get("state", "unknown"),
                    "pages_processed": worker.get("pages_processed", 0),
                    "memory_mb": worker.get("memory_mb", 0),
                })

            return {
                "dedicated": dedicated_status,
                "shared": shared_status,
                "api_key_pools": dict(self._api_key_pools),
            }

    def get_worker_count(self) -> int:
        """Get total number of registered workers."""
        with self._lock:
            return len(self._workers)


# Global instance
load_balancer = LoadBalancer()
