#!/usr/bin/env python3
"""
Docling Redis Client

Client library for submitting jobs to docling workers via Redis.
The VM has no inbound port access - this is the only way to interact with workers.

Usage:
    from local_client import DoclingClient

    client = DoclingClient(redis_url="redis://...")

    # Submit a job
    job_id = client.submit(
        worker_id="docling_1",
        method="POST",
        path="/v1/convert/source/async",
        body={"sources": [...], "options": {...}}
    )

    # Wait for result
    result = client.wait(job_id, timeout=300)
    print(result["response"]["body"])
"""

import json
import time
import uuid
from typing import Optional, Any

import redis


class DoclingClient:
    """Client for submitting jobs to docling workers via Redis."""

    def __init__(self, redis_url: str, pool_size: int = 10):
        """
        Initialize client with Redis connection.

        Args:
            redis_url: Redis connection URL
            pool_size: Connection pool size
        """
        self.redis = redis.from_url(
            redis_url,
            max_connections=pool_size,
            decode_responses=True
        )

    def get_workers(self) -> list[str]:
        """Get list of active worker IDs."""
        return list(self.redis.smembers("docling:workers"))

    def get_worker_status(self, worker_id: str) -> dict:
        """Get status of a specific worker."""
        return self.redis.hgetall(f"docling:worker:{worker_id}")

    def get_all_worker_status(self) -> list[dict]:
        """Get status of all workers."""
        workers = self.get_workers()
        return [self.get_worker_status(w) for w in workers]

    def submit(
        self,
        worker_id: str,
        method: str = "POST",
        path: str = "/v1/convert/source/async",
        content_type: str = "application/json",
        body: Any = None
    ) -> str:
        """
        Submit a job to a worker's queue.

        Args:
            worker_id: Target worker (e.g., "docling_1")
            method: HTTP method
            path: Endpoint path
            content_type: Content-Type header
            body: Request body (will be JSON serialized if dict/list)

        Returns:
            job_id: UUID of the submitted job
        """
        job_id = str(uuid.uuid4())

        # Serialize body
        if isinstance(body, (dict, list)):
            body_str = json.dumps(body)
        else:
            body_str = str(body) if body else ""

        job_data = {
            "job_id": job_id,
            "worker_id": worker_id,
            "status": "pending",
            "request": json.dumps({
                "method": method,
                "path": path,
                "content_type": content_type,
                "body": body_str
            }),
            "response": "",
            "created_at": str(time.time()),
            "started_at": "",
            "completed_at": "",
            "error": "",
            "retry_count": "0"
        }

        # Write job and push to queue
        self.redis.hset(f"docling:job:{job_id}", mapping=job_data)
        self.redis.lpush(f"docling:{worker_id}:jobs", job_id)

        return job_id

    def get_job(self, job_id: str) -> Optional[dict]:
        """
        Get job status and result.

        Args:
            job_id: Job UUID

        Returns:
            Job data dict or None if not found
        """
        job_data = self.redis.hgetall(f"docling:job:{job_id}")
        if not job_data:
            return None

        # Parse JSON fields
        result = {}
        for k, v in job_data.items():
            try:
                result[k] = json.loads(v) if v and v.startswith(("{", "[")) else v
            except:
                result[k] = v

        return result

    def wait(self, job_id: str, timeout: float = 300, poll_interval: float = 0.5) -> dict:
        """
        Wait for job to complete.

        Args:
            job_id: Job UUID
            timeout: Max seconds to wait
            poll_interval: Seconds between polls

        Returns:
            Job data dict

        Raises:
            TimeoutError: If job doesn't complete within timeout
            ValueError: If job not found
        """
        start = time.time()

        while time.time() - start < timeout:
            job = self.get_job(job_id)

            if job is None:
                raise ValueError(f"Job {job_id} not found")

            status = job.get("status", "")
            if status in ("completed", "failed"):
                return job

            time.sleep(poll_interval)

        raise TimeoutError(f"Job {job_id} did not complete within {timeout}s")

    def subscribe_events(self, callback):
        """
        Subscribe to job events via Redis pub/sub.

        Args:
            callback: Function to call with event data
        """
        pubsub = self.redis.pubsub()
        pubsub.subscribe("docling:events")

        for message in pubsub.listen():
            if message["type"] == "message":
                try:
                    event = json.loads(message["data"])
                    callback(event)
                except:
                    pass

    def close(self):
        """Close Redis connection."""
        self.redis.close()


# =============================================================================
# CLI
# =============================================================================

if __name__ == "__main__":
    import argparse
    import os

    parser = argparse.ArgumentParser(description="Docling Redis Client")
    parser.add_argument("--redis-url", default=os.environ.get("REDIS_URL"), help="Redis URL")
    parser.add_argument("--workers", action="store_true", help="List workers")

    args = parser.parse_args()

    if not args.redis_url:
        print("Error: --redis-url or REDIS_URL env var required")
        exit(1)

    client = DoclingClient(args.redis_url)

    if args.workers:
        print("Workers:")
        for w in client.get_all_worker_status():
            print(f"  {w.get('worker_id', '?')}: state={w.get('state', '?')}, pages={w.get('pages_processed', '?')}")
    else:
        parser.print_help()

    client.close()
