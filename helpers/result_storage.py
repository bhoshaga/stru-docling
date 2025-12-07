"""
Disk-based result storage for job persistence.
"""
import json
import os
import time
from pathlib import Path
from typing import Dict, Optional, Any
from threading import Lock


# Default to /data in Docker, or ./data locally
DEFAULT_BASE_DIR = os.environ.get("RESULT_STORAGE_DIR", "./data")


class ResultStorage:
    """
    Manages disk-based storage for job results.

    Directory structure:
        /data/results/{job_id}.json - Job results
        /data/uploads/{uuid}.pdf - Uploaded PDFs (temporary)
        /data/stats/metrics.json - Aggregate stats backup
    """

    def __init__(self, base_dir: str = None):
        self.base_dir = Path(base_dir or DEFAULT_BASE_DIR)
        self.results_dir = self.base_dir / "results"
        self.uploads_dir = self.base_dir / "uploads"
        self.stats_dir = self.base_dir / "stats"

        # Create directories
        self.results_dir.mkdir(parents=True, exist_ok=True)
        self.uploads_dir.mkdir(parents=True, exist_ok=True)
        self.stats_dir.mkdir(parents=True, exist_ok=True)

        self._lock = Lock()

    def save_result(self, job_id: str, result: Dict) -> bool:
        """Save a job result to disk."""
        try:
            result_path = self.results_dir / f"{job_id}.json"
            with self._lock:
                with open(result_path, "w") as f:
                    json.dump(result, f, indent=2, default=str)
            return True
        except Exception as e:
            print(f"Error saving result {job_id}: {e}")
            return False

    def get_result(self, job_id: str) -> Optional[Dict]:
        """Load a job result from disk."""
        result_path = self.results_dir / f"{job_id}.json"
        if not result_path.exists():
            return None

        try:
            with self._lock:
                with open(result_path, "r") as f:
                    return json.load(f)
        except Exception as e:
            print(f"Error loading result {job_id}: {e}")
            return None

    def result_exists(self, job_id: str) -> bool:
        """Check if a result exists on disk."""
        return (self.results_dir / f"{job_id}.json").exists()

    def delete_result(self, job_id: str) -> bool:
        """Delete a result from disk."""
        result_path = self.results_dir / f"{job_id}.json"
        try:
            if result_path.exists():
                result_path.unlink()
            return True
        except Exception as e:
            print(f"Error deleting result {job_id}: {e}")
            return False

    def save_upload(self, upload_id: str, pdf_bytes: bytes) -> str:
        """Save an uploaded PDF and return the path."""
        upload_path = self.uploads_dir / f"{upload_id}.pdf"
        with open(upload_path, "wb") as f:
            f.write(pdf_bytes)
        return str(upload_path)

    def get_upload(self, upload_id: str) -> Optional[bytes]:
        """Load an uploaded PDF."""
        upload_path = self.uploads_dir / f"{upload_id}.pdf"
        if not upload_path.exists():
            return None

        with open(upload_path, "rb") as f:
            return f.read()

    def delete_upload(self, upload_id: str) -> bool:
        """Delete an uploaded PDF."""
        upload_path = self.uploads_dir / f"{upload_id}.pdf"
        try:
            if upload_path.exists():
                upload_path.unlink()
            return True
        except Exception as e:
            print(f"Error deleting upload {upload_id}: {e}")
            return False

    def save_stats(self, stats: Dict) -> bool:
        """Save aggregate stats to disk."""
        try:
            stats_path = self.stats_dir / "metrics.json"
            with self._lock:
                with open(stats_path, "w") as f:
                    json.dump(stats, f, indent=2, default=str)
            return True
        except Exception as e:
            print(f"Error saving stats: {e}")
            return False

    def load_stats(self) -> Optional[Dict]:
        """Load aggregate stats from disk."""
        stats_path = self.stats_dir / "metrics.json"
        if not stats_path.exists():
            return None

        try:
            with self._lock:
                with open(stats_path, "r") as f:
                    return json.load(f)
        except Exception as e:
            print(f"Error loading stats: {e}")
            return None

    def save_worker_stats(self, worker_stats: Dict[int, Dict]) -> bool:
        """Save per-worker stats to disk (survives controller restart)."""
        try:
            stats_path = self.stats_dir / "worker_stats.json"
            with self._lock:
                with open(stats_path, "w") as f:
                    json.dump(worker_stats, f, indent=2, default=str)
            return True
        except Exception as e:
            print(f"Error saving worker stats: {e}")
            return False

    def load_worker_stats(self) -> Optional[Dict[str, Dict]]:
        """Load per-worker stats from disk."""
        stats_path = self.stats_dir / "worker_stats.json"
        if not stats_path.exists():
            return None

        try:
            with self._lock:
                with open(stats_path, "r") as f:
                    return json.load(f)
        except Exception as e:
            print(f"Error loading worker stats: {e}")
            return None

    def cleanup_old_files(self, max_age_seconds: int = 3600) -> Dict[str, int]:
        """
        Clean up files older than max_age_seconds.

        Returns:
            Dict with counts of deleted files by type
        """
        now = time.time()
        deleted = {"results": 0, "uploads": 0}

        # Clean results
        for result_file in self.results_dir.glob("*.json"):
            try:
                mtime = result_file.stat().st_mtime
                if now - mtime > max_age_seconds:
                    result_file.unlink()
                    deleted["results"] += 1
            except Exception:
                pass

        # Clean uploads
        for upload_file in self.uploads_dir.glob("*.pdf"):
            try:
                mtime = upload_file.stat().st_mtime
                if now - mtime > max_age_seconds:
                    upload_file.unlink()
                    deleted["uploads"] += 1
            except Exception:
                pass

        return deleted

    def get_storage_stats(self) -> Dict:
        """Get storage usage statistics."""
        results_count = len(list(self.results_dir.glob("*.json")))
        uploads_count = len(list(self.uploads_dir.glob("*.pdf")))

        results_size = sum(f.stat().st_size for f in self.results_dir.glob("*.json"))
        uploads_size = sum(f.stat().st_size for f in self.uploads_dir.glob("*.pdf"))

        return {
            "results_count": results_count,
            "results_size_mb": round(results_size / (1024 * 1024), 2),
            "uploads_count": uploads_count,
            "uploads_size_mb": round(uploads_size / (1024 * 1024), 2),
        }


# Global instance
result_storage = ResultStorage()
