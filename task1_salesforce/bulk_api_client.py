"""Salesforce Bulk API v2 Client with OAuth2 authentication.

Implements the full Bulk API v2 lifecycle:
1. OAuth2 token acquisition
2. Create bulk query job
3. Poll job status until completion
4. Retrieve paginated results
5. Exponential backoff retry for rate limits

Supports both real Salesforce instances and the mock server.
"""

import time
import httpx
import structlog
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)
from typing import Optional

from shared.config import get_settings

logger = structlog.get_logger(__name__)


class RateLimitError(Exception):
    """Raised when Salesforce returns HTTP 429 (rate limit)."""
    pass


class BulkJobError(Exception):
    """Raised when a bulk job fails."""
    pass


class SalesforceBulkClient:
    """Client for Salesforce Bulk API v2 with retry and rate-limit handling."""

    # The 12 Salesforce objects to ingest
    OBJECTS = [
        "Account", "Contact", "Opportunity", "Lead", "Task", "Case",
        "Product2", "PricebookEntry", "Contract", "Asset", "Campaign", "Event",
    ]

    API_VERSION = "v59.0"

    def __init__(self, base_url: str = None):
        settings = get_settings()
        self.base_url = base_url or settings.MOCK_SALESFORCE_URL
        self.api_base = f"{self.base_url}/services/data/{self.API_VERSION}"
        self.access_token: Optional[str] = None
        self.client = httpx.Client(timeout=30.0)

    def _headers(self) -> dict:
        """Build authorization headers."""
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def authenticate(self) -> str:
        """Obtain OAuth2 access token from Salesforce."""
        logger.info("[AUTH] Authenticating with Salesforce...")
        response = self.client.post(
            f"{self.base_url}/services/oauth2/token",
            json={
                "grant_type": "client_credentials",
                "client_id": "mock_client_id",
                "client_secret": "mock_client_secret",
            },
        )
        response.raise_for_status()
        data = response.json()
        self.access_token = data["access_token"]
        logger.info("[AUTH] Authentication successful", token_prefix=self.access_token[:20])
        return self.access_token

    @retry(
        retry=retry_if_exception_type(RateLimitError),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        stop=stop_after_attempt(8),
        before_sleep=lambda retry_state: structlog.get_logger().warning(
            "[RETRY] Rate limited, backing off",
            attempt=retry_state.attempt_number,
            wait=retry_state.next_action.sleep,
        ),
    )
    def _request(self, method: str, url: str, **kwargs) -> httpx.Response:
        """Make an HTTP request with automatic rate-limit retry."""
        response = self.client.request(method, url, headers=self._headers(), **kwargs)

        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", "2"))
            logger.warning("[RATE LIMIT] 429 received", retry_after=retry_after)
            raise RateLimitError(f"Rate limited, retry after {retry_after}s")

        response.raise_for_status()
        return response

    def create_bulk_job(self, object_name: str) -> dict:
        """Create a Bulk API v2 query job for a Salesforce object.

        Args:
            object_name: Salesforce object name (e.g., 'Account').

        Returns:
            Job info dict with 'id', 'state', 'object', etc.
        """
        soql = f"SELECT FIELDS(ALL) FROM {object_name} LIMIT 10000"

        logger.info("[BULK JOB] Creating query job", object=object_name)
        response = self._request(
            "POST",
            f"{self.api_base}/jobs/query",
            json={
                "operation": "query",
                "query": soql,
                "object": object_name,
                "contentType": "JSON",
            },
        )
        job = response.json()
        logger.info("[BULK JOB] Job created", job_id=job["id"], object=object_name, state=job["state"])
        return job

    def get_job_status(self, job_id: str) -> dict:
        """Poll the status of a bulk query job."""
        response = self._request("GET", f"{self.api_base}/jobs/query/{job_id}")
        return response.json()

    def wait_for_job(self, job_id: str, poll_interval: float = 1.0, timeout: float = 300.0) -> dict:
        """Wait for a bulk job to complete, polling at intervals.

        Args:
            job_id: The bulk job ID.
            poll_interval: Seconds between status checks.
            timeout: Maximum wait time in seconds.

        Returns:
            Final job status dict.

        Raises:
            BulkJobError: If job fails or times out.
        """
        start = time.time()
        while True:
            status = self.get_job_status(job_id)
            state = status.get("state", "Unknown")

            logger.info(
                "[BULK JOB] Polling status",
                job_id=job_id,
                state=state,
                records=status.get("numberRecordsProcessed", 0),
            )

            if state == "JobComplete":
                return status
            elif state in ("Failed", "Aborted"):
                raise BulkJobError(f"Job {job_id} ended with state: {state}")

            if time.time() - start > timeout:
                raise BulkJobError(f"Job {job_id} timed out after {timeout}s")

            time.sleep(poll_interval)

    def get_results(self, job_id: str, max_records: int = 2000) -> list[dict]:
        """Retrieve all results from a completed bulk job with pagination.

        Handles the Salesforce sforce-locator pagination pattern.
        """
        all_records = []
        locator = None

        while True:
            params = {"maxRecords": max_records}
            if locator:
                params["locator"] = locator

            response = self._request(
                "GET",
                f"{self.api_base}/jobs/query/{job_id}/results",
                params=params,
            )
            data = response.json()

            records = data.get("records", [])
            all_records.extend(records)

            next_locator = data.get("sforce-locator")
            is_done = data.get("done", True)

            logger.info(
                "[BULK JOB] Retrieved batch",
                job_id=job_id,
                batch_size=len(records),
                total=len(all_records),
                done=is_done,
            )

            if is_done or not next_locator:
                break
            locator = next_locator

        return all_records

    def abort_job(self, job_id: str) -> dict:
        """Abort a running bulk job."""
        response = self._request("PATCH", f"{self.api_base}/jobs/query/{job_id}")
        return response.json()

    def list_jobs(self) -> list[dict]:
        """List all bulk API jobs."""
        response = self._request("GET", f"{self.api_base}/jobs/query")
        data = response.json()
        return data.get("records", [])

    def get_available_objects(self) -> dict:
        """List available Salesforce objects and their schemas."""
        response = self.client.get(f"{self.base_url}/api/objects")
        return response.json()

    def close(self):
        """Close the HTTP client."""
        self.client.close()
