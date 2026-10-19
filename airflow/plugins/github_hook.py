from __future__ import annotations

from typing import Any

import requests

from airflow.hooks.base import BaseHook


class GitHubHook(BaseHook):
    """
    Hook for interacting with GitHub REST API v3.

    Uses Airflow connection with schema:
      github://[token]@github.com

    Or uses extra fields:
      {"token": "ghp_xxx", "base_url": "https://api.github.com"}
    """

    def __init__(
        self,
        github_conn_id: str = "github_default",
        timeout: int = 120,
        retry: int = 3,
    ) -> None:
        self.github_conn_id = github_conn_id
        self.timeout = timeout
        self.retry = retry
        super().__init__()

    def get_conn(self) -> dict[str, Any]:
        """Get GitHub API configuration from connection."""
        conn = self.get_connection(self.github_conn_id)

        token = conn.login or conn.password
        if not token and conn.extra:
            extra = self._parse_extra(conn.extra)
            token = extra.get("token", token)

        base_url = conn.host or "https://api.github.com"
        if not base_url.startswith("http"):
            base_url = f"https://{base_url}"

        return {
            "base_url": base_url,
            "token": token,
            "timeout": self.timeout,
        }

    def _parse_extra(self, extra: str) -> dict[str, Any]:
        """Parse extra JSON field from connection."""
        import json

        try:
            return json.loads(extra)
        except (json.JSONDecodeError, TypeError):
            return {}

    def _get_headers(self, token: str | None = None) -> dict[str, str]:
        """Get standard GitHub API headers."""
        headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "airflow-dora-metrics",
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    def _handle_pagination(
        self,
        base_url: str,
        endpoint: str,
        params: dict | None = None,
        per_page: int = 100,
        since_param_name: str = "since",
        since_param_value: str | None = None,
        until_param_name: str = "until",
        until_param_value: str | None = None,
        max_records: int = 10000,
        data_array_key: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        Handle GitHub API pagination using cursor-based pagination.

        Args:
            base_url: GitHub API base URL
            endpoint: API endpoint path
            params: Query parameters (will be preserved across pages)
            per_page: Items per page
            since_param_name: Name of the since query parameter (default "since")
            since_param_value: If provided, this param value is locked across all pages
                               for incremental loads
            until_param_name: Name of the until query parameter (default "until")
            until_param_value: If provided, this param value is locked across all pages
                               (e.g. "now()" for commits date range)
            max_records: Maximum number of records to fetch (default 10 000)
            data_array_key: If set, extract array from response dict (e.g. "workflow_runs")
        """
        import copy
        import re
        import time

        all_items = []
        base_params = copy.deepcopy(params or {})
        base_params["per_page"] = per_page

        # Lock the since_param value if provided (incremental load)
        if since_param_value:
            base_params[since_param_name] = since_param_value

        # Lock the until_param value if provided
        if until_param_value:
            base_params[until_param_name] = until_param_value

        # Build base URL
        if endpoint.startswith("http"):
            url = endpoint
        else:
            url = f"{base_url}{endpoint}"

        # Log start
        params_str = "&".join(f"{k}={v}" for k, v in sorted(base_params.items()))
        self.log.info("Starting pagination: %s?%s (per_page=%d)", endpoint, params_str, per_page)

        start_time = time.time()
        page_count = 0
        use_cursor = False
        consecutive_errors = 0

        while True:
            config = self.get_conn()
            headers = self._get_headers(config["token"])

            # Create fresh params for each request
            current_params = copy.deepcopy(base_params)

            try:
                response = requests.get(
                    url,
                    headers=headers,
                    params=None if use_cursor else current_params,
                    timeout=config["timeout"],
                )

                # Handle rate limit
                if response.status_code == 403:
                    reset_time = int(response.headers.get("X-RateLimit-Reset", 0))
                    wait_time = max(1, reset_time - int(time.time()))
                    self.log.warning("Rate limit hit during pagination. Waiting %d seconds", min(wait_time, 60))
                    time.sleep(min(wait_time, 60))
                    continue

                # Retry on any server error (4xx/5xx)
                if response.status_code >= 400:
                    consecutive_errors += 1
                    self.log.warning(
                        "Request error %d (retry %d/10), retrying in 5 seconds",
                        response.status_code,
                        consecutive_errors,
                    )
                    time.sleep(5)
                    if consecutive_errors >= 10:
                        self.log.error("Too many errors, stopping pagination")
                        break
                    continue

                consecutive_errors = 0
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                self.log.warning("Request failed: %s", e)
                break

            # Rate limiting: GitHub allows 5000 req/hr, add small delay
            time.sleep(0.3)

            if not response.content:
                break

            data = response.json()
            if not data:
                break

            # Extract array from dict if data_array_key is specified
            if data_array_key and isinstance(data, dict):
                items = data.get(data_array_key, [])
            elif isinstance(data, list):
                items = data
            else:
                self.log.warning("Unexpected response format, stopping pagination")
                break

            all_items.extend(items)
            page_count += 1

            # Log progress every 10 pages
            if page_count % 10 == 0 or page_count <= 3:
                self.log.info("Progress: page %d, fetched %d items", page_count, len(all_items))

            # Check max records limit
            if len(all_items) >= max_records:
                self.log.warning("Reached limit of %d records, stopping pagination", max_records)
                break

            # Check Link header for next page (cursor-based)
            link_header = response.headers.get("Link", "")
            if 'rel="next"' not in link_header:
                break

            # Extract next page URL from Link header
            next_urls = re.findall(r'<([^>]+)>;\s*rel="next"', link_header)
            if not next_urls:
                break

            # Use the full URL from Link header (cursor-based pagination)
            url = next_urls[0]
            use_cursor = True

        elapsed = time.time() - start_time
        self.log.info("Pagination complete: %d pages, %d items, %.2f seconds", page_count, len(all_items), elapsed)

        return all_items

    def get(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        paginate: bool = False,
        per_page: int = 100,
        since_param_name: str = "since",
        since_param_value: str | None = None,
        until_param_name: str = "until",
        until_param_value: str | None = None,
        max_records: int = 10000,
        data_array_key: str | None = None,
    ) -> Any:
        """
        Make GET request to GitHub API.

        Args:
            endpoint: API endpoint (e.g. /repos/microsoft/vscode/releases)
            params: Query parameters
            paginate: Whether to handle pagination
            per_page: Items per page for pagination
            since_param_name: Name of the since query parameter (default "since")
            since_param_value: ISO timestamp for incremental loads (locked across pages)
            until_param_name: Name of the until query parameter (default "until")
            until_param_value: ISO timestamp for end of range (locked across pages)
            max_records: Maximum records to fetch (default 10 000)
            data_array_key: Extract array from response dict (e.g. "workflow_runs")

        Returns:
            JSON response as dict/list or None on error
        """
        config = self.get_conn()

        # If endpoint is already a full URL, use it directly
        if endpoint.startswith("http"):
            url = endpoint
        else:
            url = f"{config['base_url']}{endpoint}"

        headers = self._get_headers(config["token"])

        last_error = None
        for attempt in range(self.retry):
            try:
                self.log.debug("GET %s (attempt %d)", url, attempt + 1)
                response = requests.get(
                    url,
                    headers=headers,
                    params=params,
                    timeout=config["timeout"],
                )

                if response.status_code == 403:
                    # Rate limit - wait and retry
                    reset_time = int(response.headers.get("X-RateLimit-Reset", 0))
                    import time

                    wait_time = max(1, reset_time - int(time.time()))
                    self.log.warning("Rate limit hit. Waiting %d seconds until reset", wait_time)
                    time.sleep(min(wait_time, 60))  # Max 60s wait
                    continue

                response.raise_for_status()

                if not response.content:
                    return None

                data = response.json()

                if paginate:
                    return self._handle_pagination(
                        config["base_url"],
                        endpoint,
                        params,
                        per_page,
                        since_param_name,
                        since_param_value,
                        until_param_name,
                        until_param_value,
                        max_records,
                        data_array_key,
                    )

                return data

            except requests.exceptions.RequestException as e:
                last_error = e
                self.log.warning("Request failed (attempt %d/%d): %s", attempt + 1, self.retry, e)
                if attempt < self.retry - 1:
                    import time

                    time.sleep(2**attempt)

        if last_error:
            raise last_error
        return None

    def post(
        self,
        endpoint: str,
        json: dict[str, Any] | None = None,
    ) -> Any:
        """Make POST request to GitHub API."""
        config = self.get_conn()
        url = f"{config['base_url']}{endpoint}"
        headers = self._get_headers(config["token"])
        last_exc: Exception | None = None

        for attempt in range(self.retry):
            try:
                response = requests.post(
                    url,
                    headers=headers,
                    json=json,
                    timeout=config["timeout"],
                )
                response.raise_for_status()
                return response.json() if response.content else None
            except requests.exceptions.RequestException as e:
                last_exc = e
                self.log.warning("POST failed (attempt %d/%d): %s", attempt + 1, self.retry, e)
                if attempt < self.retry - 1:
                    import time

                    time.sleep(2**attempt)

        raise Exception("POST request failed") from last_exc

    # ===== Convenience methods for DORA metrics =====

    def get_repo(self, owner: str, repo: str) -> dict[str, Any]:
        """Get repository metadata."""
        return self.get(f"/repos/{owner}/{repo}")

    def get_releases(self, owner: str, repo: str, paginate: bool = True) -> list[dict[str, Any]]:
        """Get all releases sorted by created_at ascending."""
        return self.get(
            f"/repos/{owner}/{repo}/releases",
            params={"sort": "created", "direction": "asc"},
            paginate=paginate,
        )

    def get_pull_requests(
        self,
        owner: str,
        repo: str,
        state: str = "closed",
        base: str = "main",
        since: str | None = None,
        only_merged: bool = True,  # DORA: only merged PRs
    ) -> list[dict[str, Any]]:
        """Get pull requests with pagination."""
        params = {"state": state, "base": base, "sort": "updated", "direction": "asc"}
        data = self.get(
            f"/repos/{owner}/{repo}/pulls",
            params=params,
            paginate=True,
            since_param_name="since",
            since_param_value=since,
        )
        if only_merged:
            data = [pr for pr in data if pr.get("merged_at")]
        return data

    def get_issues(
        self,
        owner: str,
        repo: str,
        state: str = "closed",
        labels: list[str] | None = None,
        since: str | None = None,
    ) -> list[dict[str, Any]]:
        """Get issues with pagination."""
        params = {"state": state, "sort": "updated", "direction": "desc"}
        if labels:
            params["labels"] = ",".join(labels)
        if since:
            params["since"] = since
        return self.get(
            f"/repos/{owner}/{repo}/issues",
            params=params,
            paginate=True,
        )

    def get_commits(
        self,
        owner: str,
        repo: str,
        sha: str = "main",
        path: str | None = None,
        since: str | None = None,
        until: str | None = None,
    ) -> list[dict[str, Any]]:
        """Get commits with pagination."""
        params = {"sha": sha, "per_page": 100}
        if path:
            params["path"] = path
        return self.get(
            f"/repos/{owner}/{repo}/commits",
            params=params,
            paginate=True,
            since_param_name="since",
            since_param_value=since,
            until_param_name="until",
            until_param_value=until,
        )

    def get_pull_request_reviews(self, owner: str, repo: str, pull_number: int) -> list[dict[str, Any]]:
        """Get reviews for a specific PR."""
        return self.get(
            f"/repos/{owner}/{repo}/pulls/{pull_number}/reviews",
            paginate=True,
        )

    def get_issue_timeline(self, owner: str, repo: str, issue_number: int) -> list[dict[str, Any]]:
        """Get timeline events for an issue/PR."""
        return self.get(
            f"/repos/{owner}/{repo}/issues/{issue_number}/timeline",
            paginate=True,
        )

    def get_milestones(self, owner: str, repo: str, state: str = "all") -> list[dict[str, Any]]:
        """Get milestones sorted by created_at ascending."""
        return self.get(
            f"/repos/{owner}/{repo}/milestones",
            params={"state": state, "sort": "created", "direction": "asc"},
            paginate=True,
        )

    def get_compare(self, owner: str, repo: str, base: str, head: str) -> dict[str, Any]:
        """Get commits between two refs."""
        return self.get(
            f"/repos/{owner}/{repo}/compare/{base}...{head}",
        )

    def get_actions_runs(
        self,
        owner: str,
        repo: str,
        status: str | None = None,
        per_page: int = 100,
        since: str | None = None,
    ) -> list[dict[str, Any]]:
        """Get workflow runs with pagination.

        GitHub API limits to 1000 records per query. For larger datasets,
        use date-range chunking via the 'since' parameter.

        Args:
            owner: Repository owner
            repo: Repository name
            status: Filter by status (e.g., 'completed')
            per_page: Records per page (max 100)
            since: ISO timestamp to start from (for incremental loads)
        """
        all_runs = []
        page = 1

        while True:
            params = {"per_page": per_page, "page": page}
            if status:
                params["status"] = status
            if since:
                # GitHub API uses 'created' for date filtering
                params["created"] = since

            config = self.get_conn()
            headers = self._get_headers(config["token"])
            url = f"{config['base_url']}/repos/{owner}/{repo}/actions/runs"

            query_string = f"{url}?{'&'.join(f'{k}={v}' for k, v in sorted(params.items()))}"
            self.log.info("Fetching page %d (%d records): %s", page, per_page, query_string)

            try:
                response = requests.get(url, headers=headers, params=params, timeout=config["timeout"])
                response.raise_for_status()
            except requests.exceptions.RequestException:
                break

            data = response.json()
            runs = data.get("workflow_runs", [])
            if not runs:
                # No more data - stop pagination
                break

            all_runs.extend(runs)
            self.log.info("Page %d: fetched %d records, total so far: %d", page, len(runs), len(all_runs))

            # Check if we've reached the total count from API response
            total_count = data.get("total_count", 0)
            if len(all_runs) >= total_count:
                self.log.info("Reached total_count (%d), stopping pagination", total_count)
                break

            # Check Link header for next page
            link_header = response.headers.get("Link", "")
            if 'rel="next"' not in link_header:
                break

            import re

            next_urls = re.findall(r'<([^>]+)>;\s*rel="next"', link_header)
            if not next_urls:
                break

            if len(all_runs) > 10000:
                self.log.warning("Reached limit of 10000 records, stopping pagination")
                break

            page += 1

        self.log.info("Total workflow runs fetched: %d across %d pages", len(all_runs), page)
        return all_runs
