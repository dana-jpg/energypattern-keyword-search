#!/usr/bin/env python3
"""
github_repo_finder.py

Search GitHub for repositories that are > X% Python and meet minimum thresholds
for stars, contributor count, and recent activity.

Usage examples:
  python github_repo_finder.py --query "topic:data-science" --min-stars 200
  python github_repo_finder.py --days 120 --min-commits 30 --max-results 100
  python github_repo_finder.py --query "machine learning" --min-contributors 10

Auth:
  Set an environment variable GITHUB_TOKEN to a personal access token (classic or fine-grained)
  with public_repo/read access to increase rate limits.

Notes:
  - Uses GitHub REST API v3.
  - Counts contributors and commits using pagination 'Link' header with per_page=1 for efficiency.
"""

import argparse
import datetime as dt
import os
import sys
import time
from typing import Dict, List, Optional, Tuple
import requests

BASE = "https://api.github.com"

def auth_headers() -> Dict[str, str]:
    token = os.getenv("GITHUB_TOKEN")
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "github-repo-finder-script"
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers

def github_get(url: str, params: Optional[Dict]=None) -> requests.Response:
    """GET with basic rate-limit handling and debug printing."""
    while True:
        print(f"[DEBUG] Requesting: {url} | Params: {params}", file=sys.stderr)
        r = requests.get(url, headers=auth_headers(), params=params, timeout=30)
        if r.status_code == 403 and r.headers.get("X-RateLimit-Remaining") == "0":
            reset = int(r.headers.get("X-RateLimit-Reset", "0"))
            sleep_for = max(0, reset - int(time.time()) + 1)
            print(f"[rate-limit] Sleeping {sleep_for}s until reset...", file=sys.stderr)
            time.sleep(sleep_for)
            continue
        if r.status_code >= 400:
            print(f"[ERROR] {r.status_code} - {r.text[:300]}", file=sys.stderr)
            raise RuntimeError(f"GitHub API error {r.status_code}")
        try:
            j = r.json()
            print(f"[DEBUG] Response (truncated): {str(j)[:300]}...", file=sys.stderr)
        except Exception:
            print(f"[DEBUG] Non-JSON response: {r.text[:200]}", file=sys.stderr)
        return r


def parse_last_page_from_link(link_header: Optional[str]) -> Optional[int]:
    """From a Link header, return the 'last' page number if present."""
    if not link_header:
        return None
    parts = link_header.split(",")
    for p in parts:
        seg = p.strip()
        if 'rel="last"' in seg:
            # format: <url?page=N>; rel="last"
            start = seg.find("<")
            end = seg.find(">")
            if start != -1 and end != -1:
                url_part = seg[start+1:end]
                # extract page=N
                from urllib.parse import urlparse, parse_qs
                q = parse_qs(urlparse(url_part).query)
                if "page" in q and q["page"]:
                    try:
                        return int(q["page"][0])
                    except ValueError:
                        return None
    return None

def count_via_last_page(url: str, params: Optional[Dict]=None) -> int:
    """
    Efficiently count list results by requesting per_page=1 and reading 'last' page from Link header.
    Falls back to 1 or 0 when appropriate.
    """
    params = dict(params or {})
    params["per_page"] = 1
    r = github_get(url, params=params)
    if r.status_code == 204:
        return 0
    if r.status_code != 200:
        return 0
    # If there are no results, GitHub returns [] and no Link header.
    if isinstance(r.json(), list) and len(r.json()) == 0:
        return 0
    # If there is a Link header with last page number, that is the total count.
    last = parse_last_page_from_link(r.headers.get("Link"))
    if last is not None:
        return last
    # Otherwise, if we got an item but no Link header, the count is 1.
    return 1

def compute_python_percentage(owner: str, repo: str) -> float:
    url = f"{BASE}/repos/{owner}/{repo}/languages"
    r = github_get(url)
    data = r.json() or {}
    total = sum(data.values()) or 0
    if total == 0:
        return 0.0
    py_bytes = data.get("Python", 0)
    return (py_bytes / total) * 100.0

def count_contributors(owner: str, repo: str) -> int:
    # include anonymous contributors to approximate "sufficient contributors"
    url = f"{BASE}/repos/{owner}/{repo}/contributors"
    return count_via_last_page(url, params={"anon": "1"})

def count_recent_commits(owner: str, repo: str, since_iso: str) -> int:
    url = f"{BASE}/repos/{owner}/{repo}/commits"
    # Using per_page=1 + Link: rel=last to get count efficiently
    return count_via_last_page(url, params={"since": since_iso})

def search_repositories(query: str, max_results: int, min_stars: int, pushed_since: Optional[str]) -> List[Dict]:
    """
    Use GitHub code search to get candidate repos.
    We seed by language:Python + stars + pushed to keep candidates relevant and reduce API work.
    The >70% Python filter is applied after using the /languages endpoint per repo.
    """
    q_parts = [query.strip()] if query else []
    q_parts.append("language:Python")
    if min_stars:
        q_parts.append(f"stars:>={min_stars}")
    if pushed_since:
        q_parts.append(f"pushed:>={pushed_since}")
    q = " ".join(q_parts).strip()

    items: List[Dict] = []
    page = 1
    per_page = 50  # max is 100, use 50 for safety
    while len(items) < max_results:
        params = {"q": q, "sort": "stars", "order": "desc", "page": page, "per_page": per_page}
        r = github_get(f"{BASE}/search/repositories", params=params)
        data = r.json()
        batch = data.get("items", [])
        if not batch:
            break
        items.extend(batch)
        if len(batch) < per_page:
            break
        page += 1
    return items[:max_results]

def main():
    parser = argparse.ArgumentParser(description="Find GitHub repos that are > X% Python with sufficient stars, contributors, and activity.")
    parser.add_argument("--query", type=str, default="", help="GitHub search query (e.g., 'topic:mlops' or 'data engineering').")
    parser.add_argument("--min-python", type=float, default=70.0, help="Minimum Python percent (default: 70).")
    parser.add_argument("--min-stars", type=int, default=100, help="Minimum stars (default: 100).")
    parser.add_argument("--min-contributors", type=int, default=5, help="Minimum contributors (default: 5).")
    parser.add_argument("--days", type=int, default=90, help="Lookback window for activity in days (default: 90).")
    parser.add_argument("--min-commits", type=int, default=20, help="Minimum commits in the lookback window (default: 20).")
    parser.add_argument("--max-results", type=int, default=200, help="Max candidate repos to scan from search (default: 200).")
    parser.add_argument("--limit-output", type=int, default=50, help="Limit final printed results (default: 50).")
    args = parser.parse_args()

    since_date = (dt.datetime.utcnow() - dt.timedelta(days=args.days)).date().isoformat()
    since_iso = since_date + "T00:00:00Z"

    try:
        candidates = search_repositories(
            query=args.query,
            max_results=args.max_results,
            min_stars=args.min_stars,
            pushed_since=since_date
        )
        print(f"[INFO] Retrieved {len(candidates)} candidate repos from GitHub search", file=sys.stderr)

    except Exception as e:
        print(f"Search failed: {e}", file=sys.stderr)
        sys.exit(1)

    results: List[Tuple[Dict, float, int, int]] = []
    for repo in candidates:
        full_name = repo.get("full_name")
        if not full_name:
            continue
        owner, name = full_name.split("/", 1)
        try:
            py_pct = compute_python_percentage(owner, name)
            if py_pct < args.min_python:
                continue

            stars = int(repo.get("stargazers_count", 0))
            if stars < args.min_stars:
                continue

            contributors = count_contributors(owner, name)
            if contributors < args.min_contributors:
                continue

            commits_recent = count_recent_commits(owner, name, since_iso)
            if commits_recent < args.min_commits:
                continue

            results.append((repo, py_pct, contributors, commits_recent))
        except Exception as e:
            # Non-fatal: skip problematic repos
            print(f"[skip] {full_name}: {e}", file=sys.stderr)
            continue

    # Sort by a simple score: stars + 50*contributors + 10*recent_commits (tunable)
    def score(t) -> int:
        repo, _, contribs, commits = t
        return int(repo.get("stargazers_count", 0)) + 50*contribs + 10*commits

    results.sort(key=score, reverse=True)

    # Print concise table
    print(f"\nFound {len(results)} repositories matching criteria (>={args.min_python:.0f}% Python,"
          f" stars>={args.min_stars}, contributors>={args.min_contributors},"
          f" commits(last {args.days}d)>={args.min_commits}).\n")

    header = ["Repo", "Stars", "Python %", "Contributors", f"Commits last {args.days}d", "Pushed", "HTML URL", "Description"]
    print("\t".join(header))
    for (repo, py_pct, contributors, commits_recent) in results[:args.limit_output]:
        row = [
            repo.get("full_name", ""),
            str(repo.get("stargazers_count", 0)),
            f"{py_pct:.1f}",
            str(contributors),
            str(commits_recent),
            repo.get("pushed_at", "") or "",
            repo.get("html_url", ""),
            (repo.get("description") or "").replace("\n", " ").strip()
        ]
        print("\t".join(row))

    if len(results) > args.limit_output:
        print(f"\n(Showing top {args.limit_output} of {len(results)}.)")

if __name__ == "__main__":
    main()
