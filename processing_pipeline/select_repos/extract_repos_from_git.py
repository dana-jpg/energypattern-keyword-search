#!/usr/bin/env python3
"""
extract_repos_from_git.py

Find GitHub repos that are > X% Python and meet minimum thresholds
for stars, contributor count, and recent activity. After filtering,
optionally detect whether they are likely *web applications* by scanning
dependencies (requirements/pyproject/etc.) or via GitHub SBOM.
"""

import argparse
import base64
import csv
import datetime as dt
import os
import random
import re
import sys
import time
from typing import Dict, Iterable, List, Optional, Set, Tuple

import requests

# Import filter/detection module
from repo_filter import (
    DEFAULT_WEB_FRAMEWORKS,
    FilterParams,
    Helpers,
    filter_repositories,
)

BASE = "https://api.github.com"

# ---------- Auth & Request Handling (with throttling/backoff) ----------

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

REQUESTS_PER_MIN = int(os.getenv("GITHUB_REQS_PER_MIN", "20"))  # tune if needed
_MIN_INTERVAL = 60.0 / max(1, REQUESTS_PER_MIN)
_last_call = [0.0]  # mutable cell

def github_get(url: str, params: Optional[Dict] = None, allow_404: bool = False) -> requests.Response:
    """GET with primary/secondary rate-limit handling, global throttle, and optional 404 tolerance."""
    backoff = 2.0  # seconds
    while True:
        # throttle
        now = time.time()
        elapsed = now - _last_call[0]
        if elapsed < _MIN_INTERVAL:
            time.sleep(_MIN_INTERVAL - elapsed)
        _last_call[0] = time.time()

        r = requests.get(url, headers=auth_headers(), params=params, timeout=30)

        if r.status_code == 403:
            # Primary limit
            if r.headers.get("X-RateLimit-Remaining") == "0":
                reset = int(r.headers.get("X-RateLimit-Reset", "0"))
                sleep_for = max(5, reset - int(time.time()) + 1)
                print(f"[rate-limit] Core limit hit. Sleeping {sleep_for}s…", file=sys.stderr)
                time.sleep(sleep_for)
                continue
            # Secondary limit
            msg = (r.text or "").lower()
            if "secondary rate limit" in msg:
                jitter = random.uniform(0, 1.0)
                sleep_for = min(backoff + jitter, 60)
                print(f"[secondary-limit] Backing off {sleep_for:.1f}s…", file=sys.stderr)
                time.sleep(sleep_for)
                backoff = min(backoff * 2, 60)
                continue
            raise RuntimeError(f"GitHub API 403: {r.text[:300]}")

        if allow_404 and r.status_code == 404:
            return r

        if r.status_code >= 400:
            raise RuntimeError(f"GitHub API error {r.status_code}: {r.text[:300]}")

        return r

def parse_last_page_from_link(link_header: Optional[str]) -> Optional[int]:
    if not link_header:
        return None
    parts = link_header.split(",")
    for p in parts:
        seg = p.strip()
        if 'rel="last"' in seg:
            start = seg.find("<"); end = seg.find(">")
            if start != -1 and end != -1:
                from urllib.parse import urlparse, parse_qs
                q = parse_qs(urlparse(seg[start+1:end]).query)
                if "page" in q and q["page"]:
                    try:
                        return int(q["page"][0])
                    except ValueError:
                        return None
    return None

def count_via_last_page(url: str, params: Optional[Dict] = None) -> int:
    params = dict(params or {})
    params["per_page"] = 1
    r = github_get(url, params=params)
    if r.status_code == 204:
        return 0
    if r.status_code != 200:
        return 0
    if isinstance(r.json(), list) and len(r.json()) == 0:
        return 0
    last = parse_last_page_from_link(r.headers.get("Link"))
    if last is not None:
        return last
    return 1

# ---------- Core Metrics ----------

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
    url = f"{BASE}/repos/{owner}/{repo}/contributors"
    return count_via_last_page(url, params={"anon": "1"})

def count_recent_commits(owner: str, repo: str, since_iso: str) -> int:
    url = f"{BASE}/repos/{owner}/{repo}/commits"
    return count_via_last_page(url, params={"since": since_iso})

# ---------- Repo Tree + Dependency Files ----------

def get_default_branch(owner: str, repo: str) -> str:
    r = github_get(f"{BASE}/repos/{owner}/{repo}")
    return (r.json().get("default_branch") or "main")

def list_repo_tree(owner: str, repo: str, ref: Optional[str] = None) -> List[str]:
    """
    Return a list of file paths in the repo (up to GitHub's tree limits).
    Uses the Git Trees API with ?recursive=1.
    """
    if not ref:
        ref = get_default_branch(owner, repo)
    r = github_get(f"{BASE}/repos/{owner}/{repo}/git/trees/{ref}", params={"recursive": "1"})
    j = r.json()
    paths: List[str] = []
    for node in j.get("tree", []):
        if node.get("type") == "blob" and "path" in node:
            paths.append(node["path"])
    return paths

def find_dependency_paths(owner: str, repo: str) -> List[str]:
    """Kept for compatibility; unused when fallback is disabled."""
    return []

def fetch_file_base64(owner: str, repo: str, path: str) -> Optional[str]:
    """Kept for compatibility; unused when fallback is disabled."""
    return None

# ---------- SBOM (fast path for dependency detection) ----------

def _parse_purl_locator(locator: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Parse a purl like "pkg:pypi/flask@2.0.0" -> ("pypi", "flask")
    Returns (ecosystem, name)
    """
    if not locator.startswith("pkg:"):
        return None, None
    try:
        after = locator.split("pkg:", 1)[1]  # e.g. "pypi/flask@2.0.0"
        eco, rest = after.split("/", 1)
        name = rest.split("@", 1)[0]
        eco = eco.strip().lower()
        name = name.strip()
        return eco or None, name or None
    except Exception:
        return None, None

def get_repo_sbom(owner: str, repo: str) -> Tuple[int, Iterable[Tuple[str, str]]]:
    """
    Calls GitHub's SBOM export:
      GET /repos/{owner}/{repo}/dependency-graph/sbom
    Returns (status_code, iterator over (ecosystem, package_name)).
    """
    r = github_get(f"{BASE}/repos/{owner}/{repo}/dependency-graph/sbom", allow_404=True)
    status = r.status_code
    if status != 200:
        return status, []

    j = r.json() or {}
    sbom = j.get("sbom") or {}
    packages = sbom.get("packages", []) or []

    def _iter():
        for pkg in packages:
            name = (pkg.get("name") or "").strip()
            eco: Optional[str] = None
            for ref in (pkg.get("externalRefs") or []):
                if (ref.get("referenceType") or "").lower() == "purl":
                    loc = ref.get("referenceLocator", "") or ""
                    eco_p, name_p = _parse_purl_locator(loc)
                    if name_p:
                        name = name_p
                    if eco_p:
                        eco = eco_p
            yield (eco or "unknown", name)

    return status, _iter()

# ---------- Search ----------

def search_repositories(query: str, max_results: int, min_stars: int, pushed_since: Optional[str]) -> List[Dict]:
    """
    Use GitHub repository search to get candidate repos.
    Anchored on language:Python with stars and pushed filters for recency/popularity.
    """
    q_parts: List[str] = []
    if query:
        q_parts.append(query.strip())
    q_parts.append("language:Python")
    if min_stars:
        q_parts.append(f"stars:>={min_stars}")
    if pushed_since:
        q_parts.append(f"pushed:>={pushed_since}")
    q = " ".join(q_parts).strip()

    items: List[Dict] = []
    page = 1
    per_page = 100  # GitHub max
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

# ---------- Main ----------

def main():
    parser = argparse.ArgumentParser(
        description="Find Python-dominant repos with stars/contributors/activity filters; optionally detect web frameworks from SBOM. Saves results to CSV."
    )
    parser.add_argument("--query", type=str, default="", help="Additional GitHub search query (keywords). Avoid too many AND/OR operators.")
    parser.add_argument("--min-python", type=float, default=70.0, help="Minimum Python percent (default: 70).")
    parser.add_argument("--min-stars", type=int, default=100, help="Minimum stars (default: 100).")
    parser.add_argument("--min-contributors", type=int, default=5, help="Minimum contributors (default: 5).")
    parser.add_argument("--days", type=int, default=90, help="Lookback window for activity in days (default: 90).")
    parser.add_argument("--min-commits", type=int, default=20, help="Minimum commits in the lookback window (default: 20).")
    parser.add_argument("--max-results", type=int, default=200, help="Max candidate repos from search (default: 200; per-query search caps ~1000).")
    parser.add_argument("--limit-output", type=int, default=50, help="Limit final printed results (default: 50).")
    parser.add_argument("--out-csv", type=str, default="repos.csv", help="CSV path for ALL matched results (default: repos.csv).")

    # Optional: reduce API calls
    parser.add_argument("--skip-activity", action="store_true", help="Skip recent-commit activity filter (fewer API calls).")
    parser.add_argument("--skip-contributors", action="store_true", help="Skip contributor-count filter (fewer API calls).")

    # Web app detection
    parser.add_argument("--detect-webapps", action="store_true", help="Detect known web frameworks via SBOM.")
    parser.add_argument("--require-web-frameworks", action="store_true", help="If set with --detect-webapps, only keep repos where a known web framework is detected.")
    parser.add_argument("--frameworks", type=str, default="", help="Comma-separated list of frameworks to detect (overrides the default set).")

    args = parser.parse_args()

    # timezone-aware UTC
    now_utc = dt.datetime.now(dt.timezone.utc)
    since_date = (now_utc - dt.timedelta(days=args.days)).date().isoformat()
    since_iso = since_date + "T00:00:00Z"

    # frameworks to detect
    frameworks: Set[str] = DEFAULT_WEB_FRAMEWORKS.copy()
    if args.frameworks.strip():
        frameworks = {s.strip().lower() for s in args.frameworks.split(",") if s.strip()}

    # 1) Search candidates
    try:
        candidates = search_repositories(
            query=args.query,
            max_results=args.max_results,
            min_stars=args.min_stars,
            pushed_since=since_date
        )
    except Exception as e:
        print(f"Search failed: {e}", file=sys.stderr)
        sys.exit(1)

    # 2) Filter + (optional) detect frameworks (SBOM only)
    helpers = Helpers(
        compute_python_percentage=compute_python_percentage,
        count_contributors=count_contributors,
        count_recent_commits=count_recent_commits,
        fetch_file_base64=fetch_file_base64,       # kept for interface compatibility
        find_dependency_paths=find_dependency_paths,  # kept for interface compatibility
        get_repo_sbom=get_repo_sbom,  # returns (status, iterator)
        # exact progress formatting requested
        log=lambda msg: print(msg, flush=True),
        progress=lambda i, total, name: print(f"\n[{i}/{total}] Checking {name} ...", flush=True),
    )
    params = FilterParams(
        min_python=args.min_python,
        min_stars=args.min_stars,
        min_contributors=args.min_contributors,
        min_commits=args.min_commits,
        days=args.days,
        skip_contributors=args.skip_contributors,
        skip_activity=args.skip_activity,
        detect_webapps=args.detect_webapps,
        require_web_frameworks=args.require_web_frameworks,
        frameworks=frameworks,
        since_iso=since_iso,
    )

    try:
        results = filter_repositories(candidates, params, helpers)
    except Exception as e:
        print(f"Filtering failed: {e}", file=sys.stderr)
        sys.exit(1)

    # 3) Sort by simple score
    def score(t) -> int:
        repo, _, contribs, commits, _ = t
        return int(repo.get("stargazers_count", 0)) + 50 * max(contribs, 0) + 10 * max(commits, 0)

    results.sort(key=score, reverse=True)

    # 4) Print concise table
    print(f"\nFound {len(results)} repositories matching criteria (>={args.min_python:.0f}% Python, "
          f"stars>={args.min_stars}, "
          f"{'contributors skipped' if args.skip_contributors else f'contributors>=' + str(args.min_contributors)}, "
          f"{'activity skipped' if args.skip_activity else f'commits(last ' + str(args.days) + 'd)>=' + str(args.min_commits)}"
          f"{', web frameworks required' if (args.detect_webapps and args.require_web_frameworks) else ''}"
          f").\n")

    header = ["Repo", "Stars", "Python %", "Contributors", f"Commits last {args.days}d", "Pushed", "HTML URL", "Web Frameworks", "Description"]
    print("\t".join(header))
    for (repo, py_pct, contributors, commits_recent, frameworks_found) in results[:args.limit_output]:
        row = [
            repo.get("full_name", ""),
            str(repo.get("stargazers_count", 0)),
            f"{py_pct:.1f}",
            ("-" if contributors < 0 else str(contributors)),
            ("-" if commits_recent < 0 else str(commits_recent)),
            repo.get("pushed_at", "") or "",
            repo.get("html_url", ""),
            ",".join(frameworks_found),
            (repo.get("description") or "").replace("\n", " ").strip()
        ]
        print("\t".join(row))

    if len(results) > args.limit_output:
        print(f"\n(Showing top {args.limit_output} of {len(results)}.)")

    # 5) Save ALL matches to CSV
    out_path = args.out_csv
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "repo_full_name", "stars", "python_pct", "contributors",
            f"commits_last_{args.days}d", "pushed_at", "html_url",
            "web_frameworks_detected", "description"
        ])
        for (repo, py_pct, contributors, commits_recent, frameworks_found) in results:
            writer.writerow([
                repo.get("full_name", ""),
                int(repo.get("stargazers_count", 0)),
                f"{py_pct:.2f}",
                contributors,
                commits_recent,
                repo.get("pushed_at", "") or "",
                repo.get("html_url", ""),
                ",".join(frameworks_found),
                (repo.get("description") or "").replace("\n", " ").strip()
            ])
    print(f"\nSaved {len(results)} matched repositories to CSV: {out_path}")

if __name__ == "__main__":
    main()
