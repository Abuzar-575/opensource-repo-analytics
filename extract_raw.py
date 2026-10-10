"""
extract_raw.py - resumable raw extractor for the repo analytics pipeline.

Output: <out>/<mode>/<batch_date>/<owner>__<repo>/<commits|issues|pulls>/page_0001.jsonl
Full:        python extract_raw.py --mode full --start 2025-10-01T00:00:00Z --until 2026-10-01T00:00:00Z --out data_real
Incremental: python extract_raw.py --mode incremental --out data_real
"""
import argparse, json, os, time
from datetime import datetime, timezone
import requests

API = "https://api.github.com"
REPOS = ["psf/requests", "pallets/flask", "tiangolo/fastapi", "scikit-learn/scikit-learn"]
PER_PAGE = 100


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def headers():
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        raise SystemExit("Set the GITHUB_TOKEN environment variable first.")
    return {"Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28"}


def get(url, params=None):
    """GET with rate-limit handling and retries (network errors and 5xx)."""
    for attempt in range(8):
        try:
            r = requests.get(url, headers=headers(), params=params, timeout=30)
        except requests.exceptions.RequestException as e:
            print(f"  network problem ({type(e).__name__}), retry {attempt + 1}/8")
            time.sleep(5)
            continue
        if r.status_code in (403, 429):
            if r.headers.get("Retry-After"):
                wait = int(r.headers["Retry-After"]) + 2
            elif r.headers.get("X-RateLimit-Remaining") == "0":
                reset = int(r.headers.get("X-RateLimit-Reset", time.time() + 60))
                wait = max(reset - int(time.time()), 1) + 2
            else:
                wait = None
            if wait:
                print(f"  rate limited, sleeping {wait}s")
                time.sleep(wait)
                continue
        if r.status_code >= 500:
            print(f"  GitHub error {r.status_code}, retry {attempt + 1}/8")
            time.sleep(5 * (attempt + 1))
            continue
        r.raise_for_status()
        return r
    raise SystemExit(f"Failed after 8 attempts: {url}")


def write_page(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")
    os.replace(tmp, path)          # a page file is either complete or absent


def read_page(path):
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def tag(rows, repo, entity):
    for r in rows:
        r["repo_name"], r["entity_type"] = repo, entity
    return rows


def pfile(base, entity, page):
    return os.path.join(base, entity, f"page_{page:04d}.jsonl")


def do_commits(repo, since, until, base):
    page = 1
    while True:
        f = pfile(base, "commits", page)
        if os.path.exists(f):
            n = len(read_page(f))
        else:
            rows = get(f"{API}/repos/{repo}/commits",
                       {"since": since, "until": until,
                        "per_page": PER_PAGE, "page": page}).json()
            if not rows:
                break
            write_page(f, tag(rows, repo, "commit"))
            n = len(rows)
        print(f"   commits page {page} ({n} rows)")
        if n < PER_PAGE:
            break
        page += 1


def do_issues_and_pulls(repo, since, until, base):
    """Issues stream (includes PR stubs). PR details are fetched for the stubs only."""
    page = 1
    while True:
        fi, fp = pfile(base, "issues", page), pfile(base, "pulls", page)
        if os.path.exists(fi):
            rows = read_page(fi)
            last = len(rows) < PER_PAGE
        else:
            raw = get(f"{API}/repos/{repo}/issues",
                      {"state": "all", "since": since, "sort": "updated",
                       "direction": "asc", "per_page": PER_PAGE, "page": page}).json()
            if not raw:
                break
            rows = [r for r in raw if r["updated_at"] <= until]
            last = len(rows) < len(raw) or len(raw) < PER_PAGE
            if rows:
                write_page(fi, tag(rows, repo, "issue"))
        print(f"   issues page {page} ({len(rows)} rows)")

        if rows and not os.path.exists(fp):
            stubs = [r for r in rows if "pull_request" in r]
            details = []
            for n, s in enumerate(stubs, 1):
                try:
                    details.append(get(f"{API}/repos/{repo}/pulls/{s['number']}").json())
                except requests.exceptions.HTTPError as e:
                    print(f"   skip PR #{s['number']}: {e}")
                print(f"   PR details {n}/{len(stubs)}", end="\r")
            if details:
                write_page(fp, tag(details, repo, "pull_request"))
            print(f"   pulls page {page} ({len(details)} rows)          ")
        if last:
            break
        page += 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["full", "incremental"], required=True)
    ap.add_argument("--start", help="full mode: window start, e.g. 2025-10-01T00:00:00Z")
    ap.add_argument("--until", help="window end (default: now)")
    ap.add_argument("--since", help="incremental mode: override the saved watermark")
    ap.add_argument("--repos", nargs="+", default=REPOS)
    ap.add_argument("--out", default="data_real")
    a = ap.parse_args()

    until = a.until or now_iso()
    state_file = os.path.join(a.out, "_state", "watermark.json")
    if a.mode == "full":
        if not a.start:
            raise SystemExit("--start is required for full mode")
        since = a.start
    else:
        if a.since:
            since = a.since
        elif os.path.exists(state_file):
            since = json.load(open(state_file))["watermark"]
        else:
            raise SystemExit("No watermark found. Run a full load first or pass --since.")

    batch_date = until[:10]
    batch_dir = os.path.join(a.out, a.mode, batch_date)
    done_marker = os.path.join(batch_dir, "_SUCCESS")
    if os.path.exists(done_marker):
        raise SystemExit(f"Batch {batch_dir} already completed. Nothing to do.")

    print(f"mode={a.mode} since={since} until={until} batch={batch_date}")
    for repo in a.repos:
        print(f"[{a.mode}] {repo}")
        base = os.path.join(batch_dir, repo.replace("/", "__"))
        do_commits(repo, since, until, base)
        do_issues_and_pulls(repo, since, until, base)

    if set(a.repos) == set(REPOS):          # only a complete run moves the watermark
        os.makedirs(os.path.dirname(state_file), exist_ok=True)
        json.dump({"watermark": until, "updated": now_iso()}, open(state_file, "w"))
        print("Watermark saved:", until)
    open(done_marker, "w").write(now_iso())
    print("DONE ->", batch_dir)


if __name__ == "__main__":
    main()