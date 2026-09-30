"""
extract_samples.py
Builds the full-load and incremental-load sample files for the Repo Analytics pipeline.

Full load        : records up to the cutoff -> raw/full/<cutoff-date>/full_load_sample.json
Incremental load : records after the cutoff -> raw/incremental/<today>/incremental_load_sample.json

Run everything:   python extract_samples.py
"""
import argparse, json, os, shutil, time
from datetime import datetime, timezone
import requests

API = "https://api.github.com"
REPOS = ["psf/requests", "pallets/flask", "tiangolo/fastapi", "scikit-learn/scikit-learn"]
DEFAULT_CUTOFF = "2026-08-15T00:00:00Z"


def headers():
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        raise SystemExit("Set the GITHUB_TOKEN environment variable first.")
    return {"Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28"}


def get(url, params=None):
    """GET with rate-limit handling and retries (network errors and 5xx)."""
    for attempt in range(6):
        try:
            r = requests.get(url, headers=headers(), params=params, timeout=30)
        except requests.exceptions.RequestException as e:
            print(f"  network problem ({type(e).__name__}), retry {attempt + 1}/6")
            time.sleep(3)
            continue
        if r.status_code in (403, 429) and r.headers.get("X-RateLimit-Remaining") == "0":
            reset = int(r.headers.get("X-RateLimit-Reset", time.time() + 60))
            wait = max(reset - int(time.time()), 1) + 2
            print(f"  rate limited, sleeping {wait}s")
            time.sleep(wait)
            continue
        if r.status_code >= 500:
            print(f"  GitHub error {r.status_code}, retry {attempt + 1}/6")
            time.sleep(5 * (attempt + 1))
            continue
        r.raise_for_status()
        return r
    raise SystemExit(f"Failed after 6 attempts: {url}")


def paged(path, params, max_items, keep=None, max_pages=25):
    out = []
    params = dict(params, per_page=100, page=1)
    while len(out) < max_items and params["page"] <= max_pages:
        rows = get(f"{API}{path}", params).json()
        if not rows:
            break
        for row in rows:
            if keep is None or keep(row):
                out.append(row)
                if len(out) >= max_items:
                    break
        params["page"] += 1
    return out


def collect_repo(repo, mode, cutoff, cap):
    print(f"[{mode}] {repo}")

    # 1) commits
    cp = {"until": cutoff} if mode == "full" else {"since": cutoff}
    commits = paged(f"/repos/{repo}/commits", cp, cap)
    for c in commits:
        c["repo_name"], c["entity_type"] = repo, "commit"

    # 2) issues stream (contains issues AND PRs), most recently updated first
    ip = {"state": "all", "sort": "updated", "direction": "desc"}
    if mode == "full":
        stream = paged(f"/repos/{repo}/issues", ip, cap * 6,
                       keep=lambda i: i["updated_at"] <= cutoff, max_pages=40)
    else:
        ip["since"] = cutoff
        stream = paged(f"/repos/{repo}/issues", ip, cap * 3)

    issues = [i for i in stream if "pull_request" not in i][:cap]
    pr_stubs = [i for i in stream if "pull_request" in i][:cap]
    for i in issues:
        i["repo_name"], i["entity_type"] = repo, "issue"

    # 3) PR details only for items that are PRs (the workaround documented in the proposal)
    pulls = []
    for n, stub in enumerate(pr_stubs, 1):
        pr = get(f"{API}/repos/{repo}/pulls/{stub['number']}").json()
        pr["repo_name"], pr["entity_type"] = repo, "pull_request"
        pulls.append(pr)
        print(f"   fetching PR details {n}/{len(pr_stubs)}", end="\r")

    print(f"   commits={len(commits)} issues={len(issues)} pulls={len(pulls)}          ")
    return {"commits": commits, "issues": issues, "pulls": pulls}


def build(mode, cutoff, cap):
    data = {repo: collect_repo(repo, mode, cutoff, cap) for repo in REPOS}
    now = datetime.now(timezone.utc)
    payload = {"extracted_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
               "load_type": mode, "cutoff": cutoff, "data": data}
    if mode == "incremental":
        payload["since"] = cutoff

    folder_date = cutoff[:10] if mode == "full" else now.strftime("%Y-%m-%d")
    folder = os.path.join("raw", mode, folder_date)
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, f"{mode}_load_sample.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    print("Saved", path, "\n")


def latest_file(mode):
    base = os.path.join("raw", mode)
    return os.path.join(base, sorted(os.listdir(base))[-1], f"{mode}_load_sample.json")


def check():
    full = json.load(open(latest_file("full"), encoding="utf-8"))
    inc = json.load(open(latest_file("incremental"), encoding="utf-8"))
    ok = True
    print("--- FULL LOAD counts ---")
    for repo, d in full["data"].items():
        print(repo, {k: len(v) for k, v in d.items()})
        if not all(d.values()):
            ok = False
            print("  ^ EMPTY GROUP")
    print("\n--- INCREMENTAL counts + commit overlap ---")
    for repo, d in inc["data"].items():
        overlap = len({c["sha"] for c in full["data"][repo]["commits"]} &
                      {c["sha"] for c in d["commits"]})
        print(repo, {k: len(v) for k, v in d.items()}, "commit overlap:", overlap)
        if overlap:
            ok = False
    print("\nFull load cutoff :", full["cutoff"])
    print("Incremental since:", inc["since"], "| extracted:", inc["extracted_at"])
    print("RESULT:", "PASS" if ok else "FIX NEEDED (see EMPTY GROUP / overlap above)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["full", "incremental", "all"], default="all")
    ap.add_argument("--cutoff", default=DEFAULT_CUTOFF, help="e.g. 2026-08-15T00:00:00Z")
    ap.add_argument("--cap", type=int, default=30, help="max records per type per repo")
    ap.add_argument("--check", action="store_true", help="only run the check")
    a = ap.parse_args()

    if a.check:
        check()
    elif a.mode == "all":
        if os.path.isdir("raw"):
            shutil.rmtree("raw")          # start clean: removes old sample folders
        build("full", a.cutoff, a.cap)
        build("incremental", a.cutoff, a.cap)
        check()
    else:
        build(a.mode, a.cutoff, a.cap)