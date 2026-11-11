#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Trim the node-pool Clash subscription to the 8 fastest nodes.

Runs as a step in the aggregator's Process workflow, right after the
Process step, in the same Actions job (mihomo binary + PyYAML already
present from the checkout / pip install).

What it does:
  1. Reads my-config.json (repo root) for the two output Gist IDs.
  2. Fetches the just-generated full pool-clash.yaml from its Gist.
  3. Starts the repo's mihomo binary with that config and measures the
     delay of every proxy through mihomo's external-controller API
     (same method the aggregator's own liveness check uses).
  4. Keeps the user's own 2 nodes (server 129.225.207.26) always, plus
     the 6 fastest free nodes by measured delay. Total = 8.
  5. Rewrites proxies / proxy-groups (rules untouched) and PATCHes the
     Clash Gist. Also trims the v2ray file to its 8 fastest vmess nodes.
  6. Prints the kept list with delays; exits non-zero on fatal errors
     so the workflow step fails loudly instead of silently shipping
     a broken subscription.

Env:
  PUSH_TOKEN  GitHub personal access token with `gist` scope
              (already a repository secret, exposed as job env).
  TRIM_DRY_RUN=1  measure + write trimmed files locally, do NOT
              PATCH any Gist (for testing outside Actions).

Only stdlib + PyYAML are used.
"""

import base64
import concurrent.futures
import copy
import json
import os
import shutil
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

import yaml

REPO_ROOT = os.path.abspath(os.path.dirname(__file__))
CONFIG_JSON = os.path.join(REPO_ROOT, "my-config.json")
OWN_SERVER = "129.225.207.26"
KEEP_FREE = 6
KEEP_V2RAY = 8
API_BASE = "http://127.0.0.1:9090"
TEST_URL = "https://www.google.com/generate_204"
DELAY_TIMEOUT_MS = 5000
WORKDIR = os.path.join(REPO_ROOT, "trim-work")
MIHOMO_BIN = os.path.join(REPO_ROOT, "clash", "clash-linux-amd")
MMDB_SRC = os.path.join(REPO_ROOT, "clash", "Country.mmdb")
DRY_RUN = os.environ.get("TRIM_DRY_RUN", "") == "1"
# Local-test escape hatch only: when set, the config mihomo loads gets a
# TCP DNS section (this VM blocks outbound UDP, so plain DNS resolution
# fails here). NOT set in the Actions workflow, where the runner's DNS
# works the same way the aggregator's own liveness check relies on it.
DNS_TCP = os.environ.get("TRIM_DNS_TCP", "") == "1"
MIN_FREE_MEASURED = 3  # below this, measurement is broken: patch nothing.


def log(msg):
    print(f"[trim] {msg}", flush=True)


def http_get(url, timeout=30, headers=None):
    req = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def gist_raw_url(gist_id, filename):
    return f"https://gist.githubusercontent.com/hbmysh/{gist_id}/raw/{filename}"


def github_patch_gist(gist_id, filename, content, token):
    url = f"https://api.github.com/gists/{gist_id}"
    body = json.dumps({"files": {filename: {"content": content}}}).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        method="PATCH",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "Content-Type": "application/json",
            "User-Agent": "nodepool-trim",
        },
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode("utf-8"))


def fetch_full_config(gist_id, filename):
    """Fetch the Gist file, retrying briefly in case the CDN still serves
    the previous (already trimmed) revision."""
    url = gist_raw_url(gist_id, filename)
    for attempt in range(7):
        raw = http_get(url + f"?t={int(time.time())}", timeout=60)
        cfg = yaml.safe_load(raw.decode("utf-8"))
        proxies = (cfg or {}).get("proxies", []) or []
        log(f"fetched {filename}: {len(proxies)} proxies (attempt {attempt + 1})")
        if len(proxies) > KEEP_FREE + 2 or attempt == 6:
            return cfg
        time.sleep(10)
    return cfg


def start_mihomo(config_path):
    os.makedirs(WORKDIR, exist_ok=True)
    mmdb_dst = os.path.join(WORKDIR, "Country.mmdb")
    if os.path.exists(MMDB_SRC) and not os.path.exists(mmdb_dst):
        shutil.copyfile(MMDB_SRC, mmdb_dst)
    if not os.path.exists(MIHOMO_BIN):
        raise RuntimeError(f"mihomo binary not found: {MIHOMO_BIN}")
    os.chmod(MIHOMO_BIN, 0o755)
    logfile = open(os.path.join(WORKDIR, "mihomo.log"), "wb")
    proc = subprocess.Popen(
        [MIHOMO_BIN, "-d", WORKDIR, "-f", config_path],
        stdout=logfile,
        stderr=subprocess.STDOUT,
    )
    deadline = time.time() + 60
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"mihomo exited early with code {proc.returncode}")
        try:
            http_get(API_BASE + "/version", timeout=3)
            log("mihomo is up")
            return proc, logfile
        except Exception:
            time.sleep(2)
    proc.terminate()
    raise RuntimeError("mihomo did not start within 60s")


def measure_delay(name):
    try:
        url = (
            f"{API_BASE}/proxies/{urllib.parse.quote(name, safe='')}"
            f"/delay?timeout={DELAY_TIMEOUT_MS}&url={urllib.parse.quote(TEST_URL, safe='')}"
        )
        data = json.loads(http_get(url, timeout=DELAY_TIMEOUT_MS / 1000 + 10).decode("utf-8"))
        delay = data.get("delay", -1)
        return (name, delay if isinstance(delay, int) and delay > 0 else None)
    except Exception:
        return (name, None)


def measure_all(names):
    delays = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as pool:
        futures = {pool.submit(measure_delay, n): n for n in names}
        done = 0
        for fut in concurrent.futures.as_completed(futures):
            name, delay = fut.result()
            delays[name] = delay
            done += 1
            if done % 50 == 0:
                log(f"measured {done}/{len(names)}")
    ok = sum(1 for d in delays.values() if d is not None)
    log(f"delay measurement done: {ok}/{len(names)} responded")
    return delays


def trim_config(cfg, kept_names):
    kept = set(kept_names)
    group_names = {g.get("name") for g in cfg.get("proxy-groups", [])}
    keep_entry = kept | group_names | {"DIRECT", "REJECT", "PASS"}
    by_name = {p.get("name"): p for p in cfg.get("proxies", []) if p.get("name")}
    # Order follows kept_names: own nodes first, then free by delay.
    cfg["proxies"] = [by_name[n] for n in kept_names if n in by_name]
    for group in cfg.get("proxy-groups", []):
        group["proxies"] = [p for p in group.get("proxies", []) if p in keep_entry]
    return cfg


def main():
    token = os.environ.get("PUSH_TOKEN", "")
    if not DRY_RUN and not token:
        log("FATAL: PUSH_TOKEN is not set")
        return 1

    with open(CONFIG_JSON, "r", encoding="utf-8") as f:
        site_cfg = json.load(f)
    items = site_cfg["storage"]["items"]
    clash_id = items["pool-clash"]["gist_id"]
    clash_file = items["pool-clash"]["filename"]
    v2ray_id = items["pool-v2ray"]["gist_id"]
    v2ray_file = items["pool-v2ray"]["filename"]

    cfg = fetch_full_config(clash_id, clash_file)
    proxies = cfg.get("proxies", []) or []
    if not proxies:
        log("FATAL: no proxies in fetched config")
        return 1
    names = [p.get("name") for p in proxies if p.get("name")]
    by_name = {p.get("name"): p for p in proxies if p.get("name")}

    own = [p for p in proxies if p.get("server") == OWN_SERVER]
    own_names = [p["name"] for p in own if p.get("name")]
    log(f"own nodes: {own_names}")

    # Write a copy of the config for mihomo to load (measurement only).
    # The trimming below uses the original, unmodified config.
    measure_cfg = copy.deepcopy(cfg)
    if DNS_TCP:
        measure_cfg["dns"] = {
            "enable": True,
            "nameserver": ["tcp://1.1.1.1", "tcp://8.8.8.8"],
        }
    os.makedirs(WORKDIR, exist_ok=True)
    full_path = os.path.join(WORKDIR, "config.yaml")
    with open(full_path, "w", encoding="utf-8") as f:
        yaml.dump(measure_cfg, f, allow_unicode=True)

    proc = None
    logfile = None
    try:
        proc, logfile = start_mihomo(full_path)
        # Give the url-test group a moment; per-proxy delay calls are
        # on demand anyway.
        time.sleep(5)
        delays = measure_all(names)
    finally:
        if proc is not None:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except Exception:
                proc.kill()
        if logfile is not None:
            logfile.close()

    free = [p for p in proxies if p.get("server") != OWN_SERVER and p.get("name")]
    free_sorted = sorted(
        (p for p in free if delays.get(p.get("name")) is not None),
        key=lambda p: delays[p["name"]],
    )
    if len(free_sorted) < MIN_FREE_MEASURED:
        # Measurement is broken (network/DNS/mihomo), not "all nodes dead".
        # Patch NOTHING: the full config Process just wrote stays live.
        log(
            f"FATAL: only {len(free_sorted)} free nodes measured "
            f"(need >= {MIN_FREE_MEASURED}); refusing to trim, Gist untouched"
        )
        return 1
    top_free = free_sorted[:KEEP_FREE]
    kept_names = own_names + [p["name"] for p in top_free]
    log("kept nodes (own first, then fastest free):")
    for n in own_names:
        log(f"  own: {n} delay={delays.get(n)}")
    for p in top_free:
        log(f"  free: {p['name']} delay={delays[p['name']]}ms")

    trimmed = trim_config(cfg, kept_names)
    trimmed_yaml = yaml.dump(trimmed, allow_unicode=True)
    # Prove the trimmed YAML parses back and holds exactly the kept set.
    check = yaml.safe_load(trimmed_yaml)
    check_names = [p.get("name") for p in check.get("proxies", [])]
    if sorted(check_names) != sorted(kept_names):
        log(f"FATAL: trimmed config mismatch: {check_names} != {kept_names}")
        return 1
    log(f"trimmed clash config: {len(check_names)} proxies")

    # Trim the v2ray file to its 8 fastest vmess nodes by the same delays.
    v2ray_raw = http_get(gist_raw_url(v2ray_id, v2ray_file) + f"?t={int(time.time())}", timeout=60)
    try:
        decoded = base64.b64decode(v2ray_raw).decode("utf-8", errors="ignore")
    except Exception:
        decoded = ""
    vmess = []
    for line in decoded.splitlines():
        line = line.strip()
        if not line.startswith("vmess://"):
            continue
        try:
            info = json.loads(base64.b64decode(line[8:] + "=" * (-len(line[8:]) % 4)))
            vmess.append((info.get("ps", ""), line))
        except Exception:
            continue
    vmess_sorted = sorted(
        (item for item in vmess if delays.get(item[0]) is not None),
        key=lambda item: delays[item[0]],
    )
    v2ray_kept = [line for _, line in vmess_sorted[:KEEP_V2RAY]]
    log(f"v2ray: {len(vmess)} vmess nodes -> keeping {len(v2ray_kept)} fastest")
    for name, _ in vmess_sorted[:KEEP_V2RAY]:
        log(f"  vmess: {name} delay={delays[name]}ms")
    v2ray_b64 = base64.b64encode("\n".join(v2ray_kept).encode("utf-8")).decode("utf-8")

    if DRY_RUN:
        with open(os.path.join(REPO_ROOT, "trimmed-clash.yaml"), "w", encoding="utf-8") as f:
            f.write(trimmed_yaml)
        with open(os.path.join(REPO_ROOT, "trimmed-v2ray.txt"), "w", encoding="utf-8") as f:
            f.write(v2ray_b64)
        log("DRY RUN: wrote trimmed-clash.yaml and trimmed-v2ray.txt locally, Gist untouched")
        return 0

    github_patch_gist(clash_id, clash_file, trimmed_yaml, token)
    log(f"patched gist {clash_id} / {clash_file}")
    github_patch_gist(v2ray_id, v2ray_file, v2ray_b64, token)
    log(f"patched gist {v2ray_id} / {v2ray_file}")
    log("done")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        log(f"FATAL: {exc}")
        sys.exit(1)
