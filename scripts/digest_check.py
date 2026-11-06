#!/usr/bin/env python3
"""Inventory the sources of a context digest and check a digest against them.

    digest_check.py inventory <repo> [--env-prefix PREFIX]
    digest_check.py check <repo> <digest> [--budget TOKENS] [--env-prefix PREFIX]

The env-var prefix (e.g. REPLAY_) names the tunables the digest must list. Without
--env-prefix it is read from docs/agents/domain.md, from a line such as
"Env var prefix: `REPLAY_`"; with neither, tunables are not inventoried.

Stdlib only. Token counts use tiktoken (o200k_base) when importable, else chars/3.8.
"""
import hashlib
import os
import re
import sys

SOURCES = ["AGENTS.md", "CONTEXT.md", "README.md"]
ADR_DIR = "docs/adr"
AGENTS_DIR = "docs/agents"
DIGEST_NAME = "context-digest.md"
PREFIX_RE = re.compile(r"[Ee]nv(?:ironment)? var(?:iable)? prefix:\s*`?([A-Z][A-Z0-9]*_)`?")
H2_RE = re.compile(r"^## (.+?)\s*$", re.M)
TERM_RE = re.compile(r"^### (.+?)\s*$", re.M)
LINK_RE = re.compile(r"\]\(([^)#\s]+)(#[^)\s]*)?\)")


def env_re(repo, prefix=None):
    """Regex for the repo's tunables, or None when no prefix is configured."""
    if prefix is None:
        domain = os.path.join(repo, AGENTS_DIR, "domain.md")
        if os.path.isfile(domain):
            m = PREFIX_RE.search(read(domain))
            prefix = m.group(1) if m else None
    if not prefix:
        return None
    return re.compile(r"\b" + re.escape(prefix) + r"[A-Z0-9_]+\b")


def read(path):
    with open(path, encoding="utf-8") as f:
        return f.read()


def tokens(text):
    try:
        import tiktoken  # type: ignore

        return len(tiktoken.get_encoding("o200k_base").encode(text, disallowed_special=()))
    except Exception:
        return int(len(text) / 3.8)


def source_files(repo):
    files = [os.path.join(repo, s) for s in SOURCES]
    adr = os.path.join(repo, ADR_DIR)
    files += sorted(os.path.join(adr, f) for f in os.listdir(adr) if f.endswith(".md"))
    agents = os.path.join(repo, AGENTS_DIR)
    if os.path.isdir(agents):
        files += sorted(os.path.join(agents, f) for f in os.listdir(agents)
                        if f.endswith(".md") and f != DIGEST_NAME)
    return files


def fingerprint(repo):
    h = hashlib.sha256()
    for p in source_files(repo):
        h.update(p.replace(repo, "").encode())
        h.update(read(p).encode())
    return h.hexdigest()[:16]


def inventory(repo, prefix=None):
    inv = {"fingerprint": fingerprint(repo), "rules": [], "terms": [], "adrs": [], "readme": [], "env": set(), "agents": []}
    inv["rules"] = H2_RE.findall(read(os.path.join(repo, "AGENTS.md")))
    ctx = read(os.path.join(repo, "CONTEXT.md"))
    inv["terms"] = TERM_RE.findall(ctx)
    readme = read(os.path.join(repo, "README.md"))
    inv["readme"] = H2_RE.findall(readme)
    env = env_re(repo, prefix)
    inv["env"] = sorted(set(env.findall(readme)) | set(env.findall(ctx))) if env else []
    adr = os.path.join(repo, ADR_DIR)
    for f in sorted(os.listdir(adr)):
        if f.endswith(".md"):
            inv["adrs"].append(f)
    agents = os.path.join(repo, AGENTS_DIR)
    if os.path.isdir(agents):
        inv["agents"] = sorted(f for f in os.listdir(agents) if f.endswith(".md") and f != DIGEST_NAME)
    return inv


def print_inventory(inv):
    print(f"fingerprint {inv['fingerprint']}")
    for key in ("rules", "terms", "adrs", "readme", "env", "agents"):
        print(f"\n[{key}] {len(inv[key])}")
        for item in inv[key]:
            print(f"  {item}")


def norm(s):
    return re.sub(r"[`*]", "", s).strip().lower()


def check(repo, digest_path, budget, prefix=None):
    inv = inventory(repo, prefix)
    digest = read(digest_path)
    low = norm(digest)
    failures = []

    m = re.search(r"fingerprint ([0-9a-f]{16})", digest)
    if not m:
        failures.append("header: no fingerprint line")
    elif m.group(1) != inv["fingerprint"]:
        failures.append(f"stale: digest fingerprint {m.group(1)} != sources {inv['fingerprint']}")

    for rule in inv["rules"]:
        if norm(rule) not in low:
            failures.append(f"rule missing: {rule}")
    for term in inv["terms"]:
        if norm(term) not in low:
            failures.append(f"glossary term missing: {term}")
    for adr in inv["adrs"]:
        num = adr[:4]
        if f"adr-{num}" not in low and adr.lower() not in low:
            failures.append(f"ADR missing: {adr}")
    for h in inv["readme"]:
        if norm(h) not in low:
            failures.append(f"README section missing: {h}")
    for env in inv["env"]:
        if env.lower() not in low:
            failures.append(f"env var missing: {env}")
    for a in inv["agents"]:
        if a.lower() not in low:
            failures.append(f"docs/agents file missing: {a}")

    base = os.path.dirname(digest_path)
    for path, anchor in LINK_RE.findall(digest):
        if path.startswith("http"):
            continue
        target = os.path.normpath(os.path.join(base, path))
        if not os.path.exists(target):
            failures.append(f"broken link: {path}")
        elif anchor and target.endswith(".md"):
            text = read(target)
            slug = anchor[1:]
            slugs = {heading_slug(h) for h in re.findall(r"^#{1,6} (.+?)\s*$", text, re.M)}
            if slug not in slugs:
                failures.append(f"broken anchor: {path}{anchor}")

    n = tokens(digest)
    if n > budget:
        failures.append(f"over budget: {n} tokens > {budget}")

    print(f"digest: {len(digest)} chars, {n} tokens (budget {budget})")
    print(f"inventory: {len(inv['rules'])} rules, {len(inv['terms'])} terms, {len(inv['adrs'])} ADRs, "
          f"{len(inv['readme'])} README H2, {len(inv['env'])} env vars, {len(inv['agents'])} docs/agents")
    if failures:
        print(f"\nFAIL ({len(failures)})")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("\nOK: every inventory item is present, every pointer resolves, fingerprint matches, within budget")
    return 0


def heading_slug(h):
    h = re.sub(r"[`*]", "", h).strip().lower()
    h = re.sub(r"[^\w\s-]", "", h)
    return re.sub(r"\s+", "-", h)


def main(argv):
    prefix = argv[argv.index("--env-prefix") + 1] if "--env-prefix" in argv else None
    if len(argv) >= 3 and argv[1] == "inventory":
        print_inventory(inventory(os.path.abspath(argv[2]), prefix))
        return 0
    if len(argv) >= 4 and argv[1] == "check":
        budget = 8000
        if "--budget" in argv:
            budget = int(argv[argv.index("--budget") + 1])
        return check(os.path.abspath(argv[2]), os.path.abspath(argv[3]), budget, prefix)
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
