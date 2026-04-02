# 📋 Policy Gate

Organization-level agentic workflow for automated Python/PySpark code policy
enforcement across all repositories.

> **⚠️ This is a READ-ONLY system. It never modifies repository code — it only
> produces compliance reports.**

---

## What It Does

Policy Gate checks Python and PySpark code against **7 mandatory policies**
and produces a consistent, easy-to-read report — either as a PR comment
(automated) or interactively via the `@policy-reviewer` Copilot agent.

| # | Policy | What It Catches |
|:-:|--------|-----------------|
| 1 | No Global SparkSession | `SparkSession.builder.getOrCreate()` |
| 2 | No Hardcoded Secrets/Paths | File paths, passwords, emails, IPs, connection strings |
| 3 | Adequate Code Comments | Files with code-to-comment ratio > 20:1 |
| 4 | Delta Tables Only | Non-Delta formats in table writes/creates |
| 5 | No Schema Creation | `CREATE SCHEMA` / `CREATE DATABASE` |
| 6 | Meaningful File Names | `1.py`, `abc.py`, `temp.py`, etc. |
| 7 | No Local File Downloads | `urlretrieve`, `curl`, `wget`, etc. |

---

## Repository Structure

This repository is designed to be the **org-level `.github` repository**.
Files in `.github/` are automatically inherited by all repositories in the
organization.

```
.github/
├── copilot-instructions.md              # Org-wide Copilot instructions (all repos)
├── agents/
│   └── policy-reviewer.agent.md         # Custom Copilot agent (all repos)
├── instructions/
│   └── python-policies.instructions.md  # Path-specific rules for *.py files
├── prompts/
│   └── policy-report.prompt.md          # Reusable prompt for report generation
└── workflows/
    └── policy-review.yml                # Reusable GitHub Actions workflow
scripts/
└── policy_checker.py                    # Deterministic policy checker (Python)
tests/
└── fixtures/                            # Test files for validating the checker
```

---

## How It Works — Three Layers

### Layer 1: Automated PR Check (GitHub Actions)

The `policy-review.yml` workflow runs automatically on every PR that
touches Python files. It:

1. Checks out the PR code
2. Runs `policy_checker.py` (deterministic static analysis)
3. Posts a formatted report as a PR comment
4. Fails the check if any errors are found

#### Setup (Org Admin — One Time)

1. Go to **Organization → Settings → Rulesets → New Ruleset**
2. Set **Target** to "All repositories" (or a subset)
3. Add rule: **Require workflows to pass**
4. Select workflow: `.github/workflows/policy-review.yml @ main` from this repo
5. Set **Enforcement** to Active

After this, every PR in every targeted repo runs the policy check
automatically — **with zero per-repo setup**.

### Layer 2: AI-Powered Review (Copilot Custom Agent)

The `@policy-reviewer` custom agent is available in Copilot Chat across
all org repositories. To use it:

```
@policy-reviewer Review all Python files in this repository against our policies
```

The agent produces the same consistent report format as the automated check,
but with AI-powered semantic understanding that can catch nuanced issues
the static checker might miss.

### Layer 3: Reusable Prompt

For a quick policy check via Copilot Chat, use the saved prompt:

1. Open Copilot Chat in any org repo
2. Select the **Policy Review — Full Report** prompt
3. Get a comprehensive compliance report

---

## Running Locally (Ad-Hoc Scanning)

For repos not yet in the org, or for pre-release audits:

```bash
# Clone the repo you want to check
git clone https://github.com/someone/their-repo.git
cd their-repo

# Run the policy checker
python3 /path/to/policy-gate/scripts/policy_checker.py . \
  --repo "someone/their-repo" \
  --branch "main"
```

### CLI Options

```
usage: policy_checker.py [-h] [--format {markdown,json}] [--output FILE]
                         [--repo NAME] [--branch NAME] [--exit-code]
                         [target]

Options:
  target              Directory to scan (default: current directory)
  --format, -f        Output format: markdown (default) or json
  --output, -o        Write report to file instead of stdout
  --repo              Repository name for the report header
  --branch            Branch name for the report header
  --exit-code         Exit with code 1 if any errors are found
```

### Batch Scanning All Org Repos

```bash
gh repo list YOUR-ORG --json nameWithOwner --limit 100 -q '.[].nameWithOwner' | \
while read repo; do
  echo "=== Scanning $repo ==="
  dir=$(mktemp -d)
  gh repo clone "$repo" "$dir" -- --depth 1 --quiet 2>/dev/null
  python3 scripts/policy_checker.py "$dir" --repo "$repo" --format markdown
  rm -rf "$dir"
done
```

---

## Report Format

Every report — whether from the Actions workflow, the custom agent, or a
local run — uses the same consistent format:

```
📋 Policy Review Report
├── Metadata (repo, branch, date, file count)
├── Overall Result (PASS / FAIL / WARNINGS)
├── Policy Summary (table with status per policy)
├── Detailed Findings (per-policy violation tables)
└── Policy Definitions Reference
```

The report is always **read-only** — it documents findings but never
modifies any code.

---

## Customization

### Adding/Modifying Policies

1. Add the check logic to `scripts/policy_checker.py`
2. Update the agent prompt in `.github/agents/policy-reviewer.agent.md`
3. Update instructions in `.github/copilot-instructions.md`
4. Update this README

### Adjusting Comment Ratio Threshold

In `policy_checker.py`, the comment ratio threshold is set to 1:20 in
`_check_policy_3`. Change the `ratio > 20` comparison to adjust.

### Exempting Files or Directories

The checker automatically skips: `.git`, `__pycache__`, `venv`, `.venv`,
`node_modules`, `site-packages`, and `.github`. Add more directories to
`PolicyChecker.SKIP_DIRS`.
