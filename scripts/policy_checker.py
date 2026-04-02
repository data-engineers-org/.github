#!/usr/bin/env python3
"""
Policy Gate — Automated Code Policy Checker
============================================

Scans Python/PySpark code against organizational policies and produces
a structured, consistently formatted report.

⚠️  READ-ONLY: This script NEVER modifies any files.

Policies Checked:
  1. No global SparkSession via SparkSession.builder.getOrCreate()
  2. No hardcoded file paths, passwords, emails, or secrets
  3. Reasonable code commenting (~1 comment per 10-20 lines)
  4. Databricks tables must use Delta format only
  5. No schema/database creation in PySpark or Spark SQL
  6. Python filenames must be meaningful
  7. No downloading files to local VMs during execution
"""

import os
import re
import sys
import json
from pathlib import Path
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple
from collections import defaultdict


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class Violation:
    policy_id: int
    policy_name: str
    file_path: str
    line_number: int
    line_content: str
    description: str
    severity: str  # "error" or "warning"


@dataclass
class PolicyResult:
    policy_id: int
    policy_name: str
    status: str = "pass"  # "pass", "fail", "warning"
    violations: List[Violation] = field(default_factory=list)
    files_checked: int = 0


# ---------------------------------------------------------------------------
# Policy Checker
# ---------------------------------------------------------------------------

class PolicyChecker:
    """Checks Python/PySpark code against organizational policies."""

    POLICIES = {
        1: "No Global SparkSession",
        2: "No Hardcoded Secrets/Paths",
        3: "Adequate Code Comments",
        4: "Delta Tables Only",
        5: "No Schema Creation",
        6: "Meaningful File Names",
        7: "No Local File Downloads",
    }

    # Directories to always skip
    SKIP_DIRS = {
        ".git", "__pycache__", ".venv", "venv", "env", ".env",
        "node_modules", ".tox", ".mypy_cache", ".pytest_cache",
        "site-packages", ".policy-gate", ".github",
    }

    # Filenames exempt from the naming policy
    EXEMPT_FILENAMES = {
        "__init__", "__main__", "setup", "conftest", "manage",
        "wsgi", "asgi", "fabfile", "tasks", "celery",
    }

    def __init__(self, target_dir: str):
        self.target_dir = Path(target_dir).resolve()
        self.python_files: List[Path] = []
        self.results: Dict[int, PolicyResult] = {}
        self._discover_files()

    # -- file discovery ------------------------------------------------------

    def _discover_files(self):
        """Find all Python files, skipping irrelevant directories."""
        self.python_files = sorted(
            p for p in self.target_dir.rglob("*.py")
            if not any(
                part in self.SKIP_DIRS
                for part in p.relative_to(self.target_dir).parts
            )
        )

    # -- run all checks ------------------------------------------------------

    def run_all_checks(self) -> Dict[int, PolicyResult]:
        for pid, name in self.POLICIES.items():
            self.results[pid] = PolicyResult(
                policy_id=pid,
                policy_name=name,
                files_checked=len(self.python_files),
            )

        for py_file in self.python_files:
            try:
                content = py_file.read_text(encoding="utf-8", errors="replace")
                lines = content.splitlines()
                rel = str(py_file.relative_to(self.target_dir))

                self._check_policy_1(rel, lines)
                self._check_policy_2(rel, lines)
                self._check_policy_3(rel, lines)
                self._check_policy_4(rel, lines)
                self._check_policy_5(rel, lines)
                self._check_policy_6(rel, py_file)
                self._check_policy_7(rel, lines)
            except Exception as exc:
                print(f"⚠ Could not process {py_file}: {exc}", file=sys.stderr)

        # Derive final status per policy
        for r in self.results.values():
            errors = [v for v in r.violations if v.severity == "error"]
            warnings = [v for v in r.violations if v.severity == "warning"]
            if errors:
                r.status = "fail"
            elif warnings:
                r.status = "warning"
            else:
                r.status = "pass"

        return self.results

    # -- individual policy checks --------------------------------------------

    def _add(self, pid: int, fpath: str, lineno: int,
             content: str, desc: str, sev: str = "error"):
        self.results[pid].violations.append(Violation(
            policy_id=pid,
            policy_name=self.POLICIES[pid],
            file_path=fpath,
            line_number=lineno,
            line_content=content[:150],
            description=desc,
            severity=sev,
        ))

    # Policy 1 — No SparkSession.builder.getOrCreate() ----------------------

    def _check_policy_1(self, fpath: str, lines: List[str]):
        # Single-line detection
        pat = re.compile(
            r"SparkSession\s*\.\s*builder.*\.getOrCreate\s*\(", re.IGNORECASE
        )
        for i, line in enumerate(lines, 1):
            if line.strip().startswith("#"):
                continue
            if pat.search(line):
                self._add(1, fpath, i, line.strip(),
                          "Global SparkSession created via "
                          "SparkSession.builder.getOrCreate()")

        # Multi-line detection
        full = "\n".join(lines)
        for m in pat.finditer(full):
            lineno = full[: m.start()].count("\n") + 1
            already = any(
                v.line_number == lineno and v.file_path == fpath
                for v in self.results[1].violations
            )
            if not already:
                snip = lines[lineno - 1].strip() if lineno <= len(lines) else ""
                self._add(1, fpath, lineno, snip,
                          "Global SparkSession created via "
                          "SparkSession.builder...getOrCreate() (multi-line)")

    # Policy 2 — No hardcoded secrets / paths --------------------------------

    _P2_PATTERNS: List[Tuple[str, str]] = [
        # Absolute / cloud file paths
        (r"""['\"]\/(?:mnt|dbfs|tmp|data|home|usr|opt|var|etc)\/[^'\"]+['\"]""",
         "Hardcoded file path"),
        (r"""['\"][A-Z]:\\[^'\"]+['\"]""",
         "Hardcoded Windows file path"),
        (r"""['\"]s3[a]?:\/\/[^'\"]+['\"]""",
         "Hardcoded S3 path"),
        (r"""['\"]abfss?:\/\/[^'\"]+['\"]""",
         "Hardcoded Azure storage path"),
        (r"""['\"]gs:\/\/[^'\"]+['\"]""",
         "Hardcoded GCS path"),
        (r"""['\"]wasbs?:\/\/[^'\"]+['\"]""",
         "Hardcoded Azure Blob path"),
        # Passwords / secrets
        (r"""(?:password|passwd|pwd|secret|api_key|apikey|api_secret|"""
         r"""access_key|private_key|token)\s*=\s*['\"][^'\"]{3,}['\"]""",
         "Hardcoded password/secret"),
        # Emails
        (r"""['\"][a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}['\"]""",
         "Hardcoded email address"),
        # Connection strings
        (r"""['\"](?:jdbc|mongodb(\+srv)?|postgresql|mysql|redis|amqp)"""
         r""":\/\/[^'\"]+['\"]""",
         "Hardcoded connection string"),
        # IP addresses
        (r"""['\"](?:\d{1,3}\.){3}\d{1,3}(?::\d+)?['\"]""",
         "Hardcoded IP address"),
    ]

    def _check_policy_2(self, fpath: str, lines: List[str]):
        compiled = [(re.compile(p, re.IGNORECASE), d) for p, d in self._P2_PATTERNS]
        in_docstring = False

        for i, line in enumerate(lines, 1):
            stripped = line.strip()

            # Rough docstring tracking
            if '"""' in stripped or "'''" in stripped:
                count = stripped.count('"""') + stripped.count("'''")
                if count % 2 == 1:
                    in_docstring = not in_docstring
                continue
            if in_docstring:
                continue
            if stripped.startswith("#"):
                continue

            # Check only the code portion (before inline comment)
            code_part = line.split("#")[0] if "#" in line else line

            for pat, desc in compiled:
                if pat.search(code_part):
                    self._add(2, fpath, i, stripped, desc)
                    break  # one finding per line

    # Policy 3 — Adequate comments -------------------------------------------

    def _check_policy_3(self, fpath: str, lines: List[str]):
        code_lines = 0
        comment_lines = 0
        in_docstring = False

        for line in lines:
            stripped = line.strip()
            if not stripped:
                continue

            # Docstring tracking
            if not in_docstring:
                for marker in ('"""', "'''"):
                    if stripped.startswith(marker):
                        if stripped.count(marker) >= 2 and len(stripped) > len(marker):
                            comment_lines += 1
                        else:
                            in_docstring = True
                            comment_lines += 1
                        break
                else:
                    if stripped.startswith("#"):
                        comment_lines += 1
                    else:
                        code_lines += 1
                        if " #" in stripped or "\t#" in stripped:
                            comment_lines += 1
            else:
                comment_lines += 1
                if '"""' in stripped or "'''" in stripped:
                    in_docstring = False

        # Only flag files with meaningful code
        if code_lines <= 10:
            return

        if comment_lines == 0:
            self._add(
                3, fpath, 0, "",
                f"No comments found in file with {code_lines} code lines "
                f"(expected ≥1 comment per 20 lines)",
                sev="error",
            )
        else:
            ratio = code_lines / comment_lines
            if ratio > 20:
                self._add(
                    3, fpath, 0, "",
                    f"Insufficient comments: {comment_lines} comment(s) for "
                    f"{code_lines} code lines (ratio 1:{ratio:.0f}, "
                    f"expected ≤1:20)",
                    sev="warning",
                )

    # Policy 4 — Delta tables only -------------------------------------------

    _P4_PATTERNS: List[Tuple[str, str]] = [
        (r"""\.format\s*\(\s*['\"](?:parquet|csv|json|orc|avro|text)['\"]""",
         "Non-Delta write format specified"),
        (r"""CREATE\s+(?:EXTERNAL\s+)?TABLE\s+.*?\s+USING\s+"""
         r"""(?:parquet|csv|json|orc|avro|text)""",
         "CREATE TABLE using non-Delta format"),
        (r"""STORED\s+AS\s+(?:PARQUET|CSV|JSON|ORC|AVRO|TEXTFILE)""",
         "Table stored as non-Delta format"),
    ]

    def _check_policy_4(self, fpath: str, lines: List[str]):
        compiled = [(re.compile(p, re.IGNORECASE), d) for p, d in self._P4_PATTERNS]
        for i, line in enumerate(lines, 1):
            if line.strip().startswith("#"):
                continue
            for pat, desc in compiled:
                if pat.search(line):
                    self._add(4, fpath, i, line.strip(), desc)
                    break

    # Policy 5 — No schema creation ------------------------------------------

    _P5_PATTERNS: List[Tuple[str, str]] = [
        (r"""CREATE\s+(?:SCHEMA|DATABASE)""",
         "Schema/database creation statement"),
        (r"""spark\.sql\s*\(\s*['\"].*?CREATE\s+(?:SCHEMA|DATABASE)""",
         "Schema/database creation via spark.sql()"),
        (r"""\.createDatabase\s*\(""",
         "createDatabase() API call"),
    ]

    def _check_policy_5(self, fpath: str, lines: List[str]):
        compiled = [(re.compile(p, re.IGNORECASE), d) for p, d in self._P5_PATTERNS]
        for i, line in enumerate(lines, 1):
            if line.strip().startswith("#"):
                continue
            for pat, desc in compiled:
                if pat.search(line):
                    self._add(5, fpath, i, line.strip(), desc)
                    break

    # Policy 6 — Meaningful filenames ----------------------------------------

    _BAD_NAMES = re.compile(
        r"^(?:[a-z]|[a-z]{2,3}|\d+|test|temp|tmp|foo|bar|baz|xxx|zzz|"
        r"untitled|new|script|code|file|my_?file|my_?script|"
        r"notebook|nb|note|draft|scratch|playground|sandbox)$",
        re.IGNORECASE,
    )

    def _check_policy_6(self, fpath: str, py_file: Path):
        stem = py_file.stem
        if stem in self.EXEMPT_FILENAMES:
            return
        if self._BAD_NAMES.match(stem):
            self._add(
                6, fpath, 0, py_file.name,
                f"Non-descriptive filename: '{py_file.name}'",
            )

    # Policy 7 — No local file downloads ------------------------------------

    _P7_PATTERNS: List[Tuple[str, str]] = [
        (r"""urllib\.request\.urlretrieve""",
         "urllib file download to local VM"),
        (r"""urllib\.request\.urlopen""",
         "urllib URL open (potential local download)"),
        (r"""requests\.get\s*\(.*(?:stream|content)""",
         "HTTP download to local VM"),
        (r"""wget\s*\(""",
         "wget download to local VM"),
        (r"""subprocess.*(?:curl|wget)""",
         "subprocess curl/wget download"),
        (r"""os\.system\s*\(.*(?:curl|wget)""",
         "os.system curl/wget download"),
        (r"""shutil\.copyfileobj""",
         "shutil file copy (potential download)"),
        (r"""dbutils\.fs\.cp\s*\(.*['\"]file:/""",
         "Copying to local filesystem via dbutils"),
        (r"""gdown\.download""",
         "Google Drive download to local VM"),
        (r"""tf\.keras\.utils\.get_file""",
         "Keras file download to local VM"),
        (r"""torch\.hub\.download""",
         "PyTorch hub download to local VM"),
    ]

    def _check_policy_7(self, fpath: str, lines: List[str]):
        compiled = [(re.compile(p, re.IGNORECASE), d) for p, d in self._P7_PATTERNS]
        for i, line in enumerate(lines, 1):
            if line.strip().startswith("#"):
                continue
            for pat, desc in compiled:
                if pat.search(line):
                    self._add(7, fpath, i, line.strip(), desc)
                    break

    # -----------------------------------------------------------------------
    # Report generation
    # -----------------------------------------------------------------------

    def generate_report(self, repo_name: str = "",
                        branch: str = "") -> str:
        """Produce a consistently formatted Markdown report."""
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        total_errors = sum(
            len([v for v in r.violations if v.severity == "error"])
            for r in self.results.values()
        )
        total_warnings = sum(
            len([v for v in r.violations if v.severity == "warning"])
            for r in self.results.values()
        )
        has_errors = any(r.status == "fail" for r in self.results.values())
        all_pass = all(r.status == "pass" for r in self.results.values())

        lines: List[str] = []
        a = lines.append

        # -- header ----------------------------------------------------------
        a("# 📋 Policy Review Report")
        a("")
        a("> **⚠️ This is a READ-ONLY report. No files were modified.**")
        a("")

        # -- metadata --------------------------------------------------------
        a("| Field | Value |")
        a("|-------|-------|")
        if repo_name:
            a(f"| **Repository** | `{repo_name}` |")
        if branch:
            a(f"| **Branch** | `{branch}` |")
        a(f"| **Scan Date** | {now} |")
        a(f"| **Files Scanned** | {len(self.python_files)} Python file(s) |")
        a("")

        # -- overall status --------------------------------------------------
        if all_pass:
            a("## ✅ Overall Result: **PASS**")
        elif has_errors:
            a("## ❌ Overall Result: **FAIL**")
        else:
            a("## ⚠️ Overall Result: **WARNINGS**")
        a("")
        a(f"> {total_errors} error(s), {total_warnings} warning(s) "
          f"across {len(self.python_files)} file(s)")
        a("")

        # -- summary table ---------------------------------------------------
        a("---")
        a("")
        a("## Policy Summary")
        a("")
        a("| # | Policy | Status | Errors | Warnings |")
        a("|:-:|--------|:------:|:------:|:--------:|")

        icons = {"pass": "✅ Pass", "fail": "❌ Fail", "warning": "⚠️ Warn"}
        for pid in sorted(self.results):
            r = self.results[pid]
            errs = len([v for v in r.violations if v.severity == "error"])
            warns = len([v for v in r.violations if v.severity == "warning"])
            a(f"| {pid} | {r.policy_name} | {icons[r.status]} | "
              f"{errs} | {warns} |")
        a("")

        # -- detailed findings -----------------------------------------------
        a("---")
        a("")
        a("## Detailed Findings")

        for pid in sorted(self.results):
            r = self.results[pid]
            icon = {"pass": "✅", "fail": "❌", "warning": "⚠️"}[r.status]
            a("")
            a(f"### Policy {pid}: {r.policy_name} {icon}")
            a("")

            if not r.violations:
                a("> No violations found. ✔️")
                a("")
                continue

            by_file: Dict[str, List[Violation]] = defaultdict(list)
            for v in r.violations:
                by_file[v.file_path].append(v)

            a(f"> **{len(r.violations)} violation(s)** in "
              f"**{len(by_file)} file(s)**")
            a("")
            a("| Sev | File | Line | Finding |")
            a("|:---:|------|:----:|---------|")

            for fp in sorted(by_file):
                for v in sorted(by_file[fp], key=lambda x: x.line_number):
                    sev = "🔴" if v.severity == "error" else "🟡"
                    ln = str(v.line_number) if v.line_number > 0 else "—"
                    desc = v.description.replace("|", "\\|")
                    a(f"| {sev} | `{v.file_path}` | {ln} | {desc} |")

            a("")

        # -- policy reference ------------------------------------------------
        a("---")
        a("")
        a("## 📖 Policy Definitions Reference")
        a("")
        a("| # | Policy | Rule |")
        a("|:-:|--------|------|")
        a("| 1 | No Global SparkSession | Must not use "
          "`SparkSession.builder.getOrCreate()` |")
        a("| 2 | No Hardcoded Secrets/Paths | Must not hardcode file paths, "
          "passwords, emails, IPs, or connection strings |")
        a("| 3 | Adequate Code Comments | ~1 comment per 10-20 lines of code |")
        a("| 4 | Delta Tables Only | All Databricks tables must use Delta "
          "format |")
        a("| 5 | No Schema Creation | Must not CREATE SCHEMA or "
          "CREATE DATABASE |")
        a("| 6 | Meaningful File Names | No single-char, numeric, or generic "
          "filenames |")
        a("| 7 | No Local File Downloads | Must not download files to local "
          "VM |")
        a("")
        a("---")
        a(f"*Report generated by Policy Gate • {now}*")

        return "\n".join(lines)

    def generate_json(self) -> str:
        """Produce JSON output for programmatic consumption."""
        output = {
            "scan_date": datetime.now(timezone.utc).isoformat(),
            "files_scanned": len(self.python_files),
            "overall_status": (
                "pass" if all(r.status == "pass" for r in self.results.values())
                else ("warning" if not any(r.status == "fail" for r in self.results.values())
                      else "fail")
            ),
            "total_errors": sum(
                len([v for v in r.violations if v.severity == "error"])
                for r in self.results.values()
            ),
            "total_warnings": sum(
                len([v for v in r.violations if v.severity == "warning"])
                for r in self.results.values()
            ),
            "policies": {},
        }
        for pid, r in sorted(self.results.items()):
            output["policies"][str(pid)] = {
                "name": r.policy_name,
                "status": r.status,
                "violations": [
                    {
                        "file": v.file_path,
                        "line": v.line_number,
                        "severity": v.severity,
                        "description": v.description,
                        "content": v.line_content,
                    }
                    for v in r.violations
                ],
            }
        return json.dumps(output, indent=2)


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------

def main():
    import argparse

    parser = argparse.ArgumentParser(
        description=(
            "Policy Gate — Scan Python/PySpark code for policy violations.\n"
            "⚠️  READ-ONLY: This tool never modifies any files."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "target", nargs="?", default=".",
        help="Directory to scan (default: current directory)",
    )
    parser.add_argument(
        "--format", "-f", choices=["markdown", "json"], default="markdown",
        help="Output format (default: markdown)",
    )
    parser.add_argument(
        "--output", "-o",
        help="Write report to file instead of stdout",
    )
    parser.add_argument("--repo", default="", help="Repository name for the report header")
    parser.add_argument("--branch", default="", help="Branch name for the report header")
    parser.add_argument(
        "--exit-code", action="store_true",
        help="Exit with code 1 if any errors are found",
    )
    args = parser.parse_args()

    checker = PolicyChecker(args.target)
    checker.run_all_checks()

    report = (
        checker.generate_json()
        if args.format == "json"
        else checker.generate_report(repo_name=args.repo, branch=args.branch)
    )

    if args.output:
        Path(args.output).write_text(report, encoding="utf-8")
        print(f"📋 Report written to {args.output}")
    else:
        print(report)

    if args.exit_code:
        has_errors = any(r.status == "fail" for r in checker.results.values())
        sys.exit(1 if has_errors else 0)


if __name__ == "__main__":
    main()
