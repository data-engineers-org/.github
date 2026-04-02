---
applyTo: "**/*.py"
---

# Python & PySpark Code Policies

When reviewing or working with Python files in this organization,
always enforce these policies:

1. **No Global SparkSession** — Never use
   `SparkSession.builder.getOrCreate()`. Receive `spark` as a parameter
   or use the Databricks-provided session.

2. **No Hardcoded Values** — Never hardcode file paths (`/mnt/...`,
   `s3://...`), passwords, emails, API keys, tokens, connection strings,
   or IP addresses. Use `dbutils.secrets`, environment variables, or
   config files.

3. **Comment Your Code** — Maintain ~1 comment per 10-20 lines. Use
   docstrings for modules, classes, and functions. Explain *why*, not
   just *what*.

4. **Delta Format Only** — When creating Databricks tables, always use
   Delta format. Never use `.format("parquet")`, `.format("csv")`, etc.

5. **No Schema Creation** — Do not use `CREATE SCHEMA` or
   `CREATE DATABASE`. Schema management is handled by the platform team.

6. **Meaningful File Names** — Name files descriptively. Never use names
   like `1.py`, `abc.py`, `temp.py`, `script.py`, `untitled.py`.

7. **No Local Downloads** — Do not download files to local VMs. Avoid
   `urllib.request.urlretrieve`, `requests.get` with file writes,
   `subprocess` with `curl`/`wget`, etc. Use cloud storage instead.
