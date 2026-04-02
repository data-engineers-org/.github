# Organization-Wide Copilot Instructions

These instructions apply to **all repositories** in this GitHub organization.
They are automatically inherited via the org-level `.github` repository.

## Data Engineering Code Policies

All Python and PySpark code in this organization must comply with the
following 7 mandatory policies. Violations will be flagged during code
review and must be resolved before merging to production branches.

### Policy 1 — No Global SparkSession
- Do NOT use `SparkSession.builder.getOrCreate()` to create a global
  Spark context.
- Instead, receive the `spark` session as a parameter, or rely on the
  Databricks-provided session.

### Policy 2 — No Hardcoded Secrets or Paths
- Do NOT hardcode file paths (`/mnt/...`, `s3://...`, `abfss://...`),
  passwords, emails, API keys, tokens, connection strings, or IP addresses.
- Use environment variables, secret scopes (`dbutils.secrets`), or
  configuration files.

### Policy 3 — Adequate Code Comments
- Include approximately 1 comment per 10-20 lines of code.
- Use docstrings for modules, classes, and functions.
- Comments should explain *why*, not just *what*.

### Policy 4 — Delta Tables Only
- All Databricks tables must be created in **Delta** format.
- Do NOT use `.format("parquet")`, `.format("csv")`, `.format("json")`,
  `.format("orc")`, or `.format("avro")` for table writes.
- Do NOT use `USING parquet/csv/json/orc/avro` in CREATE TABLE statements.

### Policy 5 — No Schema Creation
- Do NOT use `CREATE SCHEMA` or `CREATE DATABASE` in PySpark or Spark SQL.
- Schemas and databases are managed by the platform team.

### Policy 6 — Meaningful File Names
- Python files must have descriptive, meaningful names.
- Names like `1.py`, `abc.py`, `temp.py`, `untitled.py`, or `script.py`
  are not acceptable.

### Policy 7 — No Local File Downloads
- Do NOT download files to local VMs during execution.
- Do NOT use `urllib.request.urlretrieve`, `requests.get().content` with
  file writes, `subprocess` calls to `curl`/`wget`, or similar patterns.
- Use cloud storage (DBFS, S3, ADLS) instead of local filesystems.

## Code Review Instructions

When reviewing pull requests in this organization:
1. Check all changed Python files against the 7 policies above.
2. Flag any violations with a clear reference to the policy number.
3. Do NOT approve PRs that contain policy violations.
4. Provide the policy number and a brief explanation for each finding.
