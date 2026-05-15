# Open Science / Replication Package Plan

ESEM 2026 is open-by-default. The replication artifact must be available
at submission time (anonymized) and at camera-ready (deanonymized if
accepted).

## What to release

1. **Scripts**: everything under `scripts/`.
2. **Configs**: every `.conf` used in a reported result.
3. **Query lists**: TPC-DS / TPC-H query IDs included.
4. **Synthetic generators**: code for any microbenchmark dataset.
5. **Raw event logs**: per-run Spark event logs (compressed).
6. **Analysis notebooks**: Jupyter / R producing every figure and table.
7. **Environment**: Spark git commit hash, kernel version, container image
   digest, k8s cluster spec (anonymized).

## What NOT to release

- Anything that deanonymizes authors (institutional paths, internal IPs,
  usernames, cluster names hinting at the lab).
- Proprietary data. TPC-DS / TPC-H are public, so this is fine.

## Hosting at submission (anonymous)

Recommended: https://anonymous.4open.science/

- Upload a clean copy of the repo (scrubbed of author/inst strings).
- Provide the anonymous URL in the paper's Data Availability statement.

Alternative: Zenodo anonymous deposit (DOI reserved, content shown without
author names until release).

## Hosting at camera-ready

- Public GitHub repository with full history.
- Zenodo deposit with DOI for archival.

## Scrubbing checklist before anonymous upload

- [ ] No author names in commit history (consider squash to a single commit).
- [ ] No `@institution.edu` strings in any file.
- [ ] No hostnames or IPs that map to specific labs.
- [ ] `git config` of bundled .git removed.
- [ ] `research-claude/esem2026/` path renamed to neutral (e.g., `replication/`).
- [ ] No PDF, slide deck, or note file with author info.

## Data Availability statement (paper text)

Required by LIPIcs LIPIcs Author Guidelines and ESEM Open Science Policy.
Suggested wording (anonymous submission):

> All scripts, configurations, query lists, and analysis notebooks used in
> this study are available as an anonymized replication package at
> <ANONYMIZED_URL>. Raw Spark event logs and processed CSV summaries are
> included. The cluster environment is documented as part of the package.
