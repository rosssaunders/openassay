# Plan: PG18 Regression Tests in CI Pipeline

**Status:** Waiting for Phase 1 cascade breaker fixes to land  
**Execute with:** Codex CLI (codex 5-3) or Claude Code  
**After:** Phase 1 completes and new baseline score is established

## Prompt for Codex

```
You are working on postrust at ~/code/rosssaunders/postrust — an async PostgreSQL-compatible SQL engine in Rust.

Read COMPATIBILITY.md for the current PG18 regression test results.

Your task: Add the PG18 regression compatibility suite to the CI pipeline.

REQUIREMENTS:

1. Add a new job `pg-compat` to .github/workflows/ci.yml that runs IN PARALLEL with the existing `rust` job (not dependent on it).

2. The job should:
   a. Checkout the repo
   b. Install Rust stable toolchain
   c. Cache cargo artifacts
   d. Build pg_server in RELEASE mode: `cargo build --release --bin pg_server`
   e. Install postgresql-client: `sudo apt-get install -y postgresql-client`
   f. Start pg_server on port 55433 in background
   g. Wait for it to be ready (poll with psql SELECT 1)
   h. Run each .sql file in tests/regression/pg_compat/sql/ via psql, counting ERROR lines
   i. Calculate per-file and overall pass rates
   j. Print a results table to the job summary (using $GITHUB_STEP_SUMMARY)
   k. Compare overall score against threshold in tests/regression/pg_compat/THRESHOLD
   l. FAIL the job if score drops below threshold (prevents regressions)
   m. Upload detailed results as a GitHub Actions artifact
   n. Clean up pg_server process

3. Create the test runner script at scripts/compat/ci_regression.sh that:
   - Accepts PG_SERVER_PORT as env var (default 55433)
   - Runs all .sql files, captures output, counts errors per file
   - Outputs JSON results to a file
   - Outputs markdown summary table
   - Exits with code 1 if score < THRESHOLD
   - Handles pg_server crashes gracefully (restart and continue)

4. Create tests/regression/pg_compat/THRESHOLD containing just the minimum pass percentage (integer). Set it to the CURRENT score from COMPATIBILITY.md (23 for now — we'll ratchet it up after fixes land).

5. Add a comment in ci.yml explaining: "Ratchet: bump THRESHOLD after landing compatibility fixes. Never lower it."

6. Run `cargo test` to make sure nothing breaks.

7. Commit with message: "ci: add PG18 regression compatibility gate with ratcheting threshold"

When completely finished, run:
openclaw system event --text "Done: CI regression pipeline added with ratcheting threshold" --mode now
```

## Notes

- Start threshold at whatever the new baseline is AFTER Phase 1 lands
- Ratchet up the THRESHOLD file with each batch of fixes
- The `pg-compat` job should NOT block merging initially (use `continue-on-error: true` at first, then make it required once stable)
- Consider adding a PR comment bot that shows the score diff (future enhancement)
