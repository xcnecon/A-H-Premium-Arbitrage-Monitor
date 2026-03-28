---
name: test-monitor
description: Run a quick smoke test of the monitor's data pipeline — verify Futu OpenD connectivity, AKShare data fetch, premium calculation, and alert thresholds.
disable-model-invocation: true
allowed-tools: "Bash, Read"
---

## Test Monitor Skill

Run a diagnostic check of the A/H monitor stack:

1. **OpenD connectivity**: Check if Futu OpenD is running on localhost:11111
2. **H-share data**: Attempt to subscribe and fetch a quote for a known H-share (e.g., HK.00939)
3. **A-share data**: Fetch A-share data via Sina/Tencent HTTP (do NOT use `*_em()` endpoints — they are blocked from this network)
4. **FX rate**: Fetch CNH/HKD rate via Yahoo Finance v8 chart API (`HKDCNH=X`), fallback to AKShare `fx_spot_quote()`
5. **Premium calc**: Compute premium for at least one A/H pair and verify the formula
6. **SQLite**: Verify database connectivity and schema

Report results as a pass/fail checklist with error details for any failures.
