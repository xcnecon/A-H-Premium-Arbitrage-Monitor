# Contributing

## Development Setup

1. Clone the repository and create a virtual environment:

   ```bash
   git clone https://github.com/<your-fork>/ah-arb.git
   cd ah-arb
   python -m venv .venv
   source .venv/bin/activate  # Windows: .venv\Scripts\activate
   ```

2. Install the package in editable mode with dev dependencies:

   ```bash
   pip install -e ".[dev]"
   ```

3. Install pre-commit hooks:

   ```bash
   pre-commit install
   ```

## Code Style

- **Type hints** are required on all function signatures.
- Use the **`logging` module** only. Do not use `print()`.
- Logger calls must use **`%s` formatting**, not f-strings:
  ```python
  logger.info("Fetched %s rows for %s", count, code)  # correct
  logger.info(f"Fetched {count} rows for {code}")      # wrong
  ```
- All premium and ratio calculations must use **unadjusted prices** (no qfq/hfq adjustment).
- Use `pathlib.Path` for file paths. Never hardcode OS-specific separators.

## Running Tests

```bash
pytest
```

## Pull Request Process

1. Fork the repository and create a feature branch from `main`.
2. Make your changes, keeping commits focused and well-described.
3. Ensure all tests pass (`pytest`) and code is clean (`ruff check .` and `ruff format --check .`).
4. Submit a pull request against `main`. Describe the motivation and summarize the changes.

## Reporting Issues

Use [GitHub Issues](../../issues) to report bugs or request features. Include steps to reproduce, expected behavior, and actual behavior.
