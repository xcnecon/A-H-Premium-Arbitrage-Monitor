---
name: dev
description: Scaffold or implement a new module/feature for the A/H arbitrage monitor. Handles file creation, imports, and wiring into the existing project structure.
argument-hint: "[module or feature name]"
allowed-tools: "Read, Grep, Glob, Edit, Write, Bash"
---

## Dev Skill

Implement the feature or module described by `$ARGUMENTS`.

Before writing code:
1. Read CLAUDE.md for project structure and coding standards
2. Check existing code in `src/` to understand current patterns and avoid duplication
3. Identify where the new code fits in the project structure

When writing code:
- Follow the project's coding standards (type hints, dataclasses, logging)
- Create files in the correct `src/` subdirectory per the project structure
- Add imports to relevant `__init__.py` files
- Write a minimal test in `tests/` for any new module
- Update `requirements.txt` if new dependencies are needed

After writing:
- Run `pytest` to verify nothing is broken
- Summarize what was created and how it connects to the rest of the system
