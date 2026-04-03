# Agent Guidelines for datasus_lelei

This document provides guidelines for AI agents working on this codebase. **Always keep these rules updated** as the project evolves.

## Project Overview

datasus_lelei — A short description of the project.

## Tech Stack

| Component | Technology |
|-----------|------------|
| Environment | UV + Python 3.12 venv |
| Testing | pytest |
| Linting | Ruff |

## Project Structure

Note: The main package is located in `src/datasus_lelei/`.

## Code Conventions

- Python 3.12+ features (type hints, `|` union, etc.)
- Line length: 79 characters
- Use `ruff` for linting and formatting

```toml
[tool.ruff]
line-length = 79

[tool.ruff.lint]
preview = true
select = ['I', 'F', 'E', 'W', 'PL', 'PT']
ignore = ['E402', 'F811']
```

- Docstrings for public APIs (Google style)
- **Always use type hints.**

## Commands

Use `uv` for all operations to ensure environment isolation:

```bash
uv run ruff check src/ tests/           # Lint code
uv run ruff format src/ tests/          # Format code
uv run pytest tests/                    # Run tests
```

## Commit Message Convention

All commits **must** follow Conventional Commits. Validation regex:

```
^(Notes added by 'git notes add')|(Revert "Merge branch '\S+' into '\S+'")|( chore|docs|feat|fix|perf|refactor|imp|style|infra|test|breaking)(\(.*\))?!?: .*($|\n\n.*)
```

Allowed types: `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `chore`, `infra`, `imp`, `breaking`

Format: `<type>(<optional scope>): <description>`

Examples:
- `feat: adiciona modelo de classificação`
- `fix(pipeline): corrige leitura de dados raw`
- `refactor(src): simplifica feature engineering`

## Important Rules

1. **Always use the registry pattern** for new components.
2. **Always extend base classes** for consistency.
3. **Always add type hints** to function signatures.
4. **Always import from `datasus_lelei`**, not relative paths.
5. **Always follow commit message convention** described above.
