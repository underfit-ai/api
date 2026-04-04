# Project

Underfit is an open-source model reporting dashboard for tracking experiments, metrics, and artifacts. It serves a similar role to Weights & Biases or Tensorboard, with a focus on transparent, self-hostable reporting. This repository contains the backend API, written in python + fastapi.

# Contributing

## How to contribute

- Keep code clean and concise. Do not add comments unless the logic is non-obvious.
- Avoid splitting statements across multiple lines without a readability benefit. If it fits under 120 characters, keep it on one line.
- Prefer minimal, clear abstractions over clever ones.
- Use absolute imports only, no relative imports.
- Tests are located in a top-level `tests` folder.
- Prefer behavior-focused tests that cover a related flow end-to-end over multiple tiny tests with repeated setup.
- Avoid obvious intermediates and redundant test assertions for schema shape.
- After every refactor, remove or merge tests that no longer cover unique behavior.

## Committing changes

- Always run the linter, type checker, and tests before committing.
- Run the linter with `ruff check .`, the typechecker with `ty check .`, and the tests with `pytest .`.
- Use single-line commit messages in plain English.
- Do not use conventional commit prefixes or add signatures (e.g. Co-Authored By)
- Run `git add` and `git commit` sequentially (or in one chained command), not in parallel, to avoid `.git/index.lock` conflicts.
