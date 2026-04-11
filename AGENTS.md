# Project

Underfit is an open-source model reporting dashboard for tracking experiments, metrics, and artifacts. It serves a similar role to Weights & Biases or Tensorboard, with a focus on transparent, self-hostable reporting. This repository contains the backend API, written in python + fastapi.

# Hosting modes

Underfit is designed to run in two different modes, and some storage/API choices are intentional because of that split.

- Hosted/API mode: Underfit behaves like a normal experiment tracking service. Clients write data through the API, and the database is the source of truth for run/project/artifact metadata.
- Local/backfill mode: Underfit behaves more like TensorBoard. The client SDK writes files directly to a logdir, then runs the API with `auth_enabled = false` and `backfill.enabled = true` so the server watches that storage and backfills the database from files.
- Do not assume these two modes use the same storage contract. Backfill reads an external on-disk format produced for local viewing, and the normal API endpoints are not expected to write every metadata file that backfill consumes.
- When reviewing or changing storage code, preserve this distinction. A mismatch between API-written storage and backfill-only files is not automatically a bug; first ask which mode owns that path and what the source of truth is supposed to be.

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
