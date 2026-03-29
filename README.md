# Underfit API

Backend API for [Underfit](https://github.com/underfit-ai), an open-source model reporting dashboard for tracking experiments, metrics, and artifacts. Built with Python and FastAPI.

## Quickstart

```bash
pip install underfit-api
```

Run the server:

```bash
uvicorn underfit_api.main:app
```

The API is served at `http://localhost:8000`. By default, Underfit uses SQLite and local file storage with no configuration required.

## Configuration

Underfit loads settings from `underfit.toml` (or set `UNDERFIT_CONFIG` to a custom path).

**Database** — SQLite is the default and requires no configuration. PostgreSQL and MySQL are also supported by setting `[database] type` to `"postgresql"` or `"mysql"` and providing `host`, `port`, `user`, `password`, and `database`.

**Storage** — Experiment data (logs, scalars, media) is stored on the local filesystem by default. Set `[storage] type` to `"s3"` for S3-compatible object storage.

**Local mode** — To use Underfit as a single-user experiment viewer (similar to TensorBoard), set `auth_enabled = false` and `[backfill] enabled = true`. This disables authentication and watches the storage directory for new data to ingest automatically.

## Development

Install dev dependencies:

```bash
pip install -e ".[testing]"
```

Run checks:

```bash
ruff check .
ty check .
pytest .
```
