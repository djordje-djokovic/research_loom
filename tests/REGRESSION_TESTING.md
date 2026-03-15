# Research Loom Regression Test Contract

This suite is designed for two workflows:

- Fast refactor loop: run frequently while changing internals.
- Full regression baseline: run before and after refactor to compare behavior.

## Test Groups

Markers in `pytest.ini`:

- `smoke` - fastest sanity checks.
- `core` - DAG/cache/materialization/invalidation.
- `cli` - CLI orchestration behavior.
- `artifact` - artifact spill/load behaviors.
- `integration` - cross-module tests.
- `full_matrix` - optional dependency/format matrix.
- `slow` - expensive tests (none required by default).

## Commands

Run from repository root (`research_loom/`).

### 1) Fast Loop (default via `pytest.ini`)

```bash
python -m pytest
```

Equivalent explicit command:

```bash
python -m pytest -m "smoke or core or cli"
```

### 2) Full Regression (pre/post refactor)

```bash
python -m pytest -m "core or cli or artifact or integration or full_matrix"
```

### 3) Baseline Artifacts for Comparison

If `pytest-cov` is installed, use:

```bash
python -m pytest -m "core or cli or artifact or integration or full_matrix" \
  --junitxml=tests/.artifacts/junit_pre_refactor.xml \
  --durations=25 \
  --cov=pipeline --cov=cli --cov=output \
  --cov-report=xml:tests/.artifacts/coverage_pre_refactor.xml \
  --cov-report=html:tests/.artifacts/coverage_pre_refactor_html
```

Post-refactor run (same command, different output names):

```bash
python -m pytest -m "core or cli or artifact or integration or full_matrix" \
  --junitxml=tests/.artifacts/junit_post_refactor.xml \
  --durations=25 \
  --cov=pipeline --cov=cli --cov=output \
  --cov-report=xml:tests/.artifacts/coverage_post_refactor.xml \
  --cov-report=html:tests/.artifacts/coverage_post_refactor_html
```

If `pytest-cov` is unavailable, still capture pass/fail and timing:

```bash
python -m pytest -m "core or cli or artifact or integration or full_matrix" \
  --junitxml=tests/.artifacts/junit_pre_refactor.xml \
  --durations=25
```

## Comparison Checklist

- Compare `junit_pre_refactor.xml` vs `junit_post_refactor.xml`:
  - no unexpected test failures
  - no removed critical tests
- Compare duration output:
  - watch for major slowdowns in `core` and `artifact` tests
- Compare coverage XML/HTML (if enabled):
  - confirm no accidental drop in coverage across `pipeline`, `cli`, `output`
