# CLI Arguments Documentation

This document explains in detail how to use the `--config`, `--materialize`, and cache invalidation arguments (`--force-downstream`, `--force-upstream`, `--force-all`, `--force-only`) when running the study pipeline.

## Table of Contents
1. [--config: Configuration Selection](#--config-configuration-selection)
2. [--materialize: Output Selection](#--materialize-output-selection)
3. [Cache Invalidation: --force-downstream, --force-upstream, --force-all, --force-only](#cache-invalidation--force-downstream--force-upstream--force-all--force-only)

---

## --config: Configuration Selection

### What It Does
The `--config` argument selects which YAML configuration file to use for the pipeline execution. Each configuration file contains settings that control how nodes execute (e.g., data sources, processing parameters, model hyperparameters).

### How It Works
- **Default**: `base` (uses `config/base.yaml`)
- **Format**: Just the filename without the `.yaml` extension
- **Location**: Configuration files are in `config/` directory relative to `study_pipeline.py`

### Examples

```bash
# Use default config (base.yaml)
python study_pipeline.py

# Explicitly use base config
python study_pipeline.py --config base

# Use a different config file
python study_pipeline.py --config default

# Use a development config
python study_pipeline.py --config dev
```

### What's in a Config File?
Each config file contains sections that correspond to different nodes:

```yaml
# config/base.yaml example
data:
  industry: "Artificial Intelligence"
  join_sources:
    companieshouse: "LEFT"
    linkedin: "LEFT"
    webcontent: null
  limit: 10

archetypes:
  embed_dim: 768
  force_embedding: false

semantic_similarity:
  embed_dim: 768
  min_words: 10

visualizations:
  company_count:
    round_days: 120
  semantic_similarity:
    round_days: 120
    round_years: 0.5

cox_data:
  admin_censor_date: "2024-01-01"
```

### Important Notes
- **Cache Isolation**: Each config has its own cache entries. Running with `--config base` won't affect cache from `--config default`
- **Config-Specific Cache Keys**: The pipeline generates unique cache keys based on config content, so changing a config value will invalidate affected nodes automatically
- **File Must Exist**: The config file must exist in `config/` directory, otherwise you'll get a `FileNotFoundError`

---

## --materialize: Output Selection

### What It Does
The `--materialize` argument controls **which nodes' outputs are returned** in the results dictionary. It also determines **which nodes need to be executed** (by computing the dependency closure).

### Key Concepts

1. **Materialization** = "Which outputs do I want back?"
2. **Dependency Closure** = "What nodes must run to produce those outputs?"
3. **Execution** = The pipeline will run all nodes in the dependency closure, but only return the materialized ones

### How It Works

The `--materialize` argument accepts:
- **List of node names**: `["node1", "node2"]` → Execute their closure, return only these
- **"all"**: Execute and return all nodes
- **"sinks"** (default in framework): Return only sink nodes (nodes with no downstream dependents)
- **"none"**: Don't return anything (rarely used)

**Note**: In `study_pipeline.py`, the default is `["results"]`, which gets converted to `"all"` if no specific nodes are provided.

### Pipeline Structure Reference

For `study_archetypes`, the pipeline has this dependency structure:

```
raw_data (no inputs)
archetypes (no inputs)
semantic_similarity (depends on: raw_data, archetypes)
  ├── viz_company_count (depends on: semantic_similarity)
  ├── viz_semantic_similarity (depends on: semantic_similarity)
  ├── viz_fit (depends on: semantic_similarity)
  └── cox_data (depends on: semantic_similarity)
      └── cox_model (depends on: cox_data)
          └── results (depends on: raw_data, archetypes, semantic_similarity, cox_data, cox_model)
```

### Examples

#### Example 1: Materialize a Single Node
```bash
# Only return the cox_model output
python study_pipeline.py --materialize cox_model
```
**What happens:**
- Pipeline executes: `raw_data`, `archetypes`, `semantic_similarity`, `cox_data`, `cox_model`
- Pipeline returns: `{"cox_model": {...}}`
- **Note**: `results` node is NOT executed because it's not in the dependency closure of `cox_model`

#### Example 2: Materialize Multiple Nodes
```bash
# Return both visualization outputs
python study_pipeline.py --materialize viz_company_count viz_semantic_similarity
```
**What happens:**
- Pipeline executes: `raw_data`, `archetypes`, `semantic_similarity`, `viz_company_count`, `viz_semantic_similarity`
- Pipeline returns: `{"viz_company_count": {...}, "viz_semantic_similarity": {...}}`
- **Note**: `viz_fit` is NOT executed because it's not requested

#### Example 3: Materialize All Nodes
```bash
# Execute and return everything
python study_pipeline.py --materialize all
```
**What happens:**
- Pipeline executes: ALL nodes in dependency order
- Pipeline returns: All node outputs in a dictionary

#### Example 4: Default Behavior (No --materialize)
```bash
# Default: materialize "all" (because of the conversion in study_pipeline.py)
python study_pipeline.py
```
**What happens:**
- Pipeline executes: ALL nodes
- Pipeline returns: All node outputs

#### Example 5: Materialize Only Results
```bash
# Only return the final results node
python study_pipeline.py --materialize results
```
**What happens:**
- Pipeline executes: ALL nodes (because `results` depends on everything)
- Pipeline returns: `{"results": {...}}`

### Dependency Closure Explained

When you materialize a node, the pipeline automatically computes its **dependency closure**:

- **Direct dependencies**: Nodes that the materialized node directly depends on
- **Transitive dependencies**: Dependencies of dependencies (recursive)
- **Execution order**: Nodes are executed in topological order (dependencies before dependents)

**Example**: If you materialize `cox_model`:
- Direct dependency: `cox_data`
- Transitive dependencies: `semantic_similarity` → `raw_data`, `archetypes`
- Execution order: `raw_data`, `archetypes` → `semantic_similarity` → `cox_data` → `cox_model`

### Cache Behavior with --materialize

- **Cached nodes are skipped**: If a node in the dependency closure is already cached, it's loaded from cache (not re-executed)
- **Only requested outputs are returned**: Even if 10 nodes execute, you only get back the materialized ones
- **Efficient**: The pipeline uses a "fast path" when everything is cached - it only loads the materialized outputs

### Common Use Cases

```bash
# Quick visualization check (only run visualization nodes)
python study_pipeline.py --materialize viz_company_count

# Check model without running visualizations
python study_pipeline.py --materialize cox_model

# Full pipeline run (default)
python study_pipeline.py --materialize all

# Just check if data loading works
python study_pipeline.py --materialize raw_data
```

---

## Cache Invalidation: --force-downstream, --force-upstream, --force-all, --force-only

### Overview

The pipeline provides four options for cache invalidation, each serving different use cases:

- **`--force-downstream <node>`**: Invalidate node + all downstream dependents (most common)
- **`--force-upstream <node>`**: Invalidate node + all upstream dependencies  
- **`--force-all`**: Invalidate all nodes (nuclear option)
- **`--force-only <node>`**: Invalidate only the specified node (no dependencies/dependents)

### Key Concepts

1. **Config-Specific**: Only deletes cache for the **current config** (specified by `--config`)
2. **File-Based**: Cache invalidation is persistent (deletes files), so it works across pipeline instances
3. **Directional**: Choose whether to invalidate upstream (dependencies) or downstream (dependents)
4. **Re-execution**: When the pipeline runs, invalidated nodes will be re-executed (not loaded from cache)

### How It Works

1. **Compute Dependencies/Dependents**: For each node, compute upstream dependencies or downstream dependents
2. **Get Cache Keys**: For each node (including dependencies/dependents), compute the cache key for the current config
3. **Delete Cache Directories**: Remove the specific cache directory for each node/config combination
4. **Re-execution**: When the pipeline runs, invalidated nodes will be re-executed (not loaded from cache)

### Cache Structure

Cache is organized as:
```
cache/
  node_name/
    cache_key_abc123/  ← Config-specific cache entry
      cache.json
      artifacts/
    cache_key_def456/  ← Different config's cache entry
      cache.json
      artifacts/
```

When you use any force option, only the cache entry matching the current config is deleted.

### --force-downstream: Most Common Use Case

**Use when**: You've changed code/config in a node and want to regenerate it and everything that uses it.

#### Example 1: Force Downstream
```bash
# Force re-run viz_data and all nodes that depend on it
python study_pipeline.py --config base --force-downstream viz_data
```
**What happens:**
1. Invalidates cache for: `viz_data`, `viz_company_count`, `viz_fit`
2. Pipeline executes: All three nodes are re-run (not loaded from cache)
3. **Note**: Upstream nodes (`semantic_similarity`, `raw_data`, etc.) remain cached and are reused

#### Example 2: Multiple Nodes Downstream
```bash
# Force multiple nodes and their dependents
python study_pipeline.py --config base --force-downstream semantic_similarity viz_data
```
**What happens:**
1. Invalidates: `semantic_similarity` + all its dependents, `viz_data` + all its dependents
2. Pipeline executes: All invalidated nodes are re-run

### --force-upstream: Regenerate from Source

**Use when**: You suspect upstream data is wrong and want to regenerate everything up to a node.

#### Example 3: Force Upstream
```bash
# Force re-run semantic_similarity and all its dependencies
python study_pipeline.py --config base --force-upstream semantic_similarity
```
**What happens:**
1. Invalidates cache for: `raw_data`, `archetypes`, `semantic_similarity`
2. Pipeline executes: All three nodes are re-run (not loaded from cache)
3. **Note**: Downstream nodes will automatically re-execute because their inputs changed

### --force-all: Nuclear Option

**Use when**: You want a completely fresh run from scratch.

#### Example 4: Force All
```bash
# Force re-run everything
python study_pipeline.py --config base --force-all
```
**What happens:**
1. Invalidates cache for: ALL nodes
2. Pipeline executes: Everything is re-run from scratch

### --force-only: Isolated Changes

**Use when**: You've changed code in a node but inputs/outputs are unchanged (rare).

#### Example 5: Force Only
```bash
# Force re-run only viz_company_count (no dependencies/dependents)
python study_pipeline.py --config base --force-only viz_company_count
```
**What happens:**
1. Invalidates cache for: `viz_company_count` only
2. Pipeline executes: Only `viz_company_count` is re-run
3. Uses cached inputs from `viz_data`

### Dependency Chain Examples

#### Force Downstream: `--force-downstream viz_data`
```
Force: viz_data
  ↓
Invalidates: viz_data
  ↓
Invalidates: viz_company_count (depends on viz_data)
  ↓
Invalidates: viz_fit (depends on viz_data)
```
**Result**: 3 nodes invalidated. Upstream nodes (`semantic_similarity`, etc.) remain cached.

#### Force Upstream: `--force-upstream cox_model`
```
Force: cox_model
  ↓
Invalidates: cox_model
  ↓
Invalidates: cox_data (dependency)
  ↓
Invalidates: semantic_similarity (dependency of cox_data)
  ↓
Invalidates: raw_data, archetypes (dependencies of semantic_similarity)
```
**Result**: 5 nodes invalidated. Downstream nodes will re-execute automatically.

### When to Use Each Option

**Use `--force-downstream` when:**
- ✅ You've changed code/config in a specific node (most common)
- ✅ You want to regenerate a node and everything that uses it
- ✅ You want to preserve upstream cache (faster)

**Use `--force-upstream` when:**
- ✅ You suspect upstream data is wrong
- ✅ You've updated external dependencies (database, data files)
- ✅ You want to ensure fresh inputs before a node

**Use `--force-all` when:**
- ✅ You want a completely clean slate
- ✅ You suspect widespread cache corruption
- ✅ You're benchmarking and want consistent fresh runs

**Use `--force-only` when:**
- ✅ You've changed code but inputs/outputs are identical
- ✅ You're debugging a specific node in isolation
- ✅ You want minimal invalidation

### Combining Arguments

```bash
# Force downstream and return only specific outputs
python study_pipeline.py --config default --force-downstream viz_data --materialize viz_company_count

# Force upstream and return all outputs
python study_pipeline.py --config base --force-upstream semantic_similarity --materialize all

# Force all and return results
python study_pipeline.py --config base --force-all --materialize results
```

### Important Notes

1. **Config Isolation**: All force options only affect the config specified by `--config`. Other configs' caches remain untouched.

2. **Automatic Re-execution**: When a node's inputs change (due to upstream invalidation), downstream nodes will automatically re-execute even if not explicitly invalidated. However, explicitly invalidating downstream nodes ensures their cache is cleared upfront.

3. **Efficiency**: Use force options sparingly. Cache is designed to save time - only force when necessary. `--force-downstream` is usually the best choice as it preserves upstream cache.

4. **Multiple Nodes**: You can force multiple nodes at once. The pipeline will compute the union of all dependencies.

5. **Non-existent Nodes**: If you specify a node that doesn't exist, you'll get a warning and it will be skipped.

---

## Complete Examples

### Example 1: Full Pipeline Run
```bash
# Run everything with base config, return all outputs
python study_pipeline.py --config base --materialize all
```

### Example 2: Quick Visualization Check
```bash
# Just generate one visualization (uses cache for dependencies if available)
python study_pipeline.py --config base --materialize viz_company_count
```

### Example 3: Force Re-run After Code Change
```bash
# You modified viz_data.py, force it and dependents to re-run
python study_pipeline.py --config base --force-downstream viz_data --materialize viz_data
```

### Example 4: Clean Sweep for Testing
```bash
# Force everything to re-run (useful for testing)
python study_pipeline.py --config base --force-all
python study_pipeline.py --config base --force results --materialize all
```

### Example 5: Config Comparison
```bash
# Run with base config
python study_pipeline.py --config base --materialize results

# Run with default config (different cache, won't affect base)
python study_pipeline.py --config default --materialize results
```

---

## Summary Table

| Argument | Purpose | Default | Accepts Multiple? | Config-Specific? |
|----------|---------|---------|-------------------|------------------|
| `--config` | Select configuration file | `base` | No | N/A |
| `--materialize` | Select which outputs to return | `all` | Yes (list) | No |
| `--force-downstream` | Invalidate node + downstream dependents | None | Yes (list) | Yes |
| `--force-upstream` | Invalidate node + upstream dependencies | None | Yes (list) | Yes |
| `--force-all` | Invalidate all nodes | None | No (flag) | Yes |
| `--force-only` | Invalidate only specified node | None | Yes (list) | Yes |

---

## Troubleshooting

### "Node 'xyz' not found"
- Check that the node name is spelled correctly
- Use `--materialize all` to see all available node names

### "Config file not found"
- Ensure the config file exists in `config/` directory
- Don't include `.yaml` extension in `--config` argument

### Cache not invalidating
- Ensure you're using the same `--config` that was used to create the cache
- Check that the cache directory exists and is writable
- Verify the node name is correct

### Unexpected re-execution
- Check if config values changed (this automatically invalidates affected nodes)
- Verify you didn't use `--force` accidentally
- Check if upstream dependencies were invalidated

