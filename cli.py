"""
Generic CLI utilities for research pipeline studies.
Provides reusable argument parsing and command handling for all studies.
"""

import argparse
import sys
import webbrowser
from pathlib import Path
from typing import Dict, Any, List, Optional, Callable

from research_loom.pipeline.core import ResearchPipeline, create_study_structure


def _collect_invalidated_nodes(
    pipeline: ResearchPipeline,
    force_downstream: Optional[List[str]],
    force_upstream: Optional[List[str]],
    force_all: bool,
    force_only: Optional[List[str]],
) -> set:
    """Compute the node set that will be invalidated by force flags."""
    invalidated = set()
    if force_all:
        return set(pipeline.nodes.keys())

    if force_downstream:
        for node_name in force_downstream:
            if node_name in pipeline.nodes:
                invalidated.add(node_name)
                invalidated.update(pipeline.get_all_downstream_nodes(node_name))

    if force_upstream:
        for node_name in force_upstream:
            if node_name in pipeline.nodes:
                invalidated.add(node_name)
                invalidated.update(pipeline.get_dependencies(node_name))

    if force_only:
        for node_name in force_only:
            if node_name in pipeline.nodes:
                invalidated.add(node_name)

    return invalidated


def setup_cli_parser(description: str = "Run research pipeline", epilog: Optional[str] = None) -> argparse.ArgumentParser:
    """
    Create a standardized argument parser for research pipeline studies.
    
    Args:
        description: Description for the argument parser
        epilog: Optional epilog text with examples
        
    Returns:
        Configured ArgumentParser with standard research pipeline arguments
    """
    parser = argparse.ArgumentParser(
        description=description,
        epilog=epilog or """
Examples:
  # Run full pipeline with default config
  python study_pipeline.py

  # Run only specific nodes
  python study_pipeline.py --materialize viz_company_count cox_model

  # Force re-run node and all downstream dependents (most common)
  python study_pipeline.py --force-downstream viz_data

  # Force re-run node and all upstream dependencies
  python study_pipeline.py --force-upstream semantic_similarity

  # Force re-run all nodes (nuclear option)
  python study_pipeline.py --force-all

  # Force re-run only specific node (no dependencies/dependents)
  python study_pipeline.py --force-only viz_company_count

  # View cached visualization (opens in browser)
  python study_pipeline.py --view viz_semantic_similarity

  # View cached data node (prints summary)
  python study_pipeline.py --view raw_data

  # Use different config
  python study_pipeline.py --config default --materialize all
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        "--config",
        default="base",
        help="Configuration file name (without .yaml extension). "
             "Selects which YAML config from config/ directory to use. "
             "Each config has its own isolated cache. Default: 'base'"
    )
    parser.add_argument(
        "--materialize",
        nargs="*",
        default=["results"],
        help="Nodes whose outputs to return. Accepts: node names (space-separated), 'all', or 'none'. "
             "Pipeline will execute dependency closure of specified nodes. "
             "Default: 'results'. "
             "Example: --materialize viz_company_count cox_model"
    )
    parser.add_argument(
        "--force-downstream",
        nargs="+",
        help="Force re-run these nodes and all downstream dependents. "
             "Deletes cache entries for current config only (config-specific). "
             "Use when you've changed code/config in a node and want to regenerate it and everything that uses it. "
             "Example: --force-downstream viz_data (invalidates viz_data, viz_company_count, viz_fit)"
    )
    parser.add_argument(
        "--force-upstream",
        nargs="+",
        help="Force re-run these nodes and all upstream dependencies. "
             "Deletes cache entries for current config only (config-specific). "
             "Use when you suspect upstream data is wrong and want to regenerate everything up to this node. "
             "Example: --force-upstream viz_data (invalidates raw_data, archetypes, semantic_similarity, viz_data)"
    )
    parser.add_argument(
        "--force-all",
        action="store_true",
        help="Force re-run all nodes (invalidates entire cache for current config). "
             "Use for a complete fresh run from scratch. "
             "Example: --force-all"
    )
    parser.add_argument(
        "--force-only",
        nargs="+",
        help="Force re-run these nodes only (does not invalidate dependencies or dependents). "
             "Deletes cache entries for current config only (config-specific). "
             "Useful for debugging or when you've changed code in a specific node but inputs/outputs are unchanged. "
             "Example: --force-only viz_company_count"
    )
    parser.add_argument(
        "--view",
        nargs="*",
        help="View cached node result(s). "
             "For visualization nodes: Opens HTML in browser. "
             "For data/model nodes: Prints summary information. "
             "Example: --view viz_semantic_similarity"
             "Example: --view raw_data cox_model"
    )
    parser.add_argument(
        "--pipeline-report",
        choices=["on", "off"],
        help="Override logging.pipeline_report.enabled for this run."
    )
    parser.add_argument(
        "--pipeline-report-format",
        choices=["json", "html", "both"],
        help="Override logging.pipeline_report.format for this run."
    )
    parser.add_argument(
        "--pipeline-report-dir",
        help="Override logging.pipeline_report.output_dir for this run."
    )
    parser.add_argument(
        "--pipeline-report-keep-last",
        type=int,
        help="Override logging.pipeline_report.keep_last_n for this run."
    )
    parser.add_argument(
        "--open-pipeline-report",
        action="store_true",
        help="Open latest pipeline DAG HTML report in browser after run."
    )
    
    return parser


def handle_invalidation(
    pipeline: ResearchPipeline,
    config: Dict[str, Any],
    force_downstream_nodes: Optional[List[str]],
    force_upstream_nodes: Optional[List[str]],
    force_all: bool,
    force_only_nodes: Optional[List[str]],
    config_name: str = "base"
) -> None:
    """
    Handle cache invalidation based on force arguments.
    
    Args:
        pipeline: The ResearchPipeline instance
        config: Configuration dictionary
        force_downstream_nodes: List of nodes to force with downstream dependents, or None
        force_upstream_nodes: List of nodes to force with upstream dependencies, or None
        force_all: Whether to force all nodes
        force_only_nodes: List of nodes to force-only (no dependencies/dependents), or None
        config_name: Name of the config (for logging)
    """
    # Handle --force-all (invalidates all nodes)
    if force_all:
        print(f"Force invalidating all nodes for config '{config_name}'...")
        for node_name in pipeline.nodes.keys():
            pipeline.invalidate_node(config, node_name)
        return
    
    # Handle --force-downstream (invalidates node and downstream dependents)
    if force_downstream_nodes is not None:
        for node_name in force_downstream_nodes:
            if node_name not in pipeline.nodes:
                print(f"Warning: Node '{node_name}' not found, skipping")
                continue
            print(f"Force invalidating '{node_name}' and downstream dependents for config '{config_name}'...")
            pipeline.invalidate_downstream(config, node_name)
    
    # Handle --force-upstream (invalidates node and upstream dependencies)
    if force_upstream_nodes is not None:
        for node_name in force_upstream_nodes:
            if node_name not in pipeline.nodes:
                print(f"Warning: Node '{node_name}' not found, skipping")
                continue
            print(f"Force invalidating '{node_name}' and upstream dependencies for config '{config_name}'...")
            pipeline.invalidate_upstream(config, node_name)
    
    # Handle --force-only (invalidates only the specified nodes)
    if force_only_nodes is not None:
        for node_name in force_only_nodes:
            if node_name not in pipeline.nodes:
                print(f"Warning: Node '{node_name}' not found, skipping")
                continue
            print(f"Force invalidating '{node_name}' only (no dependencies/dependents) for config '{config_name}'...")
            pipeline.invalidate_node(config, node_name)


def handle_view(
    pipeline: ResearchPipeline,
    config: Dict[str, Any],
    view_nodes: List[str],
    open_browser: bool = True,
    config_name: str = "base"
) -> None:
    """
    Handle viewing cached node results.
    
    Args:
        pipeline: The ResearchPipeline instance
        config: Configuration dictionary
        view_nodes: List of node names to view
        open_browser: Whether to open HTML visualizations in browser
        config_name: Configuration file name for display
    """
    for node_name in view_nodes:
        if node_name not in pipeline.nodes:
            print(f"Warning: Node '{node_name}' not found, skipping")
            continue
        try:
            pipeline.view_cached_node(config, node_name, open_browser=open_browser, config_name=config_name)
        except Exception as e:
            print(f"Error viewing node '{node_name}': {e}")
            import traceback
            traceback.print_exc()


def normalize_materialize(materialize: Any) -> Any:
    """
    Normalize materialize argument to standard format.

    Default behavior:
      - If the user does NOT specify --materialize (empty / ["results"]):
          → return "none"  (do not materialize any nodes, just manage cache/compute)
      - If the user explicitly specifies values (e.g. ["all"], ["results"], ["node_a"]):
          → pass through unchanged

    This makes the CLI conservative by default for large studies: it won't
    load any (potentially huge) cached results into memory unless the user
    explicitly asks for them.

    Args:
        materialize: Materialize argument from argparse (list, "all", "none", etc.)

    Returns:
        Normalized materialize value
    """
    if not materialize or materialize == ["results"]:
        return "none"
    if isinstance(materialize, (list, tuple)) and len(materialize) == 1:
        token = str(materialize[0]).strip().lower()
        if token in {"all", "none", "sinks"}:
            return token
    return materialize


def apply_pipeline_report_overrides(config: Dict[str, Any], args: argparse.Namespace) -> Dict[str, Any]:
    """Apply CLI overrides to strict logging.pipeline_report config."""
    has_override = any(
        [
            args.pipeline_report is not None,
            args.pipeline_report_format is not None,
            args.pipeline_report_dir is not None,
            args.pipeline_report_keep_last is not None,
        ]
    )
    if not has_override:
        return config

    logging_cfg = config.setdefault("logging", {})
    existing = logging_cfg.get("pipeline_report")
    report_cfg = dict(existing) if isinstance(existing, dict) else {}

    if args.pipeline_report is not None:
        report_cfg["enabled"] = args.pipeline_report == "on"
    if args.pipeline_report_format is not None:
        report_cfg["format"] = args.pipeline_report_format
    if args.pipeline_report_dir is not None:
        report_cfg["output_dir"] = args.pipeline_report_dir
    if args.pipeline_report_keep_last is not None:
        report_cfg["keep_last_n"] = args.pipeline_report_keep_last
    logging_cfg["pipeline_report"] = report_cfg
    return config


def run_cli(
    create_pipeline: Callable[[], ResearchPipeline],
    load_config: Callable[[str], Dict[str, Any]],
    run_study: Callable[[str, Any], tuple],
    description: str = "Run research pipeline",
    epilog: Optional[str] = None
) -> None:
    """
    Main CLI entry point for research pipeline studies.
    
    This function handles all standard CLI operations:
    - Argument parsing
    - Cache invalidation (--force-downstream, --force-upstream, --force-all, --force-only)
    - Viewing cached results (--view)
    - Running the pipeline
    
    Args:
        create_pipeline: Function that creates and returns a ResearchPipeline instance
        load_config: Function that loads config given a config name (str) -> Dict
        run_study: Function that runs the study, takes (config_name, materialize) -> (results, pipeline, config)
        description: Description for the argument parser
        epilog: Optional epilog text with examples
    """
    parser = setup_cli_parser(description, epilog)
    args = parser.parse_args()
    
    # Handle --view (view cached nodes without running pipeline)
    if args.view:
        view_nodes = args.view if isinstance(args.view, list) else [args.view]
        config = load_config(args.config)
        temp_pipeline = create_pipeline()
        handle_view(temp_pipeline, config, view_nodes, open_browser=True, config_name=args.config)
        sys.exit(0)
    
    # Load configuration (needed for invalidation and running pipeline)
    config = load_config(args.config)
    config = apply_pipeline_report_overrides(config, args)
    
    # Track which nodes were invalidated so we can ensure they're computed
    invalidated_nodes = set()
    
    # Invalidate cache if any force arguments are specified
    if (args.force_downstream is not None or args.force_upstream is not None or 
        args.force_all or args.force_only is not None):
        temp_pipeline = create_pipeline()
        invalidated_nodes = _collect_invalidated_nodes(
            temp_pipeline,
            args.force_downstream,
            args.force_upstream,
            args.force_all,
            args.force_only,
        )
        
        handle_invalidation(
            temp_pipeline, 
            config, 
            args.force_downstream,
            args.force_upstream,
            args.force_all,
            args.force_only,
            args.config
        )
    
    # Normalize materialize argument
    materialize = normalize_materialize(args.materialize)
    
    # If nodes were invalidated, ensure they get computed
    # - If materialize is "none" (default), auto-materialize invalidated nodes
    # - If materialize is a list, merge invalidated nodes into it
    if invalidated_nodes:
        if materialize == "none":
            materialize = list(invalidated_nodes)
            print(f"Auto-materializing invalidated nodes: {sorted(materialize)}")
        elif isinstance(materialize, (list, tuple, set)):
            # Merge invalidated nodes into existing materialize list
            materialize_list = list(materialize) if not isinstance(materialize, list) else materialize
            for node in invalidated_nodes:
                if node not in materialize_list:
                    materialize_list.append(node)
            materialize = materialize_list
            print(f"Auto-materializing invalidated nodes: {sorted(invalidated_nodes)}")
    
    # Run with the already-loaded (and CLI-overridden) config so strict
    # pipeline_report settings apply deterministically.
    pipeline = create_pipeline()
    results = pipeline.run_pipeline(config, materialize=materialize)
    
    print(f"\nStudy completed!")
    print(f"Results: {list(results.keys())}")
    
    # Print model summary if available
    if "results" in results:
        results_data = results["results"]
        print(f"\nModel Summary:")
        print(results_data)

    if args.open_pipeline_report:
        report_paths = getattr(pipeline, "_last_report_paths", {}) or {}
        html_path = report_paths.get("latest_html") or report_paths.get("html")
        if html_path:
            file_url = Path(html_path).resolve().as_uri()
            webbrowser.open(file_url)
            print(f"Opened pipeline report: {html_path}")
        else:
            print("No HTML pipeline report available to open (check pipeline_report.format).")


def main():
    """
    Standalone CLI entry point for research_loom.
    Can be used to create new studies or run general commands.
    """
    parser = argparse.ArgumentParser(
        description="Research Loom - Research Pipeline Framework",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Create study command
    create_parser = subparsers.add_parser(
        "create-study",
        help="Create a new study with directory structure and templates"
    )
    create_parser.add_argument(
        "study_name",
        help="Name of the study (will be used as folder name)"
    )
    create_parser.add_argument(
        "--dir",
        default=".",
        help="Parent directory where to create the study (default: current directory)"
    )
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 0
    
    if args.command == "create-study":
        try:
            study_path = create_study_structure(
                study_name=args.study_name,
                study_dir=args.dir
            )
            print(f"\n✓ Study '{args.study_name}' created successfully!")
            print(f"  Location: {study_path}")
            print(f"\nNext steps:")
            print(f"  1. cd {study_path}")
            print(f"  2. Edit config/base.yaml to configure your study")
            print(f"  3. Add your data to the data/ directory")
            print(f"  4. Implement your pipeline nodes in study_pipeline.py")
            print(f"  5. Run: python study_pipeline.py")
            return 0
        except Exception as e:
            print(f"Error creating study: {e}")
            import traceback
            traceback.print_exc()
            return 1
    else:
        parser.print_help()
        return 0


if __name__ == "__main__":
    sys.exit(main())

