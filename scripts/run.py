#!/usr/bin/env python3
"""
CLI entrypoint for the research pipeline framework.
"""

import argparse
import sys
import yaml
from pathlib import Path
from typing import Dict, Any

# Add the project root to the path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from pipeline.core import ResearchPipeline, create_study_structure


def load_config(study_dir: str, config_name: str = "base") -> Dict[str, Any]:
    """Load configuration from study directory"""
    config_path = Path(study_dir) / "config" / f"{config_name}.yaml"
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def main():
    parser = argparse.ArgumentParser(description="Research Pipeline CLI")
    parser.add_argument("study_dir", nargs="?", help="Path to study directory")
    parser.add_argument("--config", default="base", help="Configuration name (default: base)")
    parser.add_argument("--materialize", default="sinks", help="Nodes to materialize (default: sinks)")
    parser.add_argument("--export", help="Export node to specified directory")
    parser.add_argument("--include-ancestors", action="store_true", help="Include ancestor artifacts in export")
    parser.add_argument("--plan", action="store_true", help="Show execution plan without running")
    parser.add_argument("--cache-dir", help="Override cache directory")
    parser.add_argument("--create-study", help="Create a new study with the given name")
    
    args = parser.parse_args()
    
    # Handle study creation
    if args.create_study:
        try:
            # Create studies directory relative to current working directory
            studies_dir = "studies"
            study_path = create_study_structure(
                study_name=args.create_study,
                study_dir=studies_dir
            )
            print(f"\nStudy '{args.create_study}' created successfully!")
            print(f"Location: {study_path}")
            print(f"Template: FULL (with all features)")
            print("\nNext steps:")
            print(f"1. cd {study_path}")
            print("2. Edit config/base.yaml to configure your study")
            print("3. Add your data to the data/ directory")
            print("4. Implement your pipeline nodes in study_pipeline.py")
            print("5. Run: python study_pipeline.py")
            return 0
        except Exception as e:
            print(f"Error creating study: {e}")
            return 1
    
    # Validate that study_dir is provided for other operations
    if not args.study_dir:
        parser.error("study_dir is required when not using --create-study")
    
    # Load configuration
    try:
        config = load_config(args.study_dir, args.config)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        return 1
    
    # Initialize pipeline
    cache_dir = args.cache_dir or f"{args.study_dir}/cache"
    pipe = ResearchPipeline(cache_dir=cache_dir)
    
    # For now, this is a placeholder - in a real implementation,
    # you would define your study-specific nodes here
    print(f"Study directory: {args.study_dir}")
    print(f"Configuration: {args.config}")
    print(f"Cache directory: {cache_dir}")
    
    if args.plan:
        print("\nExecution plan would be shown here")
        return 0
    
    if args.export:
        print(f"Export functionality would export to: {args.export}")
        return 0
    
    print("\nPipeline execution would run here")
    return 0


if __name__ == "__main__":
    sys.exit(main())
