"""
Research Loom

A parsimonious research pipeline framework with caching, artifact management, 
and export capabilities.
"""

__version__ = "1.0.0"
__author__ = "Research Team"
__email__ = "research@example.com"

# Import main classes for easy access
from .pipeline.core import ResearchPipeline, Node

# Import output utilities
from .output import (
    simpletable,
    descriptive_stats_table,
    correlation_matrix_table,
    sample_composition_table,
    regression_table,
    combined_regression_table,
)

__all__ = [
    "ResearchPipeline",
    "Node",
    "simpletable",
    "descriptive_stats_table",
    "correlation_matrix_table",
    "sample_composition_table",
    "regression_table",
    "combined_regression_table",
]
