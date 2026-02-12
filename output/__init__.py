"""
Output formatting utilities for research pipelines.

Provides HTML table generation for:
- Descriptive statistics
- Correlation matrices
- Regression results
"""

from .html_tables import (
    simpletable,
    descriptive_stats_table,
    correlation_matrix_table,
    sample_composition_table,
    regression_table,
    combined_regression_table,
)

__all__ = [
    "simpletable",
    "descriptive_stats_table",
    "correlation_matrix_table",
    "sample_composition_table",
    "regression_table",
    "combined_regression_table",
]
