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
from .html_theme import (
    build_html_document,
    get_shared_html_css,
)

__all__ = [
    "simpletable",
    "descriptive_stats_table",
    "correlation_matrix_table",
    "sample_composition_table",
    "regression_table",
    "combined_regression_table",
    "build_html_document",
    "get_shared_html_css",
]
