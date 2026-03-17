"""Generic matrix execution helpers."""

from .executor import run_spec_matrix
from .reporting import summarize_matrix_results

__all__ = ["run_spec_matrix", "summarize_matrix_results"]
