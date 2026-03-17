"""
HTML table generation utilities for research output.

All tables use class="simpletable" for consistent styling across studies.
"""

from typing import Dict, List, Optional, Callable, Any, Union


def _default_var_formatter(var: str) -> str:
    """Default variable name formatter: replace underscores with spaces and title case."""
    return var.replace('_', ' ').title()


def _sig_stars(pval: float) -> str:
    """Return significance stars from p-value."""
    if pval < 0.001:
        return "***"
    if pval < 0.01:
        return "**"
    if pval < 0.05:
        return "*"
    return ""


def simpletable(
    rows: List[List[str]],
    headers: List[str],
    caption: Optional[str] = None,
    footer: Optional[str] = None,
) -> str:
    """
    Generate a simple HTML table with consistent styling.
    
    Args:
        rows: List of rows, each row is a list of cell values (strings)
        headers: List of header strings
        caption: Optional table caption
        footer: Optional footer text (small, below table)
    
    Returns:
        HTML table string
    """
    html = ['<table class="simpletable">']
    
    if caption:
        html.append(f'<caption>{caption}</caption>')
    
    # Header row
    html.append('<tr>')
    for h in headers:
        html.append(f'<th>{h}</th>')
    html.append('</tr>')
    
    # Data rows
    for row in rows:
        html.append('<tr>')
        for cell in row:
            html.append(f'<td>{cell}</td>')
        html.append('</tr>')
    
    html.append('</table>')
    
    if footer:
        html.append(f'<p class="rl-note">{footer}</p>')
    
    return '\n'.join(html)


def descriptive_stats_table(
    data: Union[List[Dict], Any],  # List[Dict] or pandas DataFrame
    variables: List[str],
    var_formatter: Optional[Callable[[str], str]] = None,
    title: Optional[str] = None,
) -> str:
    """
    Generate HTML table with descriptive statistics.
    
    Args:
        data: List of dicts or pandas DataFrame
        variables: List of variable names to include
        var_formatter: Optional function to format variable names for display
        title: Optional custom title (default: "Descriptive Statistics")
    
    Returns:
        HTML table string with Mean, SD, Min, Median, Max
    """
    try:
        import pandas as pd
    except ImportError:
        return "<p>pandas/numpy required for descriptive statistics</p>"
    
    if var_formatter is None:
        var_formatter = _default_var_formatter
    
    # Convert to DataFrame if needed
    if isinstance(data, list):
        df = pd.DataFrame(data)
    else:
        df = data
    
    if df.empty:
        return "<p>No data available for descriptive statistics</p>"
    
    # Filter to available variables
    available_vars = [v for v in variables if v in df.columns]
    if not available_vars:
        return "<p>No valid variables for descriptive statistics</p>"
    
    n = len(df)
    caption = title or f"Descriptive Statistics (N = {n:,})"
    
    # Calculate statistics
    desc_stats = df[available_vars].describe().T
    
    # Build HTML
    html = ['<table class="simpletable">']
    html.append(f'<caption>{caption}</caption>')
    
    # Header
    html.append('<tr>')
    html.append('<th>Variable</th><th>Mean</th><th>SD</th><th>Min</th><th>Median</th><th>Max</th>')
    html.append('</tr>')
    
    # Data rows
    for var in available_vars:
        if var not in desc_stats.index:
            continue
        var_display = var_formatter(var)
        mean_val = desc_stats.loc[var, 'mean']
        std_val = desc_stats.loc[var, 'std']
        min_val = desc_stats.loc[var, 'min']
        max_val = desc_stats.loc[var, 'max']
        median_val = desc_stats.loc[var, '50%']
        
        html.append('<tr>')
        html.append(f'<td>{var_display}</td>')
        html.append(f'<td>{mean_val:.4f}</td>')
        html.append(f'<td>{std_val:.4f}</td>')
        html.append(f'<td>{min_val:.4f}</td>')
        html.append(f'<td>{median_val:.4f}</td>')
        html.append(f'<td>{max_val:.4f}</td>')
        html.append('</tr>')
    
    html.append('</table>')
    
    return '\n'.join(html)


def correlation_matrix_table(
    data: Union[List[Dict], Any],  # List[Dict] or pandas DataFrame
    variables: List[str],
    var_formatter: Optional[Callable[[str], str]] = None,
    title: Optional[str] = None,
    show_significance: bool = True,
) -> str:
    """
    Generate HTML correlation matrix (lower triangle with significance stars).
    
    Args:
        data: List of dicts or pandas DataFrame
        variables: List of variable names to include
        var_formatter: Optional function to format variable names for display
        title: Optional custom title
        show_significance: Whether to show significance stars (* p<.05, ** p<.01, *** p<.001)
    
    Returns:
        HTML table string with correlation matrix
    """
    try:
        import pandas as pd
        import numpy as np
    except ImportError:
        return "<p>pandas/numpy required for correlation matrix</p>"
    
    try:
        from scipy.stats import t
        HAS_SCIPY = True
    except ImportError:
        HAS_SCIPY = False
    
    if var_formatter is None:
        var_formatter = _default_var_formatter
    
    # Convert to DataFrame if needed
    if isinstance(data, list):
        df = pd.DataFrame(data)
    else:
        df = data
    
    if df.empty:
        return "<p>No data available for correlation matrix</p>"
    
    # Filter to available variables
    available_vars = [v for v in variables if v in df.columns]
    if len(available_vars) < 2:
        return "<p>Need at least 2 variables for correlation matrix</p>"
    
    # Convert to numeric
    df_corr = df[available_vars].apply(pd.to_numeric, errors='coerce')
    
    # Use total observations (pairwise correlations handle NaN per pair)
    n = len(df_corr)
    
    # Calculate correlations (uses pairwise complete observations by default)
    corr_matrix = df_corr.corr()
    
    caption = title or f"Correlation Matrix (N = {n:,})"
    
    # Build HTML
    html = ['<table class="simpletable">']
    html.append(f'<caption>{caption}</caption>')
    
    # Header with variable numbers
    html.append('<tr>')
    html.append('<th></th>')
    for i in range(len(available_vars)):
        html.append(f'<th>({i+1})</th>')
    html.append('</tr>')
    
    # Data rows
    for i, var in enumerate(available_vars):
        var_display = var_formatter(var)
        html.append('<tr>')
        html.append(f'<td>({i+1}) {var_display}</td>')
        
        for j, var2 in enumerate(available_vars):
            if j > i:
                html.append('<td></td>')  # Upper triangle empty
            elif j == i:
                html.append('<td>1.00</td>')  # Diagonal
            else:
                corr_val = corr_matrix.loc[var, var2]
                if pd.isna(corr_val):
                    html.append('<td>-</td>')
                else:
                    sig = ""
                    if show_significance and HAS_SCIPY and n > 2:
                        try:
                            t_stat = corr_val * np.sqrt((n - 2) / (1 - corr_val**2 + 1e-10))
                            p_val = 2 * (1 - t.cdf(abs(t_stat), n - 2))
                            sig = _sig_stars(p_val)
                        except:
                            pass
                    html.append(f'<td>{corr_val:.2f}{sig}</td>')
        
        html.append('</tr>')
    
    html.append('</table>')
    
    if show_significance:
        html.append('<p class="rl-note">* p < .05, ** p < .01, *** p < .001</p>')
    
    return '\n'.join(html)


def sample_composition_table(
    data: Union[List[Dict], Any],
    id_column: str = 'company_id',
    unit_column: Optional[str] = None,
    outcome_column: Optional[str] = None,
    var_formatter: Optional[Callable[[str], str]] = None,
    title: Optional[str] = None,
) -> str:
    """
    Generate HTML sample composition table.
    
    Args:
        data: List of dicts or pandas DataFrame
        id_column: Column name for entity ID (e.g., 'company_id')
        unit_column: Optional column for unit of analysis (e.g., 'founder_name')
        outcome_column: Optional column for outcome variable (e.g., 'secondary_sale_shares')
        var_formatter: Optional function to format variable names for display
        title: Optional custom title
    
    Returns:
        HTML table string with sample composition
    """
    try:
        import pandas as pd
    except ImportError:
        return "<p>pandas required for sample composition</p>"
    
    if var_formatter is None:
        var_formatter = _default_var_formatter
    
    # Convert to DataFrame if needed
    if isinstance(data, list):
        df = pd.DataFrame(data)
    else:
        df = data
    
    if df.empty:
        return "<p>No data available</p>"
    
    html = ['<table class="simpletable">']
    html.append(f'<caption>{title or "Sample Composition"}</caption>')
    
    # Panel A: Overall
    html.append('<tr><th colspan="4"><b>Panel A: Overall</b></th></tr>')
    html.append('<tr><th>Metric</th><th>Count</th><th colspan="2"></th></tr>')
    
    n_obs = len(df)
    html.append(f'<tr><td>Observations</td><td>{n_obs:,}</td><td colspan="2"></td></tr>')
    
    if id_column in df.columns:
        n_entities = df[id_column].nunique()
        html.append(f'<tr><td>Entities ({id_column})</td><td>{n_entities:,}</td><td colspan="2"></td></tr>')
    
    if unit_column and unit_column in df.columns:
        n_units = df[unit_column].nunique()
        html.append(f'<tr><td>Units ({unit_column})</td><td>{n_units:,}</td><td colspan="2"></td></tr>')
    
    # Panel B: Outcome (if specified)
    if outcome_column and outcome_column in df.columns:
        outcome_display = var_formatter(outcome_column)
        html.append(f'<tr><th colspan="4"><b>Panel B: {outcome_display}</b></th></tr>')
        html.append('<tr><th>Metric</th><th>Count</th><th>%</th><th>Mean (when > 0)</th></tr>')
        
        n_positive = (df[outcome_column] > 0).sum()
        pct_positive = n_positive / n_obs * 100 if n_obs > 0 else 0
        mean_positive = df.loc[df[outcome_column] > 0, outcome_column].mean() if n_positive > 0 else 0
        
        html.append(f'<tr><td>Positive</td><td>{n_positive:,}</td><td>{pct_positive:.1f}%</td><td>{mean_positive:,.2f}</td></tr>')
        
        n_zero = (df[outcome_column] == 0).sum()
        pct_zero = n_zero / n_obs * 100 if n_obs > 0 else 0
        html.append(f'<tr><td>Zero</td><td>{n_zero:,}</td><td>{pct_zero:.1f}%</td><td>-</td></tr>')
        
        n_negative = (df[outcome_column] < 0).sum()
        if n_negative > 0:
            pct_negative = n_negative / n_obs * 100
            html.append(f'<tr><td>Negative</td><td>{n_negative:,}</td><td>{pct_negative:.1f}%</td><td>-</td></tr>')
    
    html.append('</table>')
    
    return '\n'.join(html)


def regression_table(
    model_result: Dict[str, Any],
    model_name: str,
    var_formatter: Optional[Callable[[str], str]] = None,
) -> str:
    """
    Generate HTML table for a single regression model result.
    
    Args:
        model_result: Dictionary with keys:
            - 'coefficients': Dict[str, float]
            - 'std_errors': Dict[str, float]
            - 'p_values': Dict[str, float]
            - 'model_type': str (optional)
            - 'dependent_var': str (optional)
            - 'n_obs': int (optional)
            - 'r_squared' or 'pseudo_r2': float (optional)
            - 'error': str (if model failed)
        model_name: Display name for the model
        var_formatter: Optional function to format variable names
    
    Returns:
        HTML table string
    """
    if var_formatter is None:
        var_formatter = _default_var_formatter
    
    if 'error' in model_result:
        return f"<p><b>{model_name}</b>: {model_result['error']}</p>"
    
    html = ['<table class="simpletable">']
    html.append(f'<caption><b>{model_name}</b></caption>')
    
    # Model info
    model_type = model_result.get('model_type', 'unknown').upper()
    dep_var = model_result.get('dependent_var', 'unknown')
    n_obs = model_result.get('n_obs', 0)
    
    html.append(f'<tr><td colspan="5">Model: {model_type} | Dep. Variable: {dep_var} | N = {n_obs:,}</td></tr>')
    
    # R-squared
    if 'r_squared' in model_result:
        html.append(f'<tr><td colspan="5">R-squared: {model_result["r_squared"]:.4f}</td></tr>')
    elif 'pseudo_r2' in model_result:
        html.append(f'<tr><td colspan="5">Pseudo R-squared: {model_result["pseudo_r2"]:.4f}</td></tr>')
    
    # Header
    html.append('<tr>')
    html.append('<th>Variable</th><th>Coef</th><th>Std.Err</th><th>P>|z|</th><th>Sig</th>')
    html.append('</tr>')
    
    # Coefficients
    coeffs = model_result.get('coefficients', {})
    std_errors = model_result.get('std_errors', {})
    p_values = model_result.get('p_values', {})
    
    for var in coeffs.keys():
        coef = coeffs.get(var, 0)
        se = std_errors.get(var, 0)
        pval = p_values.get(var, 1)
        
        sig = _sig_stars(pval)
        
        var_display = var_formatter(var)
        
        html.append('<tr>')
        html.append(f'<td>{var_display}</td>')
        html.append(f'<td>{coef:.4f}</td>')
        html.append(f'<td>{se:.4f}</td>')
        html.append(f'<td>{pval:.4f}</td>')
        html.append(f'<td>{sig}</td>')
        html.append('</tr>')
    
    html.append('</table>')
    html.append('<p class="rl-note">* p < .05, ** p < .01, *** p < .001</p>')
    
    return '\n'.join(html)


def combined_regression_table(
    models: Dict[str, Dict[str, Any]],
    model_order: List[str],
    model_labels: Optional[Dict[str, str]] = None,
    var_formatter: Optional[Callable[[str], str]] = None,
    title: Optional[str] = None,
    show_p_values: bool = False,
) -> str:
    """
    Generate combined HTML table showing multiple regression models side-by-side.
    
    Args:
        models: Dictionary of model results keyed by model name
        model_order: List of model names in display order
        model_labels: Optional dict mapping model names to column labels
        var_formatter: Optional function to format variable names
        title: Optional table title
        show_p_values: If True, include p-value for each coefficient in the cell (e.g. p=0.023)
    
    Returns:
        HTML table string with models as columns
    """
    if var_formatter is None:
        var_formatter = _default_var_formatter
    
    if model_labels is None:
        model_labels = {name: name.title() for name in model_order}
    
    html = ['<table class="simpletable">']
    html.append(f'<caption><b>{title or "Regression Results"}</b></caption>')
    
    # Header row
    html.append('<tr>')
    html.append('<th>Variable</th>')
    for i, name in enumerate(model_order):
        label = model_labels.get(name, name)
        model = models.get(name, {})
        model_type = model.get('model_type', '').upper()
        # Only show model type if available
        if model_type:
            html.append(f'<th>({i+1}) {label}<br/>({model_type})</th>')
        else:
            html.append(f'<th>({i+1}) {label}</th>')
    html.append('</tr>')
    
    # Collect all variables
    all_vars = set()
    for name in model_order:
        model = models.get(name, {})
        if 'coefficients' in model:
            all_vars.update(model['coefficients'].keys())
    
    # Sort: const first, then alphabetical
    sorted_vars = sorted(all_vars, key=lambda x: (0 if x == 'const' else 1, x))
    
    # Data rows
    for var in sorted_vars:
        var_display = var_formatter(var)
        html.append('<tr>')
        html.append(f'<td>{var_display}</td>')
        
        for name in model_order:
            model = models.get(name, {})
            coeffs = model.get('coefficients', {})
            p_values = model.get('p_values', {})
            std_errors = model.get('std_errors', {})
            
            if var in coeffs:
                coef = coeffs[var]
                pval = p_values.get(var, 1)
                se = std_errors.get(var, 0)
                
                sig = _sig_stars(pval)
                
                cell = f'<td>{coef:.4f}{sig}<br/><span class="rl-muted">({se:.4f})</span>'
                if show_p_values:
                    cell += f'<br/><span class="rl-note">p={pval:.4f}</span>'
                cell += '</td>'
                html.append(cell)
            else:
                html.append('<td>-</td>')
        
        html.append('</tr>')
    
    # N row
    html.append('<tr>')
    html.append('<td><b>N</b></td>')
    for name in model_order:
        model = models.get(name, {})
        n_obs = model.get('n_obs', '-')
        html.append(f'<td>{n_obs:,}</td>' if isinstance(n_obs, int) else f'<td>{n_obs}</td>')
    html.append('</tr>')
    
    # R-squared row
    html.append('<tr>')
    html.append('<td><b>R²/Pseudo R²</b></td>')
    for name in model_order:
        model = models.get(name, {})
        r2 = model.get('r_squared', model.get('pseudo_r2', '-'))
        html.append(f'<td>{r2:.4f}</td>' if isinstance(r2, float) else f'<td>{r2}</td>')
    html.append('</tr>')
    
    html.append('</table>')
    html.append('<p class="rl-note">Standard errors in parentheses. * p < .05, ** p < .01, *** p < .001</p>')
    
    return '\n'.join(html)
