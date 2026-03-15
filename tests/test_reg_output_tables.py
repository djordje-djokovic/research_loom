import pytest

from output.html_tables import (
    combined_regression_table,
    correlation_matrix_table,
    descriptive_stats_table,
    regression_table,
    sample_composition_table,
    simpletable,
)


@pytest.mark.core
@pytest.mark.smoke
def test_simpletable_renders_expected_structure():
    html = simpletable(rows=[["a", "1"], ["b", "2"]], headers=["col1", "col2"], caption="C")
    assert '<table class="simpletable">' in html
    assert "<caption>C</caption>" in html
    assert "<th>col1</th>" in html
    assert "<td>a</td>" in html


@pytest.mark.core
def test_descriptive_stats_table_smoke():
    data = [{"x": 1.0, "y": 5.0}, {"x": 2.0, "y": 7.0}, {"x": 3.0, "y": 9.0}]
    html = descriptive_stats_table(data, variables=["x", "y"], title="Desc")
    assert "Descriptive Statistics" in html or "Desc" in html
    assert "Mean" in html
    assert "X" in html


@pytest.mark.core
def test_correlation_matrix_table_smoke():
    data = [{"x": 1, "y": 2, "z": 3}, {"x": 2, "y": 3, "z": 4}, {"x": 3, "y": 4, "z": 5}]
    html = correlation_matrix_table(data, variables=["x", "y", "z"], show_significance=False)
    assert "Correlation Matrix" in html
    assert "(1) X" in html
    assert "1.00" in html


@pytest.mark.core
def test_sample_composition_table_smoke():
    data = [
        {"company_id": "c1", "founder_id": "f1", "sale": 10},
        {"company_id": "c1", "founder_id": "f2", "sale": 0},
        {"company_id": "c2", "founder_id": "f3", "sale": 5},
    ]
    html = sample_composition_table(
        data,
        id_column="company_id",
        unit_column="founder_id",
        outcome_column="sale",
    )
    assert "Panel A: Overall" in html
    assert "Panel B" in html
    assert "Observations" in html


@pytest.mark.core
def test_regression_table_and_combined_regression_table_smoke():
    m1 = {
        "model_type": "ols",
        "dependent_var": "y",
        "n_obs": 100,
        "r_squared": 0.42,
        "coefficients": {"const": 1.0, "x": 0.5},
        "std_errors": {"const": 0.1, "x": 0.2},
        "p_values": {"const": 0.001, "x": 0.02},
    }
    m2 = {
        "model_type": "logit",
        "dependent_var": "y_bin",
        "n_obs": 80,
        "pseudo_r2": 0.11,
        "coefficients": {"const": -0.4, "x": 0.8},
        "std_errors": {"const": 0.15, "x": 0.3},
        "p_values": {"const": 0.03, "x": 0.008},
    }

    single = regression_table(m1, model_name="Model 1")
    assert "Model 1" in single
    assert "R-squared" in single
    assert "X" in single

    combined = combined_regression_table(
        models={"m1": m1, "m2": m2},
        model_order=["m1", "m2"],
        model_labels={"m1": "Baseline", "m2": "Alt"},
        show_p_values=True,
    )
    assert "Regression Results" in combined
    assert "Baseline" in combined
    assert "Alt" in combined
    assert "p=" in combined


@pytest.mark.core
def test_output_tables_edge_and_fallback_messages():
    assert "No data available for descriptive statistics" in descriptive_stats_table([], variables=["x"])
    assert "Need at least 2 variables for correlation matrix" in correlation_matrix_table(
        [{"x": 1}], variables=["x"]
    )
    assert "<b>ErrModel</b>: failed to fit" in regression_table({"error": "failed to fit"}, "ErrModel")
