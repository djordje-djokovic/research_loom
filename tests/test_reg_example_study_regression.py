from studies.example_study.study_pipeline import create_pipeline, load_study_config

import pytest


@pytest.mark.integration
@pytest.mark.full_matrix
def test_example_study_pipeline_contract(tmp_path):
    pipe = create_pipeline(cache_dir=str(tmp_path / "cache"))
    cfg = load_study_config("base")

    expected_nodes = {"raw_data", "processed_dataframe", "visualization", "statistics", "report"}
    assert expected_nodes.issubset(set(pipe.nodes.keys()))
    assert pipe.nodes["report"].materialize_by_default is True

    # Use a stable intermediate output contract for regression baseline.
    results = pipe.run_pipeline(cfg, materialize=["processed_dataframe"])
    assert "processed_dataframe" in results
    payload = results["processed_dataframe"]
    assert payload["status"] == "completed"
    assert "processed_df" in payload["outputs"]
    assert "summary" in payload["outputs"]
