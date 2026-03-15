import pytest

from pipeline.core import Node


def _identity_loader(inputs, config):
    return {"value": config.get("value", 1)}


def _from_input(input_name):
    def _fn(inputs, config):
        return {"value": inputs[input_name]["value"] + config.get("delta", 0)}
    return _fn


@pytest.mark.core
@pytest.mark.smoke
def test_add_node_rejects_unknown_inputs(pipeline):
    with pytest.raises(ValueError, match="Unknown inputs"):
        pipeline.add_node(
            Node(
                name="broken",
                func=_identity_loader,
                inputs=["does_not_exist"],
                config_section="broken",
            )
        )


@pytest.mark.core
@pytest.mark.smoke
def test_cycle_detection_rolls_back_state(pipeline):
    pipeline.add_node(Node("a", _identity_loader, [], "a"))
    pipeline.add_node(Node("b", _from_input("a"), ["a"], "b"))
    pipeline.add_node(Node("c", _from_input("b"), ["b"], "c"))

    original_inputs = list(pipeline.nodes["a"].inputs)
    original_order = pipeline._get_execution_order()

    with pytest.raises(ValueError, match="Cycle detected"):
        # Overwrite node "a" to depend on "c", which introduces a cycle.
        pipeline.add_node(Node("a", _from_input("c"), ["c"], "a"))

    # Current behavior removes the overwritten node on rollback attempt.
    # This assertion captures existing behavior pre-refactor.
    assert "a" not in pipeline.nodes
    assert "b" in pipeline.nodes
    assert "c" in pipeline.nodes


@pytest.mark.core
@pytest.mark.smoke
def test_execution_order_preserves_insertion_tie_break(pipeline):
    pipeline.add_node(Node("left", _identity_loader, [], "left"))
    pipeline.add_node(Node("right", _identity_loader, [], "right"))
    pipeline.add_node(Node("join", _from_input("left"), ["left", "right"], "join"))
    pipeline.add_node(Node("tail", _from_input("join"), ["join"], "tail"))

    order = pipeline._get_execution_order()
    assert order.index("left") < order.index("right")
    assert order.index("left") < order.index("join")
    assert order.index("right") < order.index("join")
    assert order.index("join") < order.index("tail")


@pytest.mark.core
def test_plan_only_includes_required_closure(pipeline):
    pipeline.add_node(Node("root", _identity_loader, [], "root"))
    pipeline.add_node(Node("branch_a", _from_input("root"), ["root"], "branch_a"))
    pipeline.add_node(Node("branch_b", _from_input("root"), ["root"], "branch_b"))
    pipeline.add_node(
        Node("final_a", _from_input("branch_a"), ["branch_a"], "final_a", materialize_by_default=True)
    )
    pipeline.add_node(Node("final_b", _from_input("branch_b"), ["branch_b"], "final_b"))

    config = {"root": {}, "branch_a": {}, "branch_b": {}, "final_a": {}, "final_b": {}}
    plan = pipeline.plan(config, materialize=["final_a"])

    assert set(plan.keys()) == {"root", "branch_a", "final_a"}


@pytest.mark.core
def test_unknown_materialize_target_has_no_work_and_returns_empty(
    tiny_pipeline_factory, call_counter, tmp_path
):
    p = tiny_pipeline_factory(call_counter, tmp_path / "cache_unknown_target")
    config = {"source": {"value": 1}, "mid": {"delta": 1}, "out": {"delta": 1}}

    plan = p.plan(config, materialize=["typo_node"])
    result = p.run_pipeline(config, materialize=["typo_node"])

    assert plan == {}
    assert result == {}


@pytest.mark.core
def test_plan_and_run_required_closure_parity_for_target(
    tiny_pipeline_factory, call_counter, tmp_path
):
    p = tiny_pipeline_factory(call_counter, tmp_path / "cache_plan_run_parity")
    config = {"source": {"value": 3}, "mid": {"delta": 2}, "out": {"delta": 4}}

    plan = p.plan(config, materialize=["mid"])
    result = p.run_pipeline(config, materialize=["mid"])

    assert set(plan.keys()) == {"source", "mid"}
    assert set(result.keys()) == {"mid"}
