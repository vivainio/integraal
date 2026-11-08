from pathlib import Path

from cfn_typed.testing import assert_matches_file, assert_template_ok

from stack import build


def test_reference_template() -> None:
    template = build()
    assert_template_ok(template)
    assert_matches_file(template, Path(__file__).resolve().parents[2] / "crates/control-plane/template.yaml")
    resources = template["Resources"]
    assert resources["WorkerService"]["Type"] == "AWS::ECS::Service"
    assert resources["FlowLogsRoute"]["Properties"]["AuthorizationType"] == "JWT"
    assert resources["FlowLogsRoute"]["Properties"]["AuthorizationScopes"]
    assert resources["ControlFunction"]["Properties"]["Environment"]["Variables"]["INTEGRAAL_CONTROL_CONFIG"] == resources["WorkerTask"]["Properties"]["ContainerDefinitions"][0]["Environment"][0]["Value"]
    assert all(resources[f"{name}Tick"]["Type"] == "AWS::Scheduler::Schedule" for name in ("Minute", "Hour", "Day"))
    assert all(resources[name]["DeletionPolicy"] == "Retain" for name in ("ScriptsBucket", "DataBucket", "FlowLogsBucket", "StateTable"))
