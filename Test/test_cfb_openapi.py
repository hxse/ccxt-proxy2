"""CFB 自动文档须与上游快照一致，代理只补充自身契约。"""

import json
from copy import deepcopy

import pytest

from scripts.sync_cfb_openapi import extract_contract, sync_openapi
from src.cfb_contract import CFB_ROUTES, load_contract
from src.main import app
from src.router.cfb_docs import PROXY_ERRORS, namespace_refs


def test_cfb_documentation_is_copied_from_snapshot_with_local_auth():
    source = load_contract()
    schema = app.openapi()
    assert {path for path in schema["paths"] if path.startswith("/cfb/")} == set(
        CFB_ROUTES
    )
    for path, method in CFB_ROUTES.items():
        copied = namespace_refs(source["paths"][path][method])
        operation = schema["paths"][path][method]
        assert (
            operation["security"]
            == schema["paths"]["/ctp/fetch_balance"]["get"]["security"]
        )
        assert operation["tags"] == ["CFB"] and "401" in operation["responses"]
        for key, value in copied.items():
            if key not in {"tags", "security", "operationId", "responses"}:
                assert operation[key] == value
        for code, response in copied["responses"].items():
            if code in PROXY_ERRORS:
                assert (
                    operation["responses"][code]["content"]["application/json"][
                        "schema"
                    ]["anyOf"][0]
                    == response["content"]["application/json"]["schema"]
                )
            else:
                assert operation["responses"][code] == response
    for section, definitions in source["components"].items():
        if section != "securitySchemes":
            for name, definition in definitions.items():
                assert schema["components"][section]["Cfb_" + name] == namespace_refs(
                    definition
                )


def test_all_document_references_including_cancel_discriminator_resolve():
    schema = app.openapi()

    def visit(value):
        if isinstance(value, dict):
            for item in value.values():
                visit(item)
        elif isinstance(value, list):
            for item in value:
                visit(item)
        elif isinstance(value, str) and value.startswith("#/components/"):
            resolved = schema
            for part in value[2:].split("/"):
                resolved = resolved[part]

    visit(schema)
    body = schema["paths"]["/cfb/cancel_order"]["post"]["requestBody"]["content"][
        "application/json"
    ]["schema"]
    assert set(body["discriminator"]["mapping"]) == {"exchange_order", "session_order"}


def test_order_followup_fields_and_submission_evidence_are_documented():
    schema = app.openapi()
    operation = schema["paths"]["/cfb/fetch_orders"]["get"]
    parameters = {item["name"]: item["schema"] for item in operation["parameters"]}
    identity_fields = {
        "exchange_id",
        "instrument_id",
        "trading_day",
        "front_id",
        "session_id",
        "order_ref",
    }
    assert identity_fields | {"mode", "order_sys_id"} <= parameters.keys()
    assert parameters["mode"]["default"] == "sandbox"
    assert parameters["session_id"]["anyOf"][0]["minimum"] < 0
    assert parameters["order_ref"]["anyOf"][0]["type"] == "string"
    definitions = schema["components"]["schemas"]
    assert set(definitions["Cfb_OrderIdentity"]["required"]) == identity_fields
    for name in ("Cfb_SubmissionResult", "Cfb_ErrorResponse"):
        assert {"identity", "execution", "verification", "order_id"} <= (
            definitions[name]["properties"].keys()
        )
    assert definitions["Cfb_OrdersResult"]["properties"]["consistency"]["enum"] == [
        "stable",
        "changing",
    ]


def test_sync_copies_business_docs_without_rewriting_or_exposing_diagnostic_routes():
    source = load_contract()
    source["paths"]["/v1/status"] = {
        "get": {"responses": {"200": {"description": "status"}}}
    }
    source["paths"]["/cfb/fetch_balance"]["get"]["description"] = "上游新增实盘说明"
    snapshot = deepcopy(source)
    contract = extract_contract(source)
    assert set(contract["paths"]) == set(CFB_ROUTES)
    assert (
        contract["paths"]["/cfb/fetch_balance"] == source["paths"]["/cfb/fetch_balance"]
    )
    assert contract["components"] == source["components"] and source == snapshot


def test_sync_updates_atomically_and_keeps_old_copy_when_document_is_incomplete(
    tmp_path,
):
    source = tmp_path / "source.json"
    destination = tmp_path / "cfb.json"
    document = load_contract()
    source.write_text(json.dumps(document))
    sync_openapi(source.as_uri(), destination)
    assert json.loads(destination.read_text()) == document
    old = destination.read_bytes()
    del document["paths"]["/cfb/cancel_order"]
    source.write_text(json.dumps(document))
    with pytest.raises(ValueError, match="cancel_order"):
        sync_openapi(source.as_uri(), destination)
    assert destination.read_bytes() == old
