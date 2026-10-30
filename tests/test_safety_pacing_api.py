"""
tests/test_safety_pacing_api.py — Architecture boundary and API surface tests.

Requirement:
- Pacing package does NOT import the safety kill-switch/flags module (inspect imports via ast).
- Safety exposes no mutator callable from pacing.
"""

import ast
import pathlib
import pytest

import smartdialer.safety.controller as safety_controller


def test_pacing_package_does_not_import_kill_switch_or_flags():
    """
    Parse AST of every Python file in smartdialer/pacing/ and assert
    that neither SystemFlag nor system_flags nor set_kill_switch are imported.
    """
    pacing_dir = pathlib.Path(__file__).parent.parent / "smartdialer" / "pacing"
    assert pacing_dir.exists(), f"Directory {pacing_dir} must exist"

    forbidden_names = {"SystemFlag", "system_flags", "set_kill_switch", "kill_switch"}

    for py_file in pacing_dir.glob("*.py"):
        source = py_file.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(py_file))

        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    for forbidden in forbidden_names:
                        assert forbidden not in alias.name, (
                            f"File {py_file.name} illegal import: {alias.name}"
                        )
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    for forbidden in forbidden_names:
                        assert forbidden not in node.module, (
                            f"File {py_file.name} illegal from-import module: {node.module}"
                        )
                for alias in node.names:
                    for forbidden in forbidden_names:
                        assert forbidden != alias.name, (
                            f"File {py_file.name} illegal import name: {alias.name}"
                        )


def test_safety_controller_exposes_no_mutator_for_pacing():
    """
    Verify safety module exports only pure evaluators and in-transaction enforcers,
    with no disable/override/bypass handles.
    """
    forbidden_mutators = [
        "disable",
        "disable_safety",
        "set_override",
        "bypass_safety",
        "set_approved_calls",
        "force_approve",
    ]

    for attr in dir(safety_controller):
        for forbidden in forbidden_mutators:
            assert forbidden not in attr.lower(), (
                f"Safety module must not expose mutator '{attr}'"
            )
