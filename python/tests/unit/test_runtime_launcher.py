from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from unittest.mock import patch


def test_main_execs_bootstrap_from_contract_and_clears_spark_home(tmp_path: Path) -> None:
    from databricks_agent_notebooks.runtime.launcher import main

    contract_path = tmp_path / "launcher-contract.json"
    contract_path.write_text(
        json.dumps(
            {
                "contract_version": "1",
                "kernel_id": "scala212-dbr-connect",
                "display_name": "Scala 2.12 (Databricks Connect)",
                "language": "scala",
                "argv": [
                    sys.executable,
                    "-m",
                    "databricks_agent_notebooks.runtime.launcher",
                    "--launcher-contract",
                    str(contract_path),
                    "--connection-file",
                    "{connection_file}",
                ],
                "env": {},
                "runtime_id": "scala212-dbr-connect",
                "launcher_path": sys.executable,
                "bootstrap_argv": [
                    "java",
                    "--add-opens=java.base/java.nio=ALL-UNNAMED",
                    "coursier",
                    "--connection-file",
                    "{connection_file}",
                ],
            }
        ),
        encoding="utf-8",
    )

    with patch.dict(os.environ, {"SPARK_HOME": "/opt/spark", "OTHER_ENV": "1"}, clear=True):
        with patch("databricks_agent_notebooks.runtime.launcher.os.execvpe") as execvpe:
            main(
                [
                    "--launcher-contract",
                    str(contract_path),
                    "--connection-file",
                    str(tmp_path / "connection.json"),
                ]
            )

    execvpe.assert_called_once()
    executable, argv, env = execvpe.call_args.args
    assert executable == "java"
    assert argv == [
        "java",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "coursier",
        "--connection-file",
        str(tmp_path / "connection.json"),
    ]
    assert env["SPARK_HOME"] == ""
    assert env["OTHER_ENV"] == "1"
