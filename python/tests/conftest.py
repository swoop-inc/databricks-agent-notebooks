"""Shared fixtures for the standalone databricks_agent_notebooks test suite."""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture()
def sample_markdown(tmp_path: Path) -> Path:
    content = """\
---
databricks:
  profile: nonhealth-prod
  cluster: rnd-alpha
  language: scala
---

# Test Notebook

```scala
val x = 1 + 1
println(x)
```
"""
    path = tmp_path / "test.md"
    path.write_text(content, encoding="utf-8")
    return path


@pytest.fixture()
def sample_markdown_no_frontmatter(tmp_path: Path) -> Path:
    content = """\
# Test Notebook

```scala
val x = 1 + 1
println(x)
```
"""
    path = tmp_path / "test_no_frontmatter.md"
    path.write_text(content, encoding="utf-8")
    return path


@pytest.fixture()
def sample_dbr_python(tmp_path: Path) -> Path:
    content = """\
# Databricks notebook source
# COMMAND ----------

# MAGIC %md
# MAGIC # Hello from Python

# COMMAND ----------

x = 1 + 1
print(x)
"""
    path = tmp_path / "test_notebook.py"
    path.write_text(content, encoding="utf-8")
    return path


@pytest.fixture()
def sample_dbr_scala(tmp_path: Path) -> Path:
    content = """\
// Databricks notebook source
// COMMAND ----------

// MAGIC %md
// MAGIC # Hello from Scala

// COMMAND ----------

val x = 1 + 1
println(x)
"""
    path = tmp_path / "test_notebook.scala"
    path.write_text(content, encoding="utf-8")
    return path


@pytest.fixture()
def sample_dbr_sql(tmp_path: Path) -> Path:
    content = """\
-- Databricks notebook source
-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Hello from SQL

-- COMMAND ----------

SELECT 1 + 1 AS result
"""
    path = tmp_path / "test_notebook.sql"
    path.write_text(content, encoding="utf-8")
    return path
