from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from databricks_agent_notebooks.runtime.home import HOME_ENV_VAR, ensure_runtime_home, resolve_runtime_home


def test_env_override_sets_runtime_home(tmp_path: Path) -> None:
    home = resolve_runtime_home(env={HOME_ENV_VAR: str(tmp_path / "custom-home")})

    assert home.root == tmp_path / "custom-home"
    assert home.runtimes_dir == tmp_path / "custom-home" / "data" / "runtimes"
    assert home.kernels_dir == tmp_path / "custom-home" / "data" / "kernels"


def test_default_runtime_home_uses_platformdirs() -> None:
    with patch("databricks_agent_notebooks.runtime.home.PlatformDirs") as mock_platform_dirs:
        mock_platform_dirs.return_value.user_data_path = "/tmp/dan-home"
        home = resolve_runtime_home(env={})

    assert home.root == Path("/tmp/dan-home")
    assert home.cache_dir == Path("/tmp/dan-home/cache")
    assert home.installations_dir == Path("/tmp/dan-home/state/installations")
    assert home.bin_dir == Path("/tmp/dan-home/bin")


def test_ensure_runtime_home_creates_expected_directories(tmp_path: Path) -> None:
    home = resolve_runtime_home(env={HOME_ENV_VAR: str(tmp_path / "runtime-home")})

    ensure_runtime_home(home)

    assert home.cache_dir.is_dir()
    assert home.runtimes_dir.is_dir()
    assert home.kernels_dir.is_dir()
    assert home.installations_dir.is_dir()
    assert home.links_dir.is_dir()
    assert home.logs_dir.is_dir()
