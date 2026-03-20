from __future__ import annotations

import shutil

import pytest

from app.storage.settings import get_paths, load_settings, save_settings


@pytest.fixture(autouse=True)
def clean_app_state():
    paths = get_paths()
    for path in (paths.imports, paths.calc_runs, paths.logs, paths.exports, paths.app_data / "analysis_runs"):
        if path.exists():
            shutil.rmtree(path)
        path.mkdir(parents=True, exist_ok=True)
    for file_name in ("transactions.json", "import_batches.json", "integration_status.json", "settings.json"):
        target = paths.app_data / file_name
        if target.exists():
            target.unlink()
    for secret in paths.secrets.glob("*"):
        secret.unlink()
    settings = load_settings()
    settings["manual_rate_file"] = None
    settings["disclaimer_acknowledged"] = False
    save_settings(settings)
    yield
