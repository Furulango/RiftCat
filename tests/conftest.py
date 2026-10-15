import pytest


@pytest.fixture(autouse=True)
def entorno(monkeypatch, tmp_path):
    monkeypatch.setenv("RIOT_API_KEY", "RGAPI-test")
    monkeypatch.chdir(tmp_path)
