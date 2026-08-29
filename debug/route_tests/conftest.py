import os
import sys

import pytest

# Add project root to path
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

STATEFUL_DEBUG_ENABLED = os.getenv("CCXT_STATEFUL_DEBUG") == "1"


def pytest_collection_modifyitems(items):
    for item in items:
        item.add_marker(pytest.mark.stateful)


@pytest.fixture(scope="session")
def client():
    if not STATEFUL_DEBUG_ENABLED:
        pytest.skip("stateful sandbox route tests require CCXT_STATEFUL_DEBUG=1")

    from fastapi.testclient import TestClient

    from src.main import app
    from src.router.auth_handler import manager

    # Override Auth to bypass login
    def mock_user():
        return {"username": "test_user"}

    app.dependency_overrides[manager] = mock_user

    try:
        with TestClient(app) as c:
            yield c
    finally:
        app.dependency_overrides.pop(manager, None)
