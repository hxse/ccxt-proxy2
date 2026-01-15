import sys
import os
import pytest
from fastapi.testclient import TestClient

# Add project root to path
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from src.main import app
from src.router.auth_handler import manager


@pytest.fixture(scope="session")
def client():
    # Override Auth to bypass login
    def mock_user():
        return {"username": "test_user"}

    app.dependency_overrides[manager] = mock_user

    with TestClient(app) as c:
        yield c

    app.dependency_overrides = {}
