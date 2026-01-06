from uuid import UUID
from uuid import uuid4

import pytest
from litestar import Litestar
from litestar.status_codes import HTTP_200_OK
from litestar.status_codes import HTTP_404_NOT_FOUND
from litestar.testing import AsyncTestClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import AsyncSession

from labox.builtin.storables import StorableValue
from labox.core.database import ContentRecord
from labox.core.database import ManifestRecord
from labox.server.app import create_app
from tests.core_api_utils import assert_save_load_equivalence
from tests.core_registry_utils import basic_registry

LitestarClient = AsyncTestClient[Litestar]


@pytest.fixture
async def client(engine: AsyncEngine):
    """Create a test client for the Labox server."""
    app = create_app(engine)
    async with AsyncTestClient(app=app) as client:
        yield client


async def test_get_manifest_success(session: AsyncSession, client: LitestarClient):
    """Test getting a manifest by ID."""
    # Populate the database with a manifest
    storable = StorableValue({"data": "test_value"})
    await assert_save_load_equivalence(storable, basic_registry, session)

    # Get the manifest ID from the session
    await session.commit()
    result = await session.execute(select(ManifestRecord.id).limit(1))
    manifest_id = result.scalar_one()

    # Test the endpoint
    response = await client.get(f"/manifests/{manifest_id}")
    assert response.status_code == HTTP_200_OK

    data = response.json()
    assert data["id"] == str(manifest_id)
    assert "class_id" in data
    assert "unpacker_name" in data
    assert "created_at" in data
    assert "contents" not in data  # Excluded by DTO


async def test_get_manifest_not_found(client: LitestarClient):
    """Test getting a non-existent manifest returns 404."""
    non_existent_id = uuid4()
    response = await client.get(f"/manifests/{non_existent_id}")
    assert response.status_code == HTTP_404_NOT_FOUND
    assert f"Manifest {non_existent_id} not found" in response.json()["detail"]


async def test_list_contents_empty(client: LitestarClient):
    """Test listing contents when database is empty."""
    response = await client.get("/contents")
    assert response.status_code == HTTP_200_OK
    assert response.json() == []


async def test_list_contents_with_data(session: AsyncSession, client: LitestarClient):
    """Test listing contents with data in the database."""
    # Populate the database with multiple manifests
    for i in range(3):
        storable = StorableValue({"data": f"test_value_{i}"})
        await assert_save_load_equivalence(storable, basic_registry, session)

    await session.commit()

    # Test the endpoint
    response = await client.get("/contents")
    assert response.status_code == HTTP_200_OK

    data = response.json()
    assert len(data) == 3

    # Verify the structure of the first content record
    first_content = data[0]
    assert "id" in first_content
    assert "manifest_id" in first_content
    assert "content_key" in first_content
    assert "content_type" in first_content
    assert "content_hash" in first_content
    assert "serializer_name" in first_content
    assert "storage_name" in first_content
    assert "created_at" in first_content


async def test_list_contents_with_pagination(session: AsyncSession, client: LitestarClient):
    """Test listing contents with pagination parameters."""
    # Populate the database with multiple manifests
    for i in range(5):
        storable = StorableValue({"data": f"test_value_{i}"})
        await assert_save_load_equivalence(storable, basic_registry, session)

    await session.commit()

    # Test with limit
    response = await client.get("/contents?limit=2")
    assert response.status_code == HTTP_200_OK
    assert len(response.json()) == 2

    # Test with offset
    response = await client.get("/contents?offset=2&limit=2")
    assert response.status_code == HTTP_200_OK
    assert len(response.json()) == 2

    # Test with large offset
    response = await client.get("/contents?offset=10")
    assert response.status_code == HTTP_200_OK
    assert len(response.json()) == 0


async def test_list_contents_filtered_by_manifest(session: AsyncSession, client: LitestarClient):
    """Test filtering contents by manifest_id."""
    # Create multiple manifests
    storable1 = StorableValue({"data": "manifest_1"})
    storable2 = StorableValue({"data": "manifest_2"})

    await assert_save_load_equivalence(storable1, basic_registry, session)
    await assert_save_load_equivalence(storable2, basic_registry, session)
    await session.commit()

    # Get all manifests
    result = await session.execute(select(ManifestRecord.id).order_by(ManifestRecord.created_at))
    manifest_ids_raw = [row[0] for row in result.fetchall()]
    # Convert to UUIDs to ensure proper formatting with hyphens
    manifest_ids = [UUID(mid) if isinstance(mid, str) else mid for mid in manifest_ids_raw]
    assert len(manifest_ids) == 2

    # Filter by first manifest
    response = await client.get(f"/contents?manifest_id={manifest_ids[0]}")
    assert response.status_code == HTTP_200_OK

    data = response.json()
    assert len(data) >= 1
    for content in data:
        assert content["manifest_id"] == str(manifest_ids[0])

    # Filter by second manifest
    response = await client.get(f"/contents?manifest_id={manifest_ids[1]}")
    assert response.status_code == HTTP_200_OK

    data = response.json()
    assert len(data) >= 1
    for content in data:
        assert content["manifest_id"] == str(manifest_ids[1])


async def test_get_content_success(session: AsyncSession, client: LitestarClient):
    """Test getting a content record by ID."""
    # Populate the database
    storable = StorableValue({"data": "test_value"})
    await assert_save_load_equivalence(storable, basic_registry, session)
    await session.commit()

    # Get a content ID
    result = await session.execute(select(ContentRecord.id).limit(1))
    content_id_raw = result.scalar_one()
    # Convert to UUID to ensure proper formatting with hyphens
    content_id = UUID(content_id_raw) if isinstance(content_id_raw, str) else content_id_raw

    # Test the endpoint
    response = await client.get(f"/contents/{content_id}")
    assert response.status_code == HTTP_200_OK

    data = response.json()
    assert data["id"] == str(content_id)
    assert "manifest_id" in data
    assert "content_key" in data
    assert "serializer_name" in data
    assert "storage_name" in data


async def test_get_content_not_found(client: LitestarClient):
    """Test getting a non-existent content record returns 404."""
    non_existent_id = uuid4()
    response = await client.get(f"/contents/{non_existent_id}")
    assert response.status_code == HTTP_404_NOT_FOUND
    assert f"Content {non_existent_id} not found" in response.json()["detail"]
