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
from labox.core.api.saver import save_one
from labox.core.database import ContentRecord
from labox.core.database import ManifestRecord
from labox.server.app import create_app
from labox.test.core_registry_utils import basic_registry

LitestarClient = AsyncTestClient[Litestar]


@pytest.fixture
async def client(engine: AsyncEngine):
    """Create a test client for the Labox server."""
    app = create_app(engine, basic_registry)
    async with AsyncTestClient(app=app) as client:
        yield client


async def test_get_manifest_success(session: AsyncSession, client: LitestarClient):
    """Test getting a manifest by ID."""
    # Populate the database with a manifest
    storable = StorableValue({"data": "test_value"})
    record = await save_one(storable, registry=basic_registry, session=session)

    # Load the contents relationship
    await session.refresh(record, ["contents"])

    # Test the endpoint
    response = await client.get(f"/manifests/{record.id}")
    assert response.status_code == HTTP_200_OK

    data = response.json()
    assert data["id"] == str(record.id)
    assert data["class_id"] == str(record.class_id)
    assert data["unpacker_name"] == record.unpacker_name
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
    records = []
    for i in range(3):
        storable = StorableValue({"data": f"test_value_{i}"})
        record = await save_one(storable, registry=basic_registry, session=session)
        records.append(record)

    # Load all contents for the records
    for record in records:
        await session.refresh(record, ["contents"])

    # Test the endpoint
    response = await client.get("/contents")
    assert response.status_code == HTTP_200_OK

    data = response.json()
    assert len(data) == 3

    # Verify the first content record matches the saved data
    first_content = data[0]
    expected_content = records[0].contents[0]
    assert first_content["id"] == str(expected_content.id)
    assert first_content["manifest_id"] == str(expected_content.manifest_id)
    assert first_content["content_key"] == expected_content.content_key
    assert first_content["content_type"] == expected_content.content_type
    assert first_content["content_hash"] == expected_content.content_hash
    assert first_content["serializer_name"] == expected_content.serializer_name
    assert first_content["storage_name"] == expected_content.storage_name
    assert "created_at" in first_content


async def test_list_contents_with_pagination(session: AsyncSession, client: LitestarClient):
    """Test listing contents with pagination parameters."""
    # Populate the database with multiple manifests
    for i in range(5):
        storable = StorableValue({"data": f"test_value_{i}"})
        await save_one(storable, registry=basic_registry, session=session)

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

    await save_one(storable1, registry=basic_registry, session=session)
    await save_one(storable2, registry=basic_registry, session=session)

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
    await save_one(storable, registry=basic_registry, session=session)

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


async def test_get_content_data_success(session: AsyncSession, client: LitestarClient):
    """Test streaming the actual data from a content record."""
    # Populate the database with a storable
    test_data = {"data": "test_value_for_streaming"}
    storable = StorableValue(test_data)
    await save_one(storable, registry=basic_registry, session=session)

    # Get a content ID
    result = await session.execute(select(ContentRecord.id).limit(1))
    content_id_raw = result.scalar_one()
    content_id = UUID(content_id_raw) if isinstance(content_id_raw, str) else content_id_raw

    # Test the streaming endpoint
    response = await client.get(f"/contents/{content_id}/data")
    assert response.status_code == HTTP_200_OK

    # The response should contain the serialized data
    assert len(response.content) > 0


async def test_get_content_data_not_found(client: LitestarClient):
    """Test streaming data from a non-existent content record returns 404."""
    non_existent_id = uuid4()
    response = await client.get(f"/contents/{non_existent_id}/data")
    assert response.status_code == HTTP_404_NOT_FOUND
    assert f"Content {non_existent_id} not found" in response.json()["detail"]
