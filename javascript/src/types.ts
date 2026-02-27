/**
 * Mirrors the Python `ContentRecord` dataclass from `labox.core.database`.
 */
export interface ContentRecord {
    id: string;
    manifest_id: string;
    content_key: string;
    content_type: string;
    content_encoding: string | null;
    content_hash: string;
    content_hash_algorithm: string;
    content_size: number;
    serializer_name: string;
    serializer_config: string;
    serializer_type: number;
    storage_name: string;
    storage_config: string;
    created_at: string;
}
