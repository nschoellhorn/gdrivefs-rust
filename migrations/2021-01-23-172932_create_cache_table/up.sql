CREATE TABLE object_chunk (
    id INTEGER PRIMARY KEY,
    file_id VARCHAR NOT NULL,
    chunk_sequence INTEGER NOT NULL,
    last_read INTEGER,
    last_write INTEGER,
    cached_size INTEGER NOT NULL,
    byte_from INTEGER NOT NULL,
    byte_to INTEGER NOT NULL,
    is_complete INTEGER CHECK(is_complete IN (0, 1)) NOT NULL DEFAULT 0,
    object_name VARCHAR NOT NULL
);

CREATE INDEX object_chunk_access_index
	on object_chunk (last_read, last_write);

CREATE INDEX object_chunk_from_index
	on object_chunk (byte_from);

CREATE INDEX object_chunk_to_index
	on object_chunk (byte_to);

CREATE INDEX object_chunk_name_index
    on object_chunk (object_name);