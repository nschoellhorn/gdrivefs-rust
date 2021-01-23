CREATE TABLE object_cache_meta (
    file_id VARCHAR PRIMARY KEY,
    last_read INTEGER,
    last_write INTEGER,
    cached_size INTEGER NOT NULL,
);

CREATE INDEX object_cache_meta_access_index
	on object_cache_meta (last_read, last_write);