CREATE TABLE filesystem (
    id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    entry_type VARCHAR(9) CHECK(entry_type IN ('file', 'directory')) NOT NULL DEFAULT 'file',
    created_at INTEGER NOT NULL,
    last_modified_at INTEGER NOT NULL,
    remote_type VARCHAR CHECK(remote_type IN ('team_drive', 'directory', 'file')),
    inode INTEGER NULL,
    parent_id VARCHAR NULL,

    FOREIGN KEY(parent_id) REFERENCES filesystem(id)
);
