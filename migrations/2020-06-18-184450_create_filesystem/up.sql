CREATE TABLE filesystem (
    inode INTEGER PRIMARY KEY,
    parent_inode INTEGER NULL,
    name VARCHAR NOT NULL,
    entry_type VARCHAR(9) CHECK(entry_type IN ('file', 'directory')) NOT NULL DEFAULT 'file',
    created_at INTEGER NOT NULL,
    last_modified_at INTEGER NOT NULL,
    remote_type VARCHAR CHECK(remote_type IN ('team_drive', 'directory', 'file')),
    remote_id VARCHAR,

    FOREIGN KEY(parent_inode) REFERENCES filesystem(inode)
);
