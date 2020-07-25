INSERT INTO filesystem (
    inode,
    parent_inode,
    name,
    entry_type,
    created_at,
    last_modified_at
) VALUES (
    2, 1,
    'Eigene Ablagen',
    'directory',
    datetime('now'),
    datetime('now')
), (
    3, 1,
    'Geteilte Ablagen',
    'directory',
    datetime('now'),
    datetime('now')
);
