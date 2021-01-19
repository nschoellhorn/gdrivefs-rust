CREATE TABLE index_state (
    drive_id VARCHAR PRIMARY KEY,
    page_token INTEGER,
    remote_type VARCHAR CHECK(remote_type IN ('own_drive', 'team_drive'))
)