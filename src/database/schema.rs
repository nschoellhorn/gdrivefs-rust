table! {
    use crate::database::entity::EntryTypeMapping;
    use crate::database::entity::RemoteTypeMapping;
    use diesel::sql_types::*;

    filesystem (id) {
        id -> Text,
        name -> Text,
        entry_type -> EntryTypeMapping,
        created_at -> Timestamp,
        last_modified_at -> Timestamp,
        last_accessed_at -> Timestamp,
        mode -> Integer,
        remote_type -> Nullable<RemoteTypeMapping>,
        inode -> BigInt,
        size -> BigInt,
        parent_id -> Nullable<Text>,
        parent_inode -> Nullable<BigInt>,
    }
}

table! {
    use diesel::sql_types::*;
    use crate::database::entity::RemoteTypeMapping;

    index_state (drive_id) {
        drive_id -> Text,
        page_token -> BigInt,
        remote_type -> RemoteTypeMapping,
    }
}
