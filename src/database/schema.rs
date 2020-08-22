table! {
    use crate::database::EntryTypeMapping;
    use crate::database::RemoteTypeMapping;
    use diesel::sql_types::*;

    filesystem (id) {
        id -> Text,
        name -> Text,
        entry_type -> EntryTypeMapping,
        created_at -> Timestamp,
        last_modified_at -> Timestamp,
        remote_type -> Nullable<RemoteTypeMapping>,
        inode -> BigInt,
        parent_id -> Nullable<Text>,
        size -> BigInt,
    }
}
