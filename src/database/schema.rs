table! {
    use crate::database::EntryTypeMapping;
    use crate::database::RemoteTypeMapping;
    use diesel::sql_types::*;

    filesystem (inode) {
        inode -> BigInt,
        parent_inode -> BigInt,
        name -> Text,
        entry_type -> EntryTypeMapping,
        created_at -> Timestamp,
        last_modified_at -> Timestamp,
        remote_type -> Nullable<RemoteTypeMapping>,
        remote_id -> Nullable<Text>,
        size -> BigInt,
    }
}
