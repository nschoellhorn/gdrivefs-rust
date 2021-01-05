use crate::database::EntryType::File;
use crate::database::{EntryType, FilesystemEntry, FilesystemRepository};
use crate::drive_client::DriveClient;
use chrono::{DateTime, NaiveDateTime};
use diesel::dsl::exists;
use diesel::prelude::*;
use diesel::{select, SqliteConnection};
use fuse::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyOpen, ReplyStatfs, Request,
};
use libc::ENOENT;
use std::boxed::Box;
use std::cell::Cell;
use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::ops::Add;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const ROOT_DIR_ATTR: FileAttr = FileAttr {
    ino: 1,
    size: 0,
    blocks: 0,
    atime: UNIX_EPOCH,
    mtime: UNIX_EPOCH,
    ctime: UNIX_EPOCH,
    crtime: UNIX_EPOCH,
    kind: FileType::Directory,
    perm: 0o755,
    nlink: 2,
    uid: 1000,
    gid: 1000,
    rdev: 0,
    flags: 0,
};

fn get_attr(entry: &FilesystemEntry) -> FileAttr {
    let created = UNIX_EPOCH.add(Duration::from_secs(entry.created_at.timestamp() as u64));
    let modified = UNIX_EPOCH.add(Duration::from_secs(
        entry.last_modified_at.timestamp() as u64
    ));

    let kind = match entry.entry_type {
        EntryType::File => FileType::RegularFile,
        EntryType::Directory => FileType::Directory,
    };

    FileAttr {
        ino: entry.inode as u64,
        size: entry.size as u64,
        blocks: 0,
        atime: created, // 1970-01-01 00:00:00
        mtime: modified,
        ctime: modified,
        crtime: created,
        kind,
        perm: 0o755,
        nlink: 2,
        uid: 1000, // TODO: which user id do we wanna use? Just the current user?
        gid: 1000, // TODO: which group id do we wanna use? Just the current group?
        rdev: 0,
        flags: 0,
    }
}

const TTL: Duration = Duration::from_secs(1);

pub(crate) struct GdriveFs {
    repository: Arc<FilesystemRepository>,
    file_handles: Mutex<HashMap<u64, String>>,
    latest_file_handle: Mutex<Cell<u64>>,
    drive_client: Arc<DriveClient>,
    root_inode: u64,
    shared_drives_inode: u64,
    my_drives_inode: u64,
}

impl GdriveFs {
    pub(crate) fn new(
        repository: Arc<FilesystemRepository>,
        drive_client: Arc<DriveClient>,
        root_inode: u64,
        shared_drives_inode: u64,
        my_drives_inode: u64
    ) -> Self {
        Self {
            repository,
            file_handles: Mutex::new(HashMap::new()),
            latest_file_handle: Mutex::new(Cell::new(0)),
            drive_client,
            root_inode,
            shared_drives_inode,
            my_drives_inode,
        }
    }

    fn to_file_type(entry_type: &EntryType) -> FileType {
        match entry_type {
            EntryType::File => FileType::RegularFile,
            EntryType::Directory => FileType::Directory,
        }
    }

    fn make_file_handle(&self, remote_id: String) -> u64 {
        let lock = self.latest_file_handle.lock().unwrap();
        let fh = lock.get() + 1;

        self.file_handles
            .lock()
            .unwrap()
            .insert(fh, remote_id.clone());

        lock.set(fh);

        fh
    }
}

impl Filesystem for GdriveFs {
    /*fn opendir(&mut self, request: &Request, inode: u64, flags: u32, reply: ReplyOpen) {
        println!(r#"
        Call: opendir()
        Request: {:?}
        Inode: {}
        Flags: {}
        "#, request, inode, flags);

        reply.error(ENOENT);
    }*/

    fn open(&mut self, _req: &Request, _ino: u64, _flags: u32, reply: ReplyOpen) {
        let fs_entry = self.repository.find_entry_for_inode(_ino);

        if fs_entry.is_none() {
            reply.error(ENOENT);
            return;
        }

        let file_handle = self.make_file_handle(fs_entry.unwrap().id);
        reply.opened(file_handle, _flags);
    }

    fn read(
        &mut self,
        _req: &Request,
        _ino: u64,
        _fh: u64,
        _offset: i64,
        _size: u32,
        reply: ReplyData,
    ) {
        let lock = self.file_handles.lock().unwrap();
        let remote_id = lock.get(&_fh);

        if remote_id.is_none() {
            reply.error(ENOENT);
            return;
        }

        let remote_id = remote_id.unwrap().clone();

        println!(
            "READ CALLED - Offset: {}, Size: {}, Inode: {}",
            _offset, _size, _ino
        );
        let client = Arc::clone(&self.drive_client);
        let data = client
            .get_file_content(
                remote_id.as_str(),
                _offset as u64,
                (_offset + _size as i64) as u64 - 1,
            )
            .expect("Unable to retrieve file content");

        use std::borrow::Borrow;
        println!(
            "Replying with some data for this request :: Offset: {}, Size: {}, Inode: {}",
            _offset, _size, _ino
        );
        println!("Our reply is {} long", data.len());
        reply.data(data.borrow());
    }

    fn lookup(
        &mut self,
        request: &Request,
        parent_inode: u64,
        entry_name: &OsStr,
        reply: ReplyEntry,
    ) {
        // We need to look up top level directories, which are the drives in our case
        let entry = self
            .repository
            .find_entry_as_child(parent_inode as i64, entry_name);
        match entry {
            Some(fs_entry) => {
                reply.entry(&TTL, &get_attr(&fs_entry), 0)
            },
            None => {
                reply.error(ENOENT)
            }
        }
    }

    fn getattr(&mut self, request: &Request, inode: u64, reply: ReplyAttr) {
        match inode {
            1 => reply.attr(&TTL, &ROOT_DIR_ATTR),
            _ => {
                let entry = self.repository.find_entry_for_inode(inode);
                match entry {
                    Some(fs_entry) => reply.attr(&TTL, &get_attr(&fs_entry)),
                    None => {
                        println!(
                            r#"
                            Call Errored: getattr()
                            Request: {:?}
                            Inode: {}
                            "#,
                            request, inode
                        );

                        reply.error(ENOENT)
                    }
                }
            }
        }
    }

    /*fn statfs(&mut self, request: &Request, inode: u64, reply: ReplyStatfs) {
        println!(r#"
        Call: statfs()
        Request: {:?}
        Inode: {}
        "#, request, inode);

        reply.error(ENOENT);
    }*/

    fn readdir(
        &mut self,
        request: &Request,
        inode: u64,
        file_handle: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let inode_exists = self.repository.inode_exists(inode);

        let results = self.repository.find_all_entries_in_parent(inode);

        let iterator = results
            .iter()
            .skip(offset as usize);

        let mut current_offset = offset + 1;
        for entry in iterator {
            let result = reply.add(
                entry.inode as u64,
                current_offset,
                GdriveFs::to_file_type(&entry.entry_type),
                entry.name.clone(),
            );

            if result {
                break;
            }

            current_offset += 1;
        }

        if inode_exists {
            reply.ok();
        } else {
            println!(
                r#"
                Call Errored: readdir()
                Request: {:?}
                Inode: {}
                File Handle: {}
                Offset: {}
                "#,
                request, inode, file_handle, offset
            );

            reply.error(ENOENT);
        }
    }

    fn releasedir(
        &mut self,
        request: &Request,
        inode: u64,
        file_handle: u64,
        flags: u32,
        reply: ReplyEmpty,
    ) {
        // we currently dont really have anything to release
        reply.ok()
    }
}
