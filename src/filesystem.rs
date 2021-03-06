use std::cell::Cell;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::ops::Add;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use chrono::{NaiveDateTime, Utc};
use fuser::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyOpen, ReplyWrite, Request, TimeOrNow,
};
use users::{Groups, Users, UsersCache};

use crate::{cache::DataCache, drive_client::DriveClient};
use crate::{
    database::entity::{EntryType, FilesystemEntry},
    database::filesystem::FilesystemRepository,
    drive_client::FileCreateRequest,
    indexing::IndexWriter,
};

const TTL: Duration = Duration::from_secs(1);
const ROOT_INODE: u64 = 1;
const SHARED_DRIVES_INODE: u64 = 2;
const BLOCK_SIZE: u32 = 512;

pub(crate) struct GdriveFs {
    repository: Arc<FilesystemRepository>,
    file_handles: Mutex<HashMap<u64, String>>,
    latest_file_handle: Mutex<Cell<u64>>,
    drive_client: Arc<DriveClient>,
    pending_writes: HashMap<u64, Vec<u8>>,
    users_cache: UsersCache,
    cache: Arc<DataCache>,
}

impl GdriveFs {
    pub(crate) fn new(
        repository: Arc<FilesystemRepository>,
        drive_client: Arc<DriveClient>,
        cache: Arc<DataCache>,
    ) -> Self {
        Self {
            repository,
            file_handles: Mutex::new(HashMap::new()),
            latest_file_handle: Mutex::new(Cell::new(0)),
            drive_client,
            pending_writes: HashMap::new(),
            users_cache: unsafe { UsersCache::with_all_users() },
            cache,
        }
    }

    fn get_attr(&self, entry: &FilesystemEntry) -> FileAttr {
        let created = UNIX_EPOCH.add(Duration::from_secs(entry.created_at.timestamp() as u64));
        let modified = UNIX_EPOCH.add(Duration::from_secs(
            entry.last_modified_at.timestamp() as u64
        ));

        let kind = GdriveFs::entry_type_to_file_type(&entry.entry_type);

        let uid = self.users_cache.get_current_uid();
        let gid = self.users_cache.get_current_gid();

        FileAttr {
            ino: entry.inode as u64,
            size: entry.size as u64,
            blocks: 0,
            atime: created, // 1970-01-01 00:00:00
            mtime: modified,
            ctime: modified,
            crtime: created,
            kind,
            perm: entry.mode as u16,
            nlink: 2,
            uid,
            gid,
            blksize: BLOCK_SIZE,
            padding: 0,
            rdev: 0,
            flags: 0,
        }
    }

    fn entry_type_to_file_type(entry_type: &EntryType) -> FileType {
        match entry_type {
            EntryType::Drive => FileType::Directory,
            EntryType::File => FileType::RegularFile,
            EntryType::Directory => FileType::Directory,
        }
    }

    fn make_file_handle(&self, remote_id: String) -> u64 {
        let lock = self.latest_file_handle.lock().unwrap();
        let fh = lock.get() + 1;

        self.file_handles.lock().unwrap().insert(fh, remote_id);

        lock.set(fh);

        fh
    }

    fn flush_handle(&mut self, handle: u64) -> Result<()> {
        match self.pending_writes.remove(&handle) {
            Some(data) => {
                log::info!("Flushing data to Google.");

                let guard = self.file_handles.lock().unwrap();
                let file_id = guard.get(&handle).unwrap();
                self.drive_client.write_file(file_id, data.as_slice())?;

                Ok(())
            }
            None => Ok(()),
        }
    }
}

impl Filesystem for GdriveFs {
    fn lookup(
        &mut self,
        _request: &Request,
        parent_inode: u64,
        entry_name: &OsStr,
        reply: ReplyEntry,
    ) {
        // We need to look up top level directories, which are the drives in our case
        let entry = self
            .repository
            .find_entry_as_child(parent_inode as i64, entry_name);
        match entry {
            Some(fs_entry) => reply.entry(&TTL, &self.get_attr(&fs_entry), 0),
            None => reply.error(libc::ENOENT),
        }
    }

    fn getattr(&mut self, request: &Request, inode: u64, reply: ReplyAttr) {
        let entry = self.repository.find_entry_for_inode(inode);
        match entry {
            Some(fs_entry) => reply.attr(&TTL, &self.get_attr(&fs_entry)),
            None => {
                println!(
                    r#"
                            Call Errored: getattr()
                            Request: {:?}
                            Inode: {}
                            "#,
                    request, inode
                );

                reply.error(libc::ENOENT)
            }
        }
    }

    fn fsync(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        log::info!("fsync() being called for inode {}", _ino);

        if self.pending_writes.contains_key(&_fh) {
            match self.flush_handle(_fh) {
                Ok(_) => reply.ok(),
                Err(_) => reply.error(libc::EIO),
            }

            return;
        }

        reply.ok();
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        log::info!("release() being called for inode {}", _ino);

        if self.pending_writes.contains_key(&_fh) {
            log::info!("Pending writes detected, flushing.");
            match self.flush_handle(_fh) {
                Ok(_) => {
                    log::info!("Successfully flushed pending data.");

                    reply.ok();
                }
                Err(error) => {
                    log::error!("I/O error while flushing: {:?}", error);

                    reply.error(libc::EIO);
                }
            }

            return;
        }

        let mut guard = self.file_handles.lock().unwrap();
        guard.remove(&_fh);

        reply.ok();
    }

    fn create(
        &mut self,
        _req: &Request<'_>,
        parent_inode: u64,
        file_name: &OsStr,
        _mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        // We cannot support creating folders/files in the root or shared drives directory, so
        //  we reply with "permission denied". Possibly, we could map create() calls in the shared
        //  drives directory to the "Create Shared Drive" API call on Google later.
        if parent_inode == ROOT_INODE || parent_inode == SHARED_DRIVES_INODE {
            reply.error(libc::EPERM);
            return;
        }

        let parent_directory = self.repository.find_entry_for_inode(parent_inode);

        if parent_directory.is_none() {
            reply.error(libc::ENOENT);
            return;
        }

        let parent_directory = parent_directory.unwrap();
        if parent_directory.entry_type == EntryType::File {
            reply.error(libc::ENOTDIR);
            return;
        }

        if self
            .repository
            .find_entry_as_child(parent_inode as i64, file_name)
            .is_some()
        {
            reply.error(libc::EEXIST);
            return;
        }

        let current_time = Utc::now();
        let create_response = self.drive_client.create_file(FileCreateRequest {
            created_time: current_time,
            modified_time: current_time,
            name: file_name.to_str().unwrap().to_string(),
            parents: vec![parent_directory.id],
            mime_type: None,
        });

        match create_response {
            Ok(file) => {
                // TODO: Improve error handling
                let remote_id = file.id.clone();
                IndexWriter::process_create_immediately(file, &self.repository);

                let cache_entry = self
                    .repository
                    .find_entry_by_id(&remote_id)
                    .expect("Freshly created entry is missing. That sucks.");
                let attr = self.get_attr(&cache_entry);
                let handle = self.make_file_handle(remote_id);

                reply.created(&TTL, &attr, 1, handle, 0);
            }
            Err(err) => {
                log::error!("Unexpected API error while creating a file: {:?}", err);
                reply.error(libc::EIO);
            }
        }
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _offset: i64,
        _data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        // We are limited by the backing Vec<> of pending changes here
        if (_offset as u64 + _data.len() as u64) > usize::MAX as u64 {
            reply.error(libc::E2BIG);
            return;
        }

        let mut pending_data = self.pending_writes.remove(&_fh).unwrap_or_else(Vec::new);

        let write_index = _offset as usize;
        _data
            .iter()
            .cloned()
            .enumerate()
            .for_each(|(data_index, byte)| {
                if write_index + data_index >= pending_data.len() {
                    pending_data.push(byte);
                } else {
                    pending_data[write_index + data_index] = byte;
                }
            });

        self.pending_writes.insert(_fh, pending_data);

        reply.written(_data.len() as u32);
    }

    fn rmdir(&mut self, _req: &Request<'_>, parent_inode: u64, _name: &OsStr, reply: ReplyEmpty) {
        // removing the "meta" directories and removing shared drives isn't supported
        if parent_inode == ROOT_INODE || parent_inode == SHARED_DRIVES_INODE {
            reply.error(libc::EPERM);
            return;
        }

        self.unlink(_req, parent_inode, _name, reply)
    }

    fn unlink(&mut self, _req: &Request<'_>, parent_ino: u64, name: &OsStr, reply: ReplyEmpty) {
        if parent_ino == ROOT_INODE || parent_ino == SHARED_DRIVES_INODE {
            reply.error(libc::EPERM);
            return;
        }

        let entry = self.repository.find_entry_as_child(parent_ino as i64, name);

        if entry.is_none() {
            reply.error(libc::ENOENT);
            return;
        }

        let entry = entry.unwrap();
        let result = self.drive_client.delete_file(entry.id.as_str());

        match result {
            Ok(_) => {
                let _ = self.repository.remove_entry_by_remote_id(entry.id.as_str());
                reply.ok();
            }
            Err(error) => {
                log::error!("Failed to delete file [unlink()]: {:?}", error);
                reply.error(libc::EIO);
            }
        }
    }

    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        // TODO: Figure out if it's smart to just swallow changes that we don't support or if it would be better to return something like ENOSYS
        //  not sure yet, whether ENOSYS would cause more problems than it would fix, so let's keep it like this for now.

        let entry = self.repository.find_entry_for_inode(ino);

        if entry.is_none() {
            reply.error(libc::ENOENT);
            return;
        }

        let update_result = self.repository.transaction::<_, anyhow::Error, _>(|| {
            if let Some(mode) = mode {
                self.repository
                    .update_mode_by_inode(ino as i64, mode as i32)?;
            }

            if let Some(size) = size {
                self.repository
                    .update_size_by_inode(ino as i64, size as i64)?;
            }

            if let Some(atime) = atime {
                let atime = match atime {
                    TimeOrNow::SpecificTime(time) => time,
                    TimeOrNow::Now => SystemTime::now(),
                };
                let res = atime.duration_since(UNIX_EPOCH);
                if let Ok(duration) = res {
                    self.repository.update_last_access_by_inode(
                        ino as i64,
                        NaiveDateTime::from_timestamp(
                            duration.as_secs() as i64,
                            duration.subsec_nanos(),
                        ),
                    )?;
                }
            }

            if let Some(mtime) = mtime {
                let mtime = match mtime {
                    TimeOrNow::SpecificTime(time) => time,
                    TimeOrNow::Now => SystemTime::now(),
                };
                let res = mtime.duration_since(UNIX_EPOCH);
                if let Ok(duration) = res {
                    self.repository.update_last_modification_by_inode(
                        ino as i64,
                        NaiveDateTime::from_timestamp(
                            duration.as_secs() as i64,
                            duration.subsec_nanos(),
                        ),
                    )?;
                }
            }

            Ok(())
        });

        if let Err(error) = update_result {
            log::error!("Failed to update attributes [setattr()]: {:?}", error);
            reply.error(libc::EIO);
            return;
        }

        let entry = self.repository.find_entry_for_inode(ino);

        reply.attr(&TTL, &self.get_attr(&entry.unwrap()));
    }

    fn open(&mut self, _req: &Request, _ino: u64, flags: i32, reply: ReplyOpen) {
        let fs_entry = self.repository.find_entry_for_inode(_ino);

        if fs_entry.is_none() {
            reply.error(libc::ENOENT);
            return;
        }

        let fs_entry = fs_entry.unwrap();
        let remote_id = fs_entry.id.clone();
        let file_handle = self.make_file_handle(remote_id);
        reply.opened(file_handle, 0);

        // Create virtual chunks of the file to speed up downloading
        self.cache.initialize_chunks(fs_entry);
    }

    fn read(
        &mut self,
        _req: &Request,
        _ino: u64,
        _fh: u64,
        _offset: i64,
        _size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        let lock = self.file_handles.lock().unwrap();
        let remote_id = lock.get(&_fh);

        if remote_id.is_none() {
            reply.error(libc::ENOENT);
            return;
        }

        let remote_id = remote_id.unwrap().clone();

        println!(
            "READ CALLED - Offset: {}, Size: {}, Inode: {}",
            _offset, _size, _ino
        );

        // TODO: handle possible race condition when the file gets deleted on the remote side. In that case
        // we wouldn't know and still have an active file handle which could lead to a lot of problems.
        let file_entry = self.repository.find_entry_by_id(&remote_id).unwrap();

        // When we know the size is 0, we can reply directly without investing the HTTP(s) overhead to fetch 0 bytes from remote.
        if file_entry.size == 0 {
            reply.data(&[]);
            return;
        }

        log::info!("Locking for get_bytes_blocking()");
        let data = self.cache.get_bytes_blocking(
            remote_id,
            _offset,
            std::cmp::min((_offset as u32 + _size - 1) as i64, file_entry.size),
        );

        if data.is_err() {
            log::error!("Failed to read bytes from cache: {:?}", data.unwrap_err());
            reply.error(libc::EIO);
            return;
        }

        let data = data.unwrap();

        println!(
            "Replying with some data for this request :: Offset: {}, Size: {}, Inode: {}",
            _offset, _size, _ino
        );
        println!("Our reply is {} long", data.len());
        reply.data(data.as_ref());
    }

    fn mkdir(
        &mut self,
        _req: &Request<'_>,
        parent_inode: u64,
        file_name: &OsStr,
        mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        if parent_inode == ROOT_INODE || parent_inode == SHARED_DRIVES_INODE {
            reply.error(libc::EPERM);
            return;
        }

        let parent_directory = self.repository.find_entry_for_inode(parent_inode);

        if parent_directory.is_none() {
            reply.error(libc::ENOENT);
            return;
        }

        let parent_directory = parent_directory.unwrap();
        if parent_directory.entry_type == EntryType::File {
            reply.error(libc::ENOTDIR);
            return;
        }

        if self
            .repository
            .find_entry_as_child(parent_inode as i64, file_name)
            .is_some()
        {
            reply.error(libc::EEXIST);
            return;
        }

        let current_time = Utc::now();
        let create_response = self.drive_client.create_file(FileCreateRequest {
            created_time: current_time,
            modified_time: current_time,
            name: file_name.to_str().unwrap().to_string(),
            parents: vec![parent_directory.id],
            mime_type: Some("application/vnd.google-apps.folder".to_string()),
        });

        match create_response {
            Ok(file) => {
                // TODO: Improve error handling
                let remote_id = file.id.clone();
                let new_inode = IndexWriter::process_create_immediately(file, &self.repository);

                let _ = self.repository.update_mode_by_inode(new_inode, mode as i32);

                let cache_entry = self
                    .repository
                    .find_entry_by_id(&remote_id)
                    .expect("Freshly created entry is missing. That sucks.");
                let attr = self.get_attr(&cache_entry);

                reply.entry(&TTL, &attr, 1);
            }
            Err(err) => {
                log::error!("Unexpected API error while creating a directory: {:?}", err);
                reply.error(libc::EIO);
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

        let iterator = results.iter().skip(offset as usize);

        let mut current_offset = offset + 1;
        for entry in iterator {
            let result = reply.add(
                entry.inode as u64,
                current_offset,
                GdriveFs::entry_type_to_file_type(&entry.entry_type),
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

            reply.error(libc::ENOENT);
        }
    }
}
