use std::{collections::{HashMap, HashSet}, env, hash::{BuildHasher, Hasher}, alloc::Layout, ops::{Deref, DerefMut, Range, RangeInclusive}, ffi::{CStr, CString}, fmt, io::Write, rc::Rc};

use nix::{fcntl::{self, OFlag}, libc::{self, c_char}, sys::stat::Mode};
use nix::NixPath;
use anyhow::Result;

mod btrfs_sys;
mod util;

pub use btrfs_sys::*;
use util::{WithMemAfter, WithMemAfterTrait};

mod ioctl {
    use super::*;
    nix::ioctl_readwrite!(search_v2, BTRFS_IOCTL_MAGIC, 17, btrfs_ioctl_search_args_v2);
    nix::ioctl_readwrite!(ino_lookup, BTRFS_IOCTL_MAGIC, 18, btrfs_ioctl_ino_lookup_args);
    nix::ioctl_readwrite!(ino_paths, BTRFS_IOCTL_MAGIC, 35, btrfs_ioctl_ino_path_args);
    nix::ioctl_readwrite!(logical_ino, BTRFS_IOCTL_MAGIC, 36, btrfs_ioctl_logical_ino_args);
    nix::ioctl_readwrite!(logical_ino_v2, BTRFS_IOCTL_MAGIC, 59, btrfs_ioctl_logical_ino_args);
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct LogicalInoItem {
    pub inum: u64,
    pub offset: u64,
    pub root: u64,
}


pub fn logical_ino(fd: i32, logical: u64, ignoring_offset: bool, mut cb: impl FnMut(Result<&[LogicalInoItem]>)) {
    let mut data = WithMemAfter::<btrfs_data_container, 4096>::new();

    let mut args = btrfs_ioctl_logical_ino_args{
        logical: logical,
        size: data.total_size() as u64,
        reserved: Default::default(),
        flags: if ignoring_offset {BTRFS_LOGICAL_INO_ARGS_IGNORE_OFFSET as u64} else {0},
        inodes: data.as_mut_ptr() as u64,
    };
    unsafe {
        match ioctl::logical_ino_v2(fd, &mut args) {
            Ok(_) => {
                let inodes = std::slice::from_raw_parts(
                    data.extra_ptr() as *const LogicalInoItem, 
                    (data.elem_cnt / 3) as usize,
                );
                cb(Ok(inodes));
            },
            Err(err) => {
                cb(Err(anyhow::anyhow!(err.to_string())));
            },
        }
    }  
}

pub fn ino_lookup(fd: i32, root: u64, inum: u64, mut cb: impl FnMut(Result<&CStr>)){
    let mut args = btrfs_ioctl_ino_lookup_args{
        treeid: root,
        objectid: inum,
        name: [0; 4080],
    };

    unsafe {
        match ioctl::ino_lookup(fd, &mut args) {
            Ok(_) => {
                cb(Ok(CStr::from_ptr(args.name.as_ptr())));
            },
            Err(err) => {
                cb(Err(anyhow::anyhow!(err.to_string())));
            },
        }
    }
}

pub struct SearchKey {
    pub objectid: u64,
    pub typ: u8,
    pub offset: u64
}

impl SearchKey {
    pub const MIN: Self = SearchKey::new(u64::MIN, u8::MIN, u64::MIN);
    pub const MAX: Self = SearchKey::new(u64::MAX, u8::MAX, u64::MAX);

    pub const ALL: RangeInclusive<Self> = Self::MIN..=Self::MAX;
    pub const fn range_fixed_id(objectid: u64) -> RangeInclusive<Self> {
        Self::new(objectid, u8::MIN, u64::MIN)..=Self::new(objectid, u8::MAX, u64::MAX)
    }

    pub const fn range_fixed_id_type(objectid: u64, typ: u8) -> RangeInclusive<Self> {
        Self::new(objectid, typ, u64::MIN)..=Self::new(objectid, typ, u64::MAX)
    }

    pub const fn new(objectid: u64, typ: u8, offset: u64) -> Self { Self { objectid, typ, offset } }

    
    
    pub fn next(&self) -> Self {
        let (offset, carry1) = self.offset.carrying_add(1, false);
        let (typ, carry2) = self.typ.carrying_add(0, carry1);
        let (objectid, _) = self.objectid.carrying_add(0, carry2);
        SearchKey {
            objectid,
            typ,
            offset,
        }
    }

    fn from(h: &btrfs_ioctl_search_header) -> Self {
        SearchKey {
            objectid: h.objectid,
            typ: h.type_ as u8,
            offset: h.offset,
        }
    }
}

unsafe fn get_and_move(ptr: &mut *const u8, n: usize) -> *const u8 {
    let res = *ptr;
    *ptr = (*ptr).add(n);
    res
}

unsafe fn get_and_move_typed<T: Sized>(ptr: &mut *const u8) -> *const T {
    let res = *ptr as *const T;
    *ptr = (*ptr).add(std::mem::size_of::<T>());
    res
}

pub fn tree_search_cb(fd: i32, tree_id: u64, range: RangeInclusive<SearchKey>, mut cb: impl FnMut(&btrfs_ioctl_search_header, &[u8])) -> Result<()> {
    let mut args = WithMemAfter::<btrfs_ioctl_search_args_v2, {16*1024}>::new();
    args.key = btrfs_ioctl_search_key{
        tree_id: tree_id,
        min_objectid: range.start().objectid,
        max_objectid: range.end().objectid,
        min_offset: range.start().offset,
        max_offset: range.end().offset,
        min_transid: u64::MIN,
        max_transid: u64::MAX,
        min_type: range.start().typ as u32,
        max_type: range.end().typ as u32,
        nr_items: u32::MAX,

        unused: 0,
        unused1: 0,
        unused2: 0,
        unused3: 0,
        unused4: 0,
        
    };
    args.buf_size = args.extra_size() as u64;

    loop {
        args.key.nr_items = u32::MAX;
        unsafe {
            ioctl::search_v2(fd, args.as_mut_ptr())?;
        }
        if args.key.nr_items == 0 {
            break
        }

        let mut ptr = args.buf.as_ptr() as *const u8;
        let mut last_search_header: *const btrfs_ioctl_search_header = std::ptr::null();
        for _ in 0..args.key.nr_items {
            let search_header = unsafe {
                get_and_move_typed::<btrfs_ioctl_search_header>(&mut ptr)
            };

            let data = unsafe {
                std::slice::from_raw_parts(
                    get_and_move(&mut ptr, (*search_header).len as usize),
                    (*search_header).len as usize
                )
            };
            last_search_header = search_header;
            unsafe {
                cb(&*search_header, data);
            }
        }

        let min_key = unsafe {
            SearchKey::from(&*last_search_header).next()
        };

        args.key.min_objectid = min_key.objectid;
        args.key.min_type = min_key.typ as u32;
        args.key.min_offset = min_key.offset;
    }

    Ok(())
}

// struct TreeSearchState {
//     pos: usize,
//     ptr: *const btrfs_ioctl_search_header,   
// }
// pub struct TreeSearch {
//     fd: i32,
//     tree_id: u64,
//     range: RangeInclusive<SearchKey>,
//     args: Option<WithMemAfter::<btrfs_ioctl_search_args_v2, {16*1024}>>,
//     pos: Option<TreeSearchState>,
// }

// impl Iterator for TreeSearch {
//     type Item;

//     fn next(&mut self) -> Option<Self::Item> {
//         match &mut self.pos {
//             Some(pos) => {
                
//             }
//         }
//         loop {
//             args.key.nr_items = u32::MAX;
//             unsafe {
//                 ioctl::search_v2(fd, args.as_mut_ptr())?;
//             }
//             if args.key.nr_items == 0 {
//                 break
//             }
    
//             let mut ptr = args.buf.as_ptr() as *const u8;
//             let mut last_search_header: *const btrfs_ioctl_search_header = std::ptr::null();
//             for _ in 0..args.key.nr_items {
//                 let search_header = unsafe {
//                     get_and_move_typed::<btrfs_ioctl_search_header>(&mut ptr)
//                 };
    
//                 let data = unsafe {
//                     std::slice::from_raw_parts(
//                         get_and_move(&mut ptr, (*search_header).len as usize),
//                         (*search_header).len as usize
//                     )
//                 };
//                 last_search_header = search_header;
//                 unsafe {
//                     cb(&*search_header, data);
//                 }
//             }
    
//             let min_key = unsafe {
//                 SearchKey::from(&*last_search_header).next()
//             };
    
//             args.key.min_objectid = min_key.objectid;
//             args.key.min_type = min_key.typ as u32;
//             args.key.min_offset = min_key.offset;
//         }
    
//         Ok(())
//     }
// }


// pub fn tree_search_once(fd: i32, tree_id: u64, range: RangeInclusive<SearchKey>, args) -> TreeSearch {
//     let mut args = WithMemAfter::<btrfs_ioctl_search_args_v2, {16*1024}>::new();
//     args.key = btrfs_ioctl_search_key{
//         tree_id: tree_id,
//         min_objectid: range.start().objectid,
//         max_objectid: range.end().objectid,
//         min_offset: range.start().offset,
//         max_offset: range.end().offset,
//         min_transid: u64::MIN,
//         max_transid: u64::MAX,
//         min_type: range.start().typ as u32,
//         max_type: range.end().typ as u32,
//         nr_items: u32::MAX,

//         unused: 0,
//         unused1: 0,
//         unused2: 0,
//         unused3: 0,
//         unused4: 0,
        
//     };
//     args.buf_size = args.extra_size() as u64;

//     TreeSearch{
//         fd,
//         tree_id,
//         range,
//         args
//     }
// }


pub fn find_root_backref(fd:i32, root_id: u64) -> Option<(String, u64)> {
    let mut res: Option<(String, u64)> = None;
    tree_search_cb(fd, BTRFS_ROOT_TREE_OBJECTID as u64, SearchKey::range_fixed_id_type(root_id, BTRFS_ROOT_BACKREF_KEY as u8), |sh, data| {
        match sh.type_ {
            BTRFS_ROOT_BACKREF_KEY => {
                let root_ref = unsafe {
                    &*(data.as_ptr() as *const btrfs_root_ref)
                };
                let name = unsafe {
                    std::str::from_utf8_unchecked(std::slice::from_raw_parts(
                        data.as_ptr().add(std::mem::size_of::<btrfs_root_ref>()),
                        root_ref.name_len as usize
                    ))
                };
                res = Some((name.to_owned(), sh.offset));
            },
            _ => {}
        };
    }).unwrap();
    if res.is_none() {
        eprintln!("find_root_backref root_id={} not found", root_id);
    }
    res
}