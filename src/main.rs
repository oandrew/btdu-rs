#![feature(bigint_helper_methods)]
#![feature(hash_raw_entry)]
#![feature(stdio_locked)]

use std::{collections::{HashMap, HashSet}, env, hash::{BuildHasher, Hasher}, alloc::Layout, ops::{Deref, DerefMut, Range, RangeInclusive}, ffi::{CStr, CString}, fmt, io::Write, rc::Rc};

use nix::{fcntl::{self, OFlag}, libc::{self, c_char}, sys::stat::Mode};
use nix::NixPath;
use anyhow::Result;

use rand::distributions::{Distribution, Uniform};

pub mod btrfs_sys;
mod btrfs;

mod ioctl {
    use crate::btrfs_sys;
    nix::ioctl_readwrite!(btrfs_ioctl_search_v2, btrfs_sys::BTRFS_IOCTL_MAGIC, 17, btrfs_sys::btrfs_ioctl_search_args_v2);
    nix::ioctl_readwrite!(btrfs_ino_lookup, btrfs_sys::BTRFS_IOCTL_MAGIC, 18, btrfs_sys::btrfs_ioctl_ino_lookup_args);
    nix::ioctl_readwrite!(btrfs_ino_paths, btrfs_sys::BTRFS_IOCTL_MAGIC, 35, btrfs_sys::btrfs_ioctl_ino_path_args);
    nix::ioctl_readwrite!(btrfs_logical_ino, btrfs_sys::BTRFS_IOCTL_MAGIC, 36, btrfs_sys::btrfs_ioctl_logical_ino_args);
    nix::ioctl_readwrite!(btrfs_logical_ino_v2, btrfs_sys::BTRFS_IOCTL_MAGIC, 59, btrfs_sys::btrfs_ioctl_logical_ino_args);
}


#[repr(C)]
struct WithMemAfter<T, const N: usize> {
    value: T,
    extra: [u8; N],
}

impl <T: Sized, const N: usize> WithMemAfter<T, N> {
    fn new() -> Self {
        unsafe {
            WithMemAfter {
                value: std::mem::zeroed(), 
                extra: [0; N],
            }
        }
    }

    fn as_mut_ptr(&mut self) -> *mut T {
        &mut self.value
    }

    fn total_size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn extra_ptr(&self) -> *const u8 {
        self.extra.as_ptr()
    }

    fn extra_size(&self) -> usize {
        N
    }

    
}

impl <T: Sized, const N: usize> Deref for WithMemAfter<T, N> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl <T: Sized, const N: usize> DerefMut for WithMemAfter<T, N> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct LogicalInoItem {
    inum: u64,
    offset: u64,
    root: u64,
}

fn btrfs_logical_ino(fd: i32, logical: u64, ignoring_offset: bool, mut cb: impl FnMut(Result<&[LogicalInoItem]>)) {
    let mut data = WithMemAfter::<btrfs_sys::btrfs_data_container, 4096>::new();

    let mut args = btrfs_sys::btrfs_ioctl_logical_ino_args{
        logical: logical,
        size: data.total_size() as u64,
        reserved: Default::default(),
        flags: if ignoring_offset {btrfs_sys::BTRFS_LOGICAL_INO_ARGS_IGNORE_OFFSET as u64} else {0},
        inodes: data.as_mut_ptr() as u64,
    };
    unsafe {
        match ioctl::btrfs_logical_ino_v2(fd, &mut args) {
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

fn btrfs_ino_lookup(fd: i32, root: u64, inum: u64, mut cb: impl FnMut(Result<&CStr>)){
    let mut args = btrfs_sys::btrfs_ioctl_ino_lookup_args{
        treeid: root,
        objectid: inum,
        name: [0; 4080],
    };

    unsafe {
        match ioctl::btrfs_ino_lookup(fd, &mut args) {
            Ok(_) => {
                cb(Ok(CStr::from_ptr(args.name.as_ptr())));
            },
            Err(err) => {
                cb(Err(anyhow::anyhow!(err.to_string())));
            },
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

struct BtrfsKey {
    objectid: u64,
    typ: u8,
    offset: u64
}

impl BtrfsKey {
    const MIN: Self = BtrfsKey::new(u64::MIN, u8::MIN, u64::MIN);
    const MAX: Self = BtrfsKey::new(u64::MAX, u8::MAX, u64::MAX);

    const ALL: RangeInclusive<Self> = Self::MIN..=Self::MAX;
    const fn range_fixed_id(objectid: u64) -> RangeInclusive<Self> {
        Self::new(objectid, u8::MIN, u64::MIN)..=Self::new(objectid, u8::MAX, u64::MAX)
    }

    const fn range_fixed_id_type(objectid: u64, typ: u8) -> RangeInclusive<Self> {
        Self::new(objectid, typ, u64::MIN)..=Self::new(objectid, typ, u64::MAX)
    }

    const fn new(objectid: u64, typ: u8, offset: u64) -> Self { Self { objectid, typ, offset } }

    
    
    fn next(&self) -> Self {
        let (offset, carry1) = self.offset.carrying_add(1, false);
        let (typ, carry2) = self.typ.carrying_add(0, carry1);
        let (objectid, _) = self.objectid.carrying_add(0, carry2);
        BtrfsKey {
            objectid,
            typ,
            offset,
        }
    }

    fn from(h: &btrfs_sys::btrfs_ioctl_search_header) -> Self {
        BtrfsKey {
            objectid: h.objectid,
            typ: h.type_ as u8,
            offset: h.offset,
        }
    }
}

fn btrfs_tree_search_cb(fd: i32, tree_id: u64, range: RangeInclusive<BtrfsKey>, mut cb: impl FnMut(&btrfs_sys::btrfs_ioctl_search_header, &[u8])) -> Result<()> {
    let mut args = WithMemAfter::<btrfs_sys::btrfs_ioctl_search_args_v2, {16*1024}>::new();
    args.key = btrfs_sys::btrfs_ioctl_search_key{
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
            ioctl::btrfs_ioctl_search_v2(fd, args.as_mut_ptr())?;
        }
        if args.key.nr_items == 0 {
            break
        }

        let mut ptr = args.buf.as_ptr() as *const u8;
        let mut last_search_header: *const btrfs_sys::btrfs_ioctl_search_header = std::ptr::null();
        for _ in 0..args.key.nr_items {
            let search_header = unsafe {
                get_and_move_typed::<btrfs_sys::btrfs_ioctl_search_header>(&mut ptr)
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
            BtrfsKey::from(&*last_search_header).next()
        };

        args.key.min_objectid = min_key.objectid;
        args.key.min_type = min_key.typ as u32;
        args.key.min_offset = min_key.offset;
    }

    Ok(())
}


struct SampleTree {
    total: usize,
    children: HashMap<String, SampleTree>,
}

impl Default for SampleTree {
    fn default() -> Self {
        Self::new()
    }
}

impl SampleTree {
    fn new() -> Self {
        Self {
            total: 0,
            children: HashMap::new(),
        }
    }

    fn add<'a>(&mut self, mut path: impl Iterator<Item=&'a str>) {
        self.total += 1;
        match path.next() {
            Some(p) => {
                self.children.raw_entry_mut().from_key(p).or_insert_with(|| (p.to_owned(), SampleTree::new())).1.add(path);
            },
            None => {},
        }   
    }

    fn print_internal<W: fmt::Write>(&self, w: &mut W, total_samples: usize, total_length:u64, depth: usize) -> fmt::Result {
        // let total_samples = self.total;
        let mut c: Vec<_> = self.children.iter().collect();
        c.sort_by_key(|(_,v)| std::cmp::Reverse(v.total));
        for (k,v) in &c {
            let mut s = String::new();
            for i in 0..depth {
                // if i == depth - 1 {
                //     s.push(' ');
                // } else {
                    s.push_str(" ");
                // }
            }
            s.push_str(k);

            let pct = (v.total as f64) / (total_samples as f64);
            let bytes = (total_length as f64 * pct) as u64;
            // if pct > 0.05 {
            writeln!(w, "{:40} {:>8} {:>4.1}% {:>16}", s, v.total,  pct * 100.0, bytesize::to_string(bytes, true))?;
            v.print_internal(w, total_samples, total_length, depth+1)?;
            // }
        }

        Ok(())
    }
}

fn btrfs_find_root_backref(fd:i32, root_id: u64) -> Option<(String, u64)> {
    let mut res: Option<(String, u64)> = None;
    btrfs_tree_search_cb(fd, btrfs_sys::BTRFS_ROOT_TREE_OBJECTID as u64, BtrfsKey::range_fixed_id_type(root_id, btrfs_sys::BTRFS_ROOT_BACKREF_KEY as u8), |sh, data| {
        match sh.type_ {
            btrfs_sys::BTRFS_ROOT_BACKREF_KEY => {
                let root_ref = unsafe {
                    &*(data.as_ptr() as *const btrfs_sys::btrfs_root_ref)
                };
                let name = unsafe {
                    std::str::from_utf8_unchecked(std::slice::from_raw_parts(
                        data.as_ptr().add(std::mem::size_of::<btrfs_sys::btrfs_root_ref>()),
                        root_ref.name_len as usize
                    ))
                };
                res = Some((name.to_owned(), sh.offset));
            },
            _ => {}
        };
    }).unwrap();
    res
}

struct Roots {
    fd: i32,
    m: HashMap<u64, Rc<Vec<String>>>,
}

impl Roots {
    fn new(fd: i32) -> Self {
        Self {
            fd,
            m: HashMap::from([(5, Rc::new(Vec::new()))]),
        }
    }
    fn get_root(&mut self, root_id: u64) -> Rc<Vec<String>> {
        match self.m.get(&root_id) {
            Some(path) => Rc::clone(path),
            None => {
                let (name, parent_id) = btrfs_find_root_backref(self.fd, root_id).unwrap();
                let mut path = Vec::clone(&self.get_root(parent_id)); 
                path.push(name);
                let path_rc = Rc::new(path);
                self.m.insert(root_id, path_rc.clone());
                path_rc
            },
        }
    }
}

unsafe fn __main() -> Result<()> {
    let args: Vec<_> = env::args().collect();
    let fd = fcntl::open(args[1].as_str(), OFlag::O_RDONLY, Mode::empty())?;
    let offset = args[2].as_str().parse::<u64>()?;

    for ignoring_offset in [false, true] {
        btrfs_logical_ino(fd, offset, ignoring_offset, |res| match res {
            Ok(inodes) => {
                println!("ignoring_offset={} offset={} inodes={:?}", ignoring_offset, offset, inodes);
                for inode in inodes {
                    btrfs_ino_lookup(fd, inode.root, inode.inum, |res| match res {
                        Ok(path) => {
                            println!("ignoring_offset={} offset={} inode={} path={:?}", ignoring_offset, offset, inode.inum, path);
                        },
                        Err(_) => {
                            println!("ignoring_offset={} offset={} inode={} err", ignoring_offset, offset, inode.inum);
                        },
                    })
                }
            },
            Err(_) => {
                println!("ignoring_offset={} offset={} err", ignoring_offset, offset);
            },
        });
    }
    

    Ok(())
}


unsafe fn _main() -> Result<()> {
    let args: Vec<_> = env::args().collect();
    let fd = fcntl::open(args[1].as_str(), OFlag::O_RDONLY, Mode::empty())?;
    let samples = args[2].as_str().parse::<usize>()?;

    let mut cnt = 0;

    #[derive(Debug)]
    struct ChunkInfo {
        pos: u64,
        chunk_offset: u64,
        chunk_length: u64,
        chunk_type: u64,
    }

    let mut chunks = Vec::new();
    let mut total_chunk_length = 0;
    btrfs_tree_search_cb(fd, btrfs_sys::BTRFS_CHUNK_TREE_OBJECTID as u64, BtrfsKey::ALL, |sh, data| {
        // println!("{:?}", sh);
        match sh.type_ {
            btrfs_sys::BTRFS_CHUNK_ITEM_KEY => {
                let chunk = unsafe {
                    &*(data.as_ptr() as *const btrfs_sys::btrfs_chunk)
                };
                // println!("{:?}", chunk);
                chunks.push(ChunkInfo{
                    pos: total_chunk_length,
                    chunk_offset:sh.offset, 
                    chunk_length:chunk.length,
                    chunk_type: chunk.type_,
                });
                total_chunk_length += chunk.length;
            },
            _ => {}
        };
        cnt += 1;
    })?;

    println!("total = {}", cnt);
    // println!("{:?}", chunks);

    let mut roots = Roots::new(fd);
 

    let uniform = Uniform::new(0, total_chunk_length);
    let mut rng = rand::thread_rng();

    // let mut trie = sequence_trie::SequenceTrie::new();
    let mut sample_tree = SampleTree::new();
    let mut total_samples = 0;
    let mut start = std::time::Instant::now();
    for _ in 0..samples {
        let random_pos = uniform.sample(&mut rng);
        let random_chunk = chunks.iter().find(|c| {
            random_pos >= c.pos && random_pos < c.pos + c.chunk_length
        }).unwrap();

        total_samples += 1;
        
        match (random_chunk.chunk_type as u32) & btrfs_sys::BTRFS_BLOCK_GROUP_TYPE_MASK {
            btrfs_sys::BTRFS_BLOCK_GROUP_DATA => {
                let random_offset = random_chunk.chunk_offset + (random_pos - random_chunk.pos);
                btrfs_logical_ino(fd, random_offset, false, |res| match res {
                    Ok(inodes) => {
                        for inode in inodes {
                            btrfs_ino_lookup(fd, inode.root, inode.inum, |res| match res {
                                Ok(path) => {
                                    //println!("offset={} inode={} path={:?}", random_offset, inode.inum, path);
                                    let root_path = roots.get_root(inode.root);
                                    let root_path_it = root_path.iter().map(|s| s.as_str());
                                    let inode_path = path.to_str().unwrap().split('/').filter(|s| !s.is_empty());
                                    // sample_tree.add(["DATA"].into_iter().chain(k));
                                    sample_tree.add(root_path_it.chain(inode_path));
                                },
                                Err(_) => {
                                    // let k = ;
                                    // sample_tree.add(["DATA", "ERROR", "INO_LOOKUP"].into_iter());
                                    // sample_tree.add(["ERROR", "INO_LOOKUP"].into_iter());
                                },
                            })
                        }
                    },
                    Err(_) => {
                        // sample_tree.add(["ERROR", "LOGICAL_TO_INO"].into_iter());
                    },
                });


            },
            btrfs_sys::BTRFS_BLOCK_GROUP_METADATA => {
                // sample_tree.add(["METADATA"].into_iter());

            },
            btrfs_sys::BTRFS_BLOCK_GROUP_SYSTEM => {
                // sample_tree.add(["SYSTEM"].into_iter());

            },
            _ => {

            }
        };
    }
    let total_time = start.elapsed();

    


    // println!("{}", sample_tree);
    let mut buf = String::new();
    sample_tree.print_internal(&mut buf, total_samples, total_chunk_length, 0)?;
    std::io::stdout_locked().write_all(buf.as_bytes())?;

    let resolution = total_chunk_length / total_samples as u64;
    println!("elapsed={:?} per_sample={:?} resolution={}", total_time, total_time/(total_samples as u32), bytesize::to_string(resolution, true));

    
    Ok(())
}


fn main() -> Result<()> {
    unsafe {
        _main()
    }

}
