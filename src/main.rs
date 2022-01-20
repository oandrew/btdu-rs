#![feature(bigint_helper_methods)]
#![feature(hash_raw_entry)]
#![feature(stdio_locked)]

use std::{collections::{HashMap, HashSet}, env, hash::{BuildHasher, Hasher}, alloc::Layout, ops::{Deref, DerefMut, Range, RangeInclusive}, ffi::{CStr, CString}, fmt, io::Write, rc::Rc};

use nix::{fcntl::{self, OFlag}, libc::{self, c_char}, sys::stat::Mode};
use nix::NixPath;
use anyhow::Result;
use clap::Parser;

use rand::distributions::{Distribution, Uniform};

mod btrfs;

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

 

    fn print_internal<W: fmt::Write>(&self, w: &mut W, total_samples: usize, total_length:u64, min_disk_fraction: Option<f64>, depth: usize) -> fmt::Result {
        let mut c: Vec<_> = self.children.iter().collect();
        c.sort_by_key(|(_,v)| std::cmp::Reverse(v.total));
        for (k,v) in &c {
            let disk_fraction = (v.total as f64) / (total_samples as f64);
            let disk_bytes = (total_length as f64 * disk_fraction) as u64;

            match min_disk_fraction {
                Some(min_disk_fraction) if disk_fraction < min_disk_fraction => continue,
                _ => {},
            }

            let path = { 
                let mut path =  String::new();
                for i in 0..depth {
                    path.push_str(" ");
                }
                path.push('/');
                path.push_str(k);
                path
            };

            writeln!(w, "{:40} {:>8} {:>4.1}% {:>16}", path, v.total,  disk_fraction * 100.0, bytesize::to_string(disk_bytes, true))?;
            v.print_internal(w, total_samples, total_length, min_disk_fraction, depth+1)?;
        }

        Ok(())
    }

    fn print<W: fmt::Write>(&self, w: &mut W, total_samples: usize, total_length:u64, min_disk_fraction: Option<f64>) -> fmt::Result {
        self.print_internal(w, total_samples, total_length, min_disk_fraction, 0)
    }
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
                let (name, parent_id) = btrfs::find_root_backref(self.fd, root_id).unwrap();
                let mut path = Vec::clone(&self.get_root(parent_id)); 
                path.push(name);
                let path_rc = Rc::new(path);
                self.m.insert(root_id, path_rc.clone());
                path_rc
            },
        }
    }
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Number of samples to take
    #[clap(short, long, default_value_t = 100000)]
    samples: u64,

    /// Filter tree by min disk usage percentage 0..100
    #[clap(short, long, default_value_t = 1.0)]
    min_pct: f64,

    /// Mounted btrfs path
    path: String,
}

fn main() -> Result<()> {
    let args = Args::parse();
    // let args: Vec<_> = env::args().collect();
    let fd = fcntl::open(args.path.as_str(), OFlag::O_RDONLY, Mode::empty())?;
    // let samples = args[2].as_str().parse::<usize>()?;
    let samples = args.samples;



    #[derive(Debug)]
    struct ChunkInfo {
        pos: u64,
        chunk_offset: u64,
        chunk_length: u64,
        chunk_type: u64,
    }

    let mut chunks = Vec::new();
    let mut total_chunk_length = 0;
    btrfs::tree_search_cb(fd, btrfs::BTRFS_CHUNK_TREE_OBJECTID as u64, btrfs::SearchKey::ALL, |sh, data| {
        match sh.type_ {
            btrfs::BTRFS_CHUNK_ITEM_KEY => {
                let chunk = unsafe {
                    &*(data.as_ptr() as *const btrfs::btrfs_chunk)
                };
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
    })?;

    let mut roots = Roots::new(fd);
 

    let uniform = Uniform::new(0, total_chunk_length);
    let mut rng = rand::thread_rng();

    let mut sample_tree = SampleTree::new();
    let mut total_samples = 0;
    let mut start = std::time::Instant::now();
    for _ in 0..samples {
        let random_pos = uniform.sample(&mut rng);
        let random_chunk = chunks.iter().find(|c| {
            random_pos >= c.pos && random_pos < c.pos + c.chunk_length
        }).unwrap();

        total_samples += 1;
        
        match (random_chunk.chunk_type as u32) & btrfs::BTRFS_BLOCK_GROUP_TYPE_MASK {
            btrfs::BTRFS_BLOCK_GROUP_DATA => {
                let random_offset = random_chunk.chunk_offset + (random_pos - random_chunk.pos);
                btrfs::logical_ino(fd, random_offset, false, |res| match res {
                    Ok(inodes) => {
                        for inode in inodes {
                            btrfs::ino_lookup(fd, inode.root, inode.inum, |res| match res {
                                Ok(path) => {
                                    let root_path = roots.get_root(inode.root);
                                    let root_path_it = root_path.iter().map(|s| s.as_str());
                                    let inode_path = path.to_str().unwrap().split('/').filter(|s| !s.is_empty());
                                    sample_tree.add(root_path_it.chain(inode_path));
                                },
                                Err(_) => {
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
            btrfs::BTRFS_BLOCK_GROUP_METADATA => {
                // sample_tree.add(["METADATA"].into_iter());

            },
            btrfs::BTRFS_BLOCK_GROUP_SYSTEM => {
                // sample_tree.add(["SYSTEM"].into_iter());

            },
            _ => {

            }
        };
    }
    let total_time = start.elapsed();

    let mut buf = String::new();
    sample_tree.print(&mut buf, total_samples, total_chunk_length, Some(args.min_pct / 100.0))?;
    std::io::stdout_locked().write_all(buf.as_bytes())?;

    let resolution = total_chunk_length / total_samples as u64;
    println!("elapsed={:?} per_sample={:?} resolution={}", total_time, total_time/(total_samples as u32), bytesize::to_string(resolution, true));

    
    Ok(())
}


