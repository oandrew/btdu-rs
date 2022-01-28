#![feature(stdio_locked)]
#![feature(hash_raw_entry)]
use std::{collections::{HashMap, HashSet, VecDeque}, env, hash::{BuildHasher, Hasher}, alloc::Layout, ops::{Deref, DerefMut, Range, RangeInclusive, AddAssign}, ffi::{CStr, CString}, fmt, io::Write, rc::Rc, borrow::Borrow, time::Duration};

use nix::{fcntl::{self, OFlag}, libc::{self, c_char}, sys::stat::Mode};
use nix::NixPath;
use anyhow::Result;
use clap::Parser;

use rand::distributions::{Distribution, Uniform};

use btdu_rs::btrfs;


struct SampleTree {
    total: u64,
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

    // fn add_sample<'a>(&mut self, mut path: impl Iterator<Item=&'a str>) {
    //     self.total += 1;
    //     match path.next() {
    //         Some(p) => {
    //             self.children.raw_entry_mut().from_key(p).or_insert_with(|| (p.to_owned(), SampleTree::new())).1.add_sample(path);
    //         },
    //         None => {},
    //     }   
    // }

    fn add(&mut self, other: &Self) {
        self.total += other.total;
        for (k, v) in &other.children {
            self.get_or_create_child(k.as_str()).add(v)
        }
    }

    fn sub(&mut self, other: &Self) {
        self.total -= other.total;
        if self.total == 0 {
            self.children.clear();
            return
        }
        for (k, v) in &other.children {
            match self.children.get_mut(k.as_str()) {
                Some(c) => c.sub(v),
                None => {},
            }
        }
    }

    fn get_or_create_child(&mut self, k: &str) -> &mut Self {
        self.children.raw_entry_mut().from_key(k).or_insert_with(|| (k.to_owned(), SampleTree::new())).1
    }

    fn add_sample<'a>(&mut self, mut path: impl Iterator<Item=&'a str>) {
        self.total += 1;
        match path.next() {
            Some(p) => {
                self.get_or_create_child(p).add_sample(path)
            },
            None => {},
        }   
    }

 

    fn print_internal<W: fmt::Write>(&self, w: &mut W, total_samples: u64, bytes_per_sample:f64, min_disk_fraction: Option<f64>, name: &str, depth: usize) -> fmt::Result {
        let disk_fraction = (self.total as f64) / (total_samples as f64);
        // let disk_bytes = (total_length as f64 * disk_fraction) as u64;
        let disk_bytes = (self.total as f64 * bytes_per_sample).round() as u64;

        match min_disk_fraction {
            Some(min_disk_fraction) if disk_fraction < min_disk_fraction => return Ok(()),
            _ => {},
        }

        let path = { 
            let mut path =  String::new();
            for i in 0..depth {
                path.push_str(" ");
            }
            path.push('/');
            path.push_str(name);
            path
        };

        writeln!(w, "{:60} {:>8} {:>4.1}% {:>16}", path, self.total,  disk_fraction * 100.0, bytesize::to_string(disk_bytes, true))?;

        
        let mut c: Vec<_> = self.children.iter().collect();
        c.sort_by_key(|(_,v)| std::cmp::Reverse(v.total));
        for (k,v) in &c {
            v.print_internal(w, total_samples, bytes_per_sample, min_disk_fraction, k, depth+1)?;    
        }

        Ok(())
    }

    fn print<W: fmt::Write>(&self, w: &mut W, total_samples: u64, bytes_per_sample: f64, min_disk_fraction: Option<f64>) -> fmt::Result {
        self.print_internal(w, total_samples, bytes_per_sample, min_disk_fraction, "", 0)
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
                // bug here
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

struct BtrfsSample {
    total_samples: u64,
    bytes_per_sample: f64,
    sample_tree: SampleTree,
}

impl Default for BtrfsSample {
    fn default() -> Self {
        Self { 
            total_samples: 0, 
            bytes_per_sample: 0.0,
            sample_tree: Default::default() 
        }
    }
}

impl BtrfsSample {
    fn add(&mut self, other: &Self) {
        self.total_samples += other.total_samples;
        self.sample_tree.add(&other.sample_tree);
    }

    fn sub(&mut self, other: &Self) {
        self.total_samples -= other.total_samples;
        self.sample_tree.sub(&other.sample_tree);
    }

    fn print<W: fmt::Write>(&self, w: &mut W,  min_disk_fraction: Option<f64>) -> fmt::Result {
        self.sample_tree.print(w, self.total_samples, self.bytes_per_sample, min_disk_fraction)
    }
}

struct BtrfsSampleAgg {
    max_buckets: usize,
    bytes_per_sample_sum: f64,
    // total_samples: u64,
    // sample_tree: SampleTree,
    cur: BtrfsSample,
    buckets: VecDeque<BtrfsSample>,
}

impl BtrfsSampleAgg {
    fn new(max_buckets: usize) -> Self {
        Self {
            max_buckets,
            // total_samples: 0,
            bytes_per_sample_sum: 0.0,

            cur: BtrfsSample::default(),
            // sample_tree: SampleTree::new(),
            buckets: VecDeque::new(),
        }
    }

    fn add(&mut self, sample: BtrfsSample) -> &BtrfsSample {
        // self.total_samples += sample.total_samples;
        self.bytes_per_sample_sum += sample.bytes_per_sample;
        self.cur.total_samples += sample.total_samples;
        self.cur.sample_tree.add(&sample.sample_tree);
        // self.sample_tree.add(&sample.sample_tree);
        self.buckets.push_back(sample);
        if self.buckets.len() > self.max_buckets {
            match self.buckets.pop_front() {
                Some(old_sample) => {
                    self.bytes_per_sample_sum -= old_sample.bytes_per_sample;
                    self.cur.total_samples -= old_sample.total_samples;
                    self.cur.sample_tree.sub(&old_sample.sample_tree);

                },
                None => {},
            }
        }
        self.cur.bytes_per_sample = self.bytes_per_sample_sum / (self.buckets.len()*self.buckets.len()) as f64;
        &self.cur

    }
}


fn btrfs_sample(fd: i32, bytes_per_sample_hint: u64) -> Result<BtrfsSample> {
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

    let samples = total_chunk_length / bytes_per_sample_hint;
    let bytes_per_sample = total_chunk_length as f64 / samples as f64;
    let mut roots = Roots::new(fd);
 

    let uniform = Uniform::new(0, total_chunk_length);
    let mut rng = rand::thread_rng();

    let mut sample_tree = SampleTree::new();
    let mut total_samples = 0;
    let mut start = std::time::Instant::now();

    let mut inode_stats = HashMap::<(u64, u64), u64>::new();

    let mut inode_cache = HashMap::<(u64, u64), Result<String>>::new();

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
                            inode_stats.entry((inode.root, inode.inum)).or_default().add_assign(1);

                            let p = inode_cache.entry((inode.root,inode.inum)).or_insert_with(|| {
                                btrfs::ino_lookup_sync(fd, inode.root,inode.inum)
                            });
                            match  p {
                                Ok(path) => {

                                    // free space cache item
                                    if inode.root == btrfs::BTRFS_ROOT_TREE_OBJECTID as u64 {
                                        return;
                                    }
                                    let root_path = roots.get_root(inode.root);
                                    let inode_path = path.split('/').filter(|s| !s.is_empty());
                                    
                                    let full_path_it = itertools::chain!(
                                        ["DATA"],
                                        root_path.iter().map(|s| s.as_str()),
                                        inode_path
                                    );  
                                    sample_tree.add_sample(full_path_it);
                                    // let q = root_path.iter();
                                    // sample_tree.add_sample(q);
                                    // sample_tree.add_sample(itertools::chain!(root_path.into_iter(), inode_path));
                                },
                                Err(_) => {
                                    sample_tree.add_sample(["DATA", "ERROR", "INO_LOOKUP"].into_iter());
                                    // sample_tree.add(["ERROR", "INO_LOOKUP"].into_iter());
                                },
                            }
                        }
                    },
                    Err(_) => {
                        sample_tree.add_sample(["DATA", "ERROR", "LOGICAL_TO_INO"].into_iter());
                    },
                });


            },
            btrfs::BTRFS_BLOCK_GROUP_METADATA => {
                sample_tree.add_sample(["METADATA"].into_iter());

            },
            btrfs::BTRFS_BLOCK_GROUP_SYSTEM => {
                sample_tree.add_sample(["SYSTEM"].into_iter());

            },
            _ => {

            }
        };
    }
    let total_time = start.elapsed();

    
    println!("samples={} elapsed={:?} per_sample={:?} bytes_per_sample={} resolution={}", total_samples, total_time, total_time/(total_samples as u32), bytes_per_sample, bytesize::to_string(bytes_per_sample as u64, true));
    {
        let unique_inodes = inode_stats.len();
        let inode_lookups: u64 = inode_stats.values().sum();
        println!("unique_inodes={} total_lookups={} unique_pct={}", unique_inodes, inode_lookups, (unique_inodes as f64) / (inode_lookups as f64) );
    }

    Ok(BtrfsSample{
        total_samples,
        bytes_per_sample,
        sample_tree
    })
} 

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Resolution
    #[clap(short, long, default_value_t = 1024.0*1024.0)]
    resolution: f64,

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
    let bytes_per_sample = args.resolution;


    // let mut merged_sample = BtrfsSample::default();
    // let mut sample_ring = VecDeque::new();
    // let max_recent = 60;

    let mut agg = BtrfsSampleAgg::new(60);

    let n = 10000;
    for i in 1..=n {
        let sample = btrfs_sample(fd, bytes_per_sample as u64)?;
        let agg_sample = agg.add(sample);
        // merged_sample.add(&sample);
        // sample_ring.push_back(sample);
        // if sample_ring.len() > max_recent {
        //     merged_sample.sub(&sample_ring.pop_front().unwrap());
        // }
        println!("agg_samples={} agg_resolution={}", agg_sample.total_samples, agg_sample.bytes_per_sample);
        let mut buf = String::new();
        agg_sample.print(&mut buf, Some(args.min_pct / 100.0))?;
        // sample.print(&mut buf, bytes_per_sample, Some(args.min_pct / 100.0))?;
        std::io::stdout_locked().write_all(buf.as_bytes())?;

        std::thread::sleep(Duration::from_millis(1000))
    }

    // println!("total_samples={}", merged_sample.total_samples);

    // let mut buf = String::new();
    // merged_sample.print(&mut buf, bytes_per_sample / (n as f64), Some(args.min_pct / 100.0))?;
    // std::io::stdout_locked().write_all(buf.as_bytes())?;
    

    
    Ok(())
}


