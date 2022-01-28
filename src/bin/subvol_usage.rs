use std::{collections::{HashMap, HashSet, VecDeque}, env, hash::{BuildHasher, Hasher}, alloc::Layout, ops::{Deref, DerefMut, Range, RangeInclusive, AddAssign}, ffi::{CStr, CString}, fmt, io::Write, rc::Rc, borrow::Borrow, time::Duration, marker::PhantomData};

use nix::{fcntl::{self, OFlag}, libc::{self, c_char}, sys::stat::Mode};
use nix::NixPath;
use anyhow::Result;
use clap::Parser;

use btdu_rs::btrfs;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
   

    /// Mounted btrfs path
    path: String,
}

struct PtrReader<'a> {
    pos: *const u8,
    end: *const u8,
    phantom: PhantomData<&'a u8>,
}

impl <'a>  PtrReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        let range = data.as_ptr_range();
        Self {
            pos: range.start,
            end: range.end,
            phantom: PhantomData
        }
    }
    // fn next<T: Sized>(&mut self) -> Option<&T> {
    //     unsafe {
    //         let next_pos = self.pos.add(std::mem::size_of::<T>());
    //         if next_pos <= self.end {
    //             let res = &*(self.pos as *const T);
    //             self.pos = next_pos;
    //             Some(res)
    //         } else {
    //             None
    //         }
    //     }
    // }

    fn next<T: Sized>(&mut self) -> Option<&'a T> {
        unsafe {
            let next_pos = self.pos.add(std::mem::size_of::<T>());
            if next_pos <= self.end {
                let res = &*(self.pos as *const T);
                self.pos = next_pos;
                Some(res)
            } else {
                None
            }
        }
    }

    fn available(&self) -> usize {
        unsafe {
            self.end.offset_from(self.pos).max(0) as usize
        }
    }
}

unsafe fn get_and_move_typed<T: Sized>(ptr: &mut *const u8) -> *const T {
    let res = *ptr as *const T;
    *ptr = (*ptr).add(std::mem::size_of::<T>());
    res
}

fn main() -> Result<()> {
    let args = Args::parse();
    // let args: Vec<_> = env::args().collect();
    let fd = fcntl::open(args.path.as_str(), OFlag::O_RDONLY, Mode::empty())?;

    btrfs::tree_search_cb(fd, btrfs::BTRFS_EXTENT_TREE_OBJECTID as u64, btrfs::SearchKey::ALL, |sh, data| {
        let mut r = PtrReader::new(data);
        match sh.type_ {
            btrfs::BTRFS_EXTENT_ITEM_KEY => {
                let extent_item = r.next::<btrfs::btrfs_extent_item>().unwrap();
                let tree_block_info = if (extent_item.flags & btrfs::BTRFS_EXTENT_FLAG_TREE_BLOCK as u64) != 0 {
                    r.next::<btrfs::btrfs_tree_block_info>()
                } else {
                    None
                };
                println!("({} BTRFS_EXTENT_ITEM_KEY {:6}) {:?} {:?}", sh.objectid, sh.offset, extent_item, tree_block_info);

                if (extent_item.flags & btrfs::BTRFS_EXTENT_FLAG_DATA as u64) != 0 {
                    while let Some(extent_inline_ref_type) = r.next::<u8>() {
                        match *extent_inline_ref_type as u32 {
                            btrfs::BTRFS_EXTENT_DATA_REF_KEY => {
                                let extent_inline_data_ref = r.next::<btrfs::btrfs_extent_data_ref>().unwrap();
                                println!("  BTRFS_EXTENT_DATA_REF_KEY {:?}", extent_inline_data_ref);
                                // *root_usage.entry(extent_inline_data_ref.root).or_default() += search_header.offset;
                            },

                            btrfs::BTRFS_SHARED_DATA_REF_KEY => {
                                let _extent_inline_ref_offset = r.next::<u64>().unwrap();
                                let shared_data_ref = r.next::<btrfs::btrfs_shared_data_ref>().unwrap();
                                println!("  BTRFS_SHARED_DATA_REF_KEY {:?}", shared_data_ref);
                            },
                            _ => {
                                // println!("  type = {}", extent_inline_ref.type_);
                                println!("BTRFS_EXTENT_ITEM_KEY / DATA / extent_inline_ref.type={}", extent_inline_ref_type);
                                break;
                            },
                        }

                    }
                } else {
                    // println!("  BTRFS_EXTENT_ITEM_KEY, non BTRFS_EXTENT_FLAG_DATA");
                    todo!("  BTRFS_EXTENT_ITEM_KEY, non BTRFS_EXTENT_FLAG_DATA");
                }

            },
            btrfs::BTRFS_METADATA_ITEM_KEY  => {
                let extent_item = r.next::<btrfs::btrfs_extent_item>().unwrap();
                println!("({} BTRFS_METADATA_ITEM_KEY {:6}) {:?}", sh.objectid, sh.offset, extent_item);

                while let Some(extent_inline_ref) = r.next::<btrfs::btrfs_extent_inline_ref>() {
                    // println!("  {:?}", extent_inline_ref);
                    match extent_inline_ref.type_ as u32 {
                        btrfs::BTRFS_TREE_BLOCK_REF_KEY => {
                            // let vaddr = search_header.objectid;
                            // let root = extent_inline_ref.offset;
                            // *root_usage.entry(root).or_default() += 16*1024;
                            // paths.insert(vaddr, root);
                            // match unresolved_usage.remove(&vaddr) {
                            //     Some(usage) => *root_usage.entry(root).or_default() += usage,
                            //     None => {},
                            // }
                            println!("  BTRFS_TREE_BLOCK_REF_KEY");
                        },
                        btrfs::BTRFS_SHARED_BLOCK_REF_KEY  => {
                            // let parent = extent_inline_ref.offset;
                            // match paths.get(&parent) {
                            //     Some(root) => *root_usage.entry(*root).or_default() += 16*1024,
                            //     None => *unresolved_usage.entry(parent).or_default() += 16*1024,
                            // }
                            println!("  BTRFS_SHARED_BLOCK_REF_KEY");
                        },
                        _ => {
                            todo!();
                        }
                    }
                    
                }
            },
            _ => {
                return;
            }
        };

        if r.available() > 0 {
            panic!("data left");
        }


    })?;

    

    
    Ok(())
}


