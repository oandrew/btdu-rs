#![feature(bigint_helper_methods)]
#![feature(maybe_uninit_slice)]
#![feature(maybe_uninit_uninit_array)]
#![feature(hash_raw_entry)]

use std::{collections::HashMap, fmt};


pub mod btrfs;


pub struct SampleTree {
    total: u64,
    children: HashMap<String, SampleTree>,
}

impl Default for SampleTree {
    fn default() -> Self {
        Self::new()
    }
}

impl SampleTree {
    pub fn new() -> Self {
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

    pub fn add(&mut self, other: &Self) {
        self.total += other.total;
        for (k, v) in &other.children {
            self.get_or_create_child(k.as_str()).add(v)
        }
    }

    pub fn sub(&mut self, other: &Self) {
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

    pub fn add_sample<'a>(&mut self, mut path: impl Iterator<Item=&'a str>) {
        self.total += 1;
        match path.next() {
            Some(p) => {
                self.get_or_create_child(p).add_sample(path)
            },
            None => {},
        }   
    }

    pub fn add_samples<'a>(&mut self, mut path: impl Iterator<Item=&'a str>, n: u64) {
        self.total += n;
        match path.next() {
            Some(p) => {
                self.get_or_create_child(p).add_samples(path, n)
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

    pub fn print<W: fmt::Write>(&self, w: &mut W, total_samples: u64, bytes_per_sample: f64, min_disk_fraction: Option<f64>) -> fmt::Result {
        self.print_internal(w, total_samples, bytes_per_sample, min_disk_fraction, "", 0)
    }
}