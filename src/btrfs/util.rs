use std::{ops::{Deref, DerefMut}, alloc::Layout, mem::MaybeUninit};


pub trait WithMemAfterTrait<T> {
    fn as_mut_ptr(&mut self) -> *mut T;
    fn total_size(&self) -> usize;
    fn extra_ptr(&self) -> *const u8;
    fn extra_size(&self) -> usize;
}

#[repr(C)]
pub struct WithMemAfter<T, const N: usize> {
    value: T,
    extra: [MaybeUninit<u8>; N],
}

impl <T: Sized, const N: usize> WithMemAfter<T, N> {
    pub fn new() -> Self {
        unsafe {
            WithMemAfter {
                value: std::mem::zeroed(), 
                extra: MaybeUninit::uninit_array(),
            }
        }
    }
}

impl <T: Sized, const N: usize> WithMemAfterTrait<T> for WithMemAfter<T, N> {
    fn as_mut_ptr(&mut self) -> *mut T {
        &mut self.value
    }

    fn total_size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn extra_ptr(&self) -> *const u8 {
        MaybeUninit::slice_as_ptr(&self.extra)
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




struct WithMemAfterOnHeap<T> {
    ptr: *mut T,
    layout: Layout,
}

impl <T: Sized> WithMemAfterOnHeap<T> {
    fn new(buf_size: usize) -> Self {
        let (layout, buf_offset) = Layout::new::<T>().extend(Layout::array::<u8>(buf_size).unwrap()).unwrap();
        println!("layout={:?} buf_offset={}", layout, buf_offset);
        unsafe {
            WithMemAfterOnHeap {
                ptr: std::alloc::alloc(layout) as *mut T,
                layout
            }
        }
    }

    fn as_mut_ptr(&self) -> *mut T {
        self.ptr
    }
}

impl <T: Sized> Deref for WithMemAfterOnHeap<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe {
            self.ptr.as_ref().unwrap()
        }
    }
}

impl <T: Sized> DerefMut for WithMemAfterOnHeap<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe {
            self.ptr.as_mut().unwrap()
        }
    }
}



impl <T> Drop for WithMemAfterOnHeap<T> {
    fn drop(&mut self) {
        unsafe {
            std::alloc::dealloc(self.ptr as *mut u8, self.layout)
        }
    }
}