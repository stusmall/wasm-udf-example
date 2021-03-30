extern crate alloc;

use arrow::array::UInt32Array;
use arrow::compute;
use arrow::ipc::reader::FileReader;
use core::alloc::Layout;

use std::io::Cursor;

// This isn't something we expect a user to write.  This will be provided by a language specific SDK
#[no_mangle]
pub unsafe extern "C" fn malloc(size: u32, alignment: u32) -> *mut u8 {
    let layout = Layout::from_size_align_unchecked(size as usize, alignment as usize);
    alloc::alloc::alloc(layout)
}

// This isn't something we expect a user to write.  This will be provided by a language specific SDK
#[no_mangle]
pub unsafe extern "C" fn free(ptr: *mut u8, size: u32, alignment: u32) {
    let layout = Layout::from_size_align_unchecked(size as usize, alignment as usize);
    alloc::alloc::dealloc(ptr, layout);
}


// When making this SDK it will make since ti save users from implementing most of this.  We can
// easily make a proc macro that will produce most of this boilerplate.  It can look something like:
// #[udf_entry_point]
// pub fn whatever(ArrayDataRef) -> ArrayData
//
// Every language can be free to define the UDF entry point in their native idioms.  They will all
// boil down to the same low level C style API.  So if there is something you don't want to
// officially support users can still find a way to make it work if they care
//
// In this example I'm using FileReader over a Cursor in RAM, but I'm sure that is me using arrow
// wrong.
#[no_mangle]
pub unsafe extern "C" fn udf(ptr: *const u8, length: i32) -> u32 {
    let input_buffer = std::slice::from_raw_parts(ptr, length as usize);
    let mut reader = Cursor::new(input_buffer);
    let reader = FileReader::try_new(&mut reader).unwrap();
    let mut sum: u32 = 0;
    for batch_result in reader {
        let batch = batch_result.unwrap();
        for array in batch.columns() {
            let values = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            if let Some(n) = compute::sum(values) {
                sum += n;
            }
        }
    }
    sum
}