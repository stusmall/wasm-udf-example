extern crate alloc;

use arrow::array::UInt32Array;
use arrow::compute;
use arrow::ipc::reader::FileReader;
use core::alloc::Layout;

use std::io::Cursor;
use arrow::datatypes::{Field, DataType, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use arrow::ipc::writer::FileWriter;

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

// This isn't something we expect a user to write.  This will be provided by a language specific SDK
#[repr(C)]
pub struct UDFResult {
    ptr: i32,
    len: i32
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
// We should talk about the best way for the UDF to communicate an error happened in run time.  IIRC
// wasm has a best practice for this.  I'll dig into it.
//
// In this example I'm using FileReader over a Cursor in RAM, but I'm sure that is me using arrow
// wrong.
#[no_mangle]
pub unsafe extern "C" fn udf(ptr: *const u8, length: i32) -> *const UDFResult {
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
    let field_a = Field::new("sum", DataType::UInt32, false);
    let schema = Schema::new(vec![field_a]);
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(UInt32Array::from(vec![Some(sum)])),
        ],
    )
        .unwrap();
    let mut data = vec![];
    {
        let mut writer = FileWriter::try_new(&mut data, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }
    let len = data.len();
    let out_ptr= data.leak().as_ptr();
    let result = Box::new(UDFResult{
        ptr: out_ptr as i32,
        len: len as i32
    });
    Box::into_raw(result) as *const UDFResult
}
