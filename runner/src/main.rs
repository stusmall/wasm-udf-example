use arrow::array::UInt32Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use wasmtime_rust::__rt::wasmtime::{Instance, Module, Store};

use arrow::ipc::reader::FileReader;
use std::io::Cursor;
use std::mem::size_of;
use std::path::Path;
use std::slice::from_raw_parts;
use std::time::Instant;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "ExampleWasmRunner",
    about = "This is just an example utility to help in experimenting with UDFs, arrow and WASM."
)]
struct Opt {
    wasm_module: String,
    #[structopt(default_value = "2")]
    value1: u32,
    #[structopt(default_value = "2")]
    value2: u32,
    #[structopt(short = "i", long = "iterations", default_value = "1")]
    iterations: u32,
}

fn main() {
    let opt = Opt::from_args();
    let instance = build_wasm(&opt.wasm_module);
    let data = build_arrow_buffer(opt.value1, opt.value2);
    for _ in 0..opt.iterations {
        run_wasm(&instance, opt.value1, opt.value2, &data);
    }
}

fn build_wasm(wasm_module: impl AsRef<Path>) -> Instance {
    println!("Setting up wasm module...");
    let now = Instant::now();
    let store = Store::default();
    let module = Module::from_file(store.engine(), wasm_module).unwrap();
    let instance = Instance::new(&store, &module, &[]).unwrap();
    println!(
        "Finished!  Setting up the wasm module took {}ms",
        now.elapsed().as_millis()
    );
    instance
}

// I literally have no idea what I'm doing with Arrow.  All I care about is easily getting a in
// memory byte array that can be easily copied into the guest environment.  I'm sure others will
// think of a better way to do this.
fn build_arrow_buffer(v1: u32, v2: u32) -> Vec<u8> {
    println!("Setting up arrow buffer...");
    let now = Instant::now();
    let field_a = Field::new("v1", DataType::UInt32, false);
    let field_b = Field::new("v2", DataType::UInt32, false);

    let schema = Schema::new(vec![field_a, field_b]);
    //let array = UInt32Array::from(vec![Some(0), Some(1)]);
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(UInt32Array::from(vec![Some(v1)])),
            Arc::new(UInt32Array::from(vec![Some(v2)])),
        ],
    )
    .unwrap();
    let mut data = vec![];
    {
        let mut writer = FileWriter::try_new(&mut data, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }
    println!(
        "Finished!  Setting up the arrow buffer took {}ms",
        now.elapsed().as_millis()
    );
    data
}

// We could probably cache all the typedfuncs instead of the Instance to save a little load.  Some
// unsafeness here.  We need to be very careful about this copy from the sandbox.  Naughtiness
// can be afoot
fn run_wasm(instance: &Instance, v1: u32, v2: u32, data: &[u8]) {
    println!("Running our UDF...");
    let now = Instant::now();
    let malloc = instance
        .get_typed_func::<(i32, i32), i32>("malloc")
        .unwrap();
    let input_data_ptr = malloc.call((data.len() as i32, 0)).unwrap();
    let memory = instance.get_memory("memory").unwrap();
    memory.write(input_data_ptr as usize, &data).unwrap();
    let udf = instance.get_typed_func::<(i32, i32), i32>("udf").unwrap();
    let ret = udf.call((input_data_ptr, data.len() as i32)).unwrap();

    let mut udf_result_buffer = Vec::with_capacity(size_of::<UDFResult>());
    for _ in 0..size_of::<UDFResult>() {
        udf_result_buffer.push(0);
    }
    memory.read(ret as usize, &mut udf_result_buffer).unwrap();
    let result_struct = unsafe {
        //TODO:  I was in a rush writing this and I'm skeptical.  Come back later with clear eyes
        &from_raw_parts::<UDFResult>(udf_result_buffer.as_ptr() as *const UDFResult, 1)[0]
    };
    let mut output_arrow_buffer: Vec<u8> = Vec::with_capacity(result_struct.len as usize);
    output_arrow_buffer.resize(result_struct.len as usize, 0);
    memory
        .read(result_struct.ptr as usize, &mut output_arrow_buffer)
        .unwrap();
    let mut reader = Cursor::new(&output_arrow_buffer);
    let reader = FileReader::try_new(&mut reader).unwrap();
    // Now that we are done we need to free the buffer used as input, the boxed struct and the output
    // arrow buffer
    let free = instance
        .get_typed_func::<(i32, i32, i32), ()>("free")
        .unwrap();
    free.call((input_data_ptr, data.len() as i32, 0)).unwrap();
    free.call((result_struct.ptr, result_struct.len, 0)).unwrap();
    free.call((ret, size_of::<UDFResult>() as i32, 0)).unwrap();
    println!("Finished! UDF ran in {}ms", now.elapsed().as_millis());

    for row_res in reader {
        let row = row_res.unwrap();
        for array in row.columns() {
            let values = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            println!("{} + {} = {:?}", v1, v2, values.value(0));
        }
    }
}

// I copy and pasted this from the udf because I'm a bad person :)  This definition belongs in the
// sdk
#[repr(C)]
pub struct UDFResult {
    ptr: i32,
    len: i32,
}
