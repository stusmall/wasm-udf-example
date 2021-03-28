use arrow::array::UInt32Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use wasmtime_rust::__rt::wasmtime::{Instance, Module, Store};

use std::path::Path;
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


// We could probably cache all th typedfuncs instead of the Instance to save a little load
fn run_wasm(instance: &Instance, v1: u32, v2: u32, data: &[u8]) {
    println!("Running our UDF...");
    let now = Instant::now();
    let malloc = instance
        .get_typed_func::<(i32, i32), i32>("malloc")
        .unwrap();
    let data_ptr = malloc.call((data.len() as i32, 0)).unwrap();
    let memory = instance.get_memory("memory").unwrap();
    memory.write(data_ptr as usize, &data).unwrap();
    let udf = instance.get_typed_func::<(i32, i32), u32>("udf").unwrap();
    let ret = udf.call((data_ptr, data.len() as i32));
    let malloc = instance
        .get_typed_func::<(i32, i32, i32), ()>("free")
        .unwrap();
    malloc.call((data_ptr, data.len() as i32, 0)).unwrap();
    println!(
        "Finished! Got the answer {} + {} = {} in {}ms",
        v1,
        v2,
        ret.unwrap(),
        now.elapsed().as_millis()
    );
}
