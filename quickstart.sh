cd adder &&
cargo build --release --target wasm32-unknown-unknown &&
cd ../runner &&
cargo run --release -- ../adder/target/wasm32-unknown-unknown/release/adder.wasm
