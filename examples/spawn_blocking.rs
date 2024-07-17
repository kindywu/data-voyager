use std::thread;

use tokio::runtime::Runtime;

fn main() {
    let rt = Runtime::new().unwrap();

    println!("thread_id: {:?}", thread::current().id());
    let ret = rt.block_on(async {
        println!("thread_id: {:?}", thread::current().id());

        rt.spawn_blocking(|| {
            println!("thread_id: {:?}", thread::current().id());
            100
        })
        .await
    });

    println!("thread_id: {:?} {:?}", thread::current().id(), ret)
}
// Create the runtime
