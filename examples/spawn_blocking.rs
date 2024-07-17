use std::thread;

use tokio::runtime::Runtime;

fn main() {
    println!("thread_id: {:?}", thread::current().id());

    let mut handles = Vec::new();

    for i in 0..3 {
        let rt = Runtime::new().unwrap();
        let handle = thread::Builder::new()
            .name(format!("ReplBackend {i}"))
            .spawn(move || {
                let ret = rt.block_on(async {
                    println!("thread_id: {:?}", thread::current().id());

                    rt.spawn_blocking(|| {
                        println!("thread_id: {:?}", thread::current().id());
                        100
                    })
                    .await
                });
                println!("thread_id: {:?} {:?}", thread::current().id(), ret)
            })
            .unwrap();
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    println!("thread_id: {:?}", thread::current().id());
}
// Create the runtime
