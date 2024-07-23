use backend::{Backend, DataFusionBackend};
use crossbeam_channel as mpsc;
use enum_dispatch::enum_dispatch;
use oneshot::Sender;
use reedline_repl_rs::CallBackMap;
use std::{collections::HashMap, thread};

use tokio::runtime::Runtime;

mod backend;
mod cli;

pub use cli::{ConnectOpts, DescribeOpts, HeadOpts, ListOpts, ReplCommand, SchemaOpts, SqlOpts};

#[enum_dispatch]
trait CmdExector {
    async fn execute<T: Backend>(self, backend: &mut T) -> anyhow::Result<String>;
}

pub fn get_callbacks() -> CallBackMap<ReplContext, reedline_repl_rs::Error> {
    let mut callbacks: CallBackMap<ReplContext, reedline_repl_rs::Error> = HashMap::new();
    callbacks.insert("connect".to_string(), cli::connect);
    callbacks.insert("list".to_string(), cli::list);
    callbacks.insert("describe".to_string(), cli::describe);
    callbacks.insert("schema".to_string(), cli::schema);
    callbacks.insert("head".to_string(), cli::head);
    callbacks.insert("sql".to_string(), cli::sql);
    callbacks
}

pub struct ReplContext {
    tx: mpsc::Sender<ReplMessage>,
}

impl Default for ReplContext {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplContext {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::unbounded::<ReplMessage>();

        let rt = Runtime::new().expect("Failed to create runtime");

        thread::Builder::new()
            .name("ReplBackend".to_string())
            .spawn(move || {
                let mut backend = DataFusionBackend::new();
                while let Ok(msg) = rx.recv() {
                    if let Err(e) = rt.block_on(async {
                        println!("!!! cmd: {:?}", msg.cmd);
                        let content = msg.cmd.execute(&mut backend).await?;
                        msg.tx.send(content).unwrap();
                        Ok::<_, anyhow::Error>(())
                    }) {
                        eprintln!("Failed to process command: {}", e);
                    }
                }
            })
            .unwrap();

        Self { tx }
    }

    pub fn send(&self, cmd: ReplCommand) -> Option<String> {
        let (tx, rx) = oneshot::channel();
        let msg = ReplMessage::new(cmd, tx);
        if let Err(e) = self.tx.send(msg) {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
        match rx.recv() {
            Ok(data) => Some(data),
            Err(e) => {
                eprintln!("Repl Recv Error: {}", e);
                std::process::exit(1);
            }
        }
    }
}

struct ReplMessage {
    cmd: ReplCommand,
    tx: Sender<String>,
}

impl ReplMessage {
    fn new(cmd: ReplCommand, tx: Sender<String>) -> Self {
        Self { cmd, tx }
    }
}
