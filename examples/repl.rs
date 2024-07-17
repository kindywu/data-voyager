#![allow(unused)]

use std::collections::HashMap;

use clap::{ArgAction, Parser, Subcommand};
use enum_dispatch::enum_dispatch;
use reedline_repl_rs::clap::{Arg, ArgMatches, Command};
use reedline_repl_rs::{CallBackMap, Error, Repl, Result};
use tokio::runtime::Runtime;
use tokio::sync::oneshot;

#[enum_dispatch]
trait CmdExector {
    async fn execute(self) -> anyhow::Result<String>;
}

#[derive(Parser, Debug)]
#[command(name = "MyApp", version = "v0.1.0", about = "My very cool app")]
pub enum MyApp {
    Say {
        #[command(subcommand)]
        command: SayCommands,
    },
}

#[enum_dispatch(CmdExector)]
#[derive(Debug, Subcommand)]
pub enum SayCommands {
    #[command(about = "Say Hello to ")]
    Hello(Hello),
}

#[derive(Debug, Parser)]
pub struct Hello {
    #[arg(required = true)]
    pub who: String,
}

impl CmdExector for Hello {
    async fn execute(self) -> anyhow::Result<String> {
        Ok(format!("Hello {}", self.who))
    }
}

/// Write "Hello" with given name
fn say_derived_command<T>(args: ArgMatches, _context: &mut T) -> Result<Option<String>> {
    match args.subcommand() {
        Some(("hello", sub_matches)) => {
            let cmd = Hello {
                who: sub_matches.get_one::<String>("who").unwrap().to_string(),
            };
            execute(SayCommands::Hello(cmd))
        }
        _ => panic!("Unknown subcommand {:?}", args.subcommand_name()),
    }
}

fn execute(cmd: SayCommands) -> Result<Option<String>> {
    let rt = Runtime::new().unwrap();
    let result = rt.block_on(async { cmd.execute().await }).unwrap();
    Ok(Some(result))
}

/// Write "Hello" with given name
fn hello_command<T>(args: ArgMatches, _context: &mut T) -> Result<Option<String>> {
    Ok(Some(format!(
        "hello, {}",
        args.get_one::<String>("who").unwrap()
    )))
}

fn main() -> Result<()> {
    let mut callbacks: CallBackMap<(), reedline_repl_rs::Error> = HashMap::new();

    callbacks.insert("say".to_string(), say_derived_command);

    let mut repl = Repl::new(())
        .with_banner("Welcome to MyApp")
        // .with_name("MyApp")
        // .with_version("v0.1.0")
        // .with_description("My very cool app")
        // .with_command(
        //     Command::new("hello")
        //         .arg(Arg::new("who").required(true))
        //         .about("Greetings!"),
        //     hello_command,
        // )
        .with_derived::<MyApp>(callbacks);

    repl.run()
}
