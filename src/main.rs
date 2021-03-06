//! The Digestiflow CLI main program.
//!
//! The code in this module parses the command line, sets up logging, and then dispatches to the
//! sub modules implementing the commands.

// `error_chain!` can recurse deeply.
#![recursion_limit = "1024"]

extern crate byteorder;
#[macro_use]
extern crate clap;
extern crate chrono;
extern crate config;
#[macro_use]
extern crate derivative;
#[macro_use]
extern crate error_chain;
extern crate flate2;
extern crate glob;
extern crate rand;
extern crate rand_xorshift;
extern crate rayon;
extern crate regex;
extern crate restson;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate shellexpand;
#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;
extern crate sxd_document;
extern crate sxd_xpath;

mod ingest;
mod settings;

use slog::Drain;

use std::result;
use std::sync::atomic::Ordering;
use std::sync::{atomic, Arc};

/// Global module with error handlers.
mod errors {
    // Create the Error, ErrorKind, ResultExt, and Result types
    error_chain! {}
}

pub use errors::*;

use clap::{App, ArgMatches};

use settings::Settings;

/// Custom `slog` Drain logic
struct RuntimeLevelFilter<D> {
    drain: D,
    log_level: Arc<atomic::AtomicIsize>,
}

impl<D> Drain for RuntimeLevelFilter<D>
where
    D: Drain,
{
    type Ok = Option<D::Ok>;
    type Err = Option<D::Err>;

    fn log(
        &self,
        record: &slog::Record,
        values: &slog::OwnedKVList,
    ) -> result::Result<Self::Ok, Self::Err> {
        let current_level = match self.log_level.load(Ordering::Relaxed) {
            0 => slog::Level::Warning,
            1 => slog::Level::Info,
            _ => slog::Level::Trace,
        };

        if record.level().is_at_least(current_level) {
            self.drain.log(record, values).map(Some).map_err(Some)
        } else {
            Ok(None)
        }
    }
}

/// Program entry point after using `clap` for parsing command line arguments, called by `main()`.
fn run(matches: ArgMatches) -> Result<()> {
    // Logging setup ------------------------------------------------------------------------------

    // Atomic variable controlling logging level
    let log_level = Arc::new(atomic::AtomicIsize::new(1));

    // Perform slog setup
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build();
    let drain = RuntimeLevelFilter {
        drain: drain,
        log_level: log_level.clone(),
    }
    .fuse();
    let drain = slog_async::Async::new(drain).build().fuse();

    let logger = slog::Logger::root(drain, o!());

    // Switch log level
    if matches.is_present("quiet") {
        log_level.store(0, Ordering::Relaxed);
    } else {
        log_level.store(
            1 + matches.occurrences_of("verbose") as isize,
            Ordering::Relaxed,
        );
    };

    // Command Line Handling ----------------------------------------------------------------------

    // Dispatch commands from command line.
    match matches.subcommand() {
        // cnvetti cmd <coverage|normalize|...>
        ("ingest", Some(_m)) => ingest::run(
            &logger,
            &Settings::new(&matches).expect("Problem with obtaining configuration"),
        )
        .chain_err(|| "Could not execute 'ingest' command")?,
        _ => bail!("Invalid command: {}", matches.subcommand().0),
    }

    info!(logger, "All done. Have a nice day.");

    Ok(())
}

/// Main entry point.
fn main() {
    let yaml = load_yaml!("cli.yaml");
    let matches = App::from_yaml(yaml).get_matches();

    if let Err(ref e) = run(matches) {
        eprintln!("error: {}", e);

        for e in e.iter().skip(1) {
            eprintln!("caused by: {}", e);
        }

        // The backtrace is not always generated. Try to run this example
        // with `RUST_BACKTRACE=1`.
        if let Some(backtrace) = e.backtrace() {
            eprintln!("backtrace: {:?}", backtrace);
        }

        ::std::process::exit(1);
    }
}
