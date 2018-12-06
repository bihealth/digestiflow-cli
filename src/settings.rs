use clap::ArgMatches;
use config::{Config, ConfigError, Environment, File};

#[derive(Derivative, Deserialize)]
#[derivative(Debug)]
pub struct Web {
    pub url: String,
    #[derivative(Debug = "ignore")]
    pub key: String,
}

#[derive(Debug, Deserialize)]
pub struct IngestArgs {
    pub path: Vec<String>,
    pub register: bool,
    pub update: bool,
    pub analyze_adapters: bool,
    pub post_adapters: bool,
}

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub debug: bool,
    pub verbose: bool,
    pub quiet: bool,
    pub threads: i32,
    pub web: Web,
    pub ingest: IngestArgs,
}

impl Settings {
    pub fn new(matches: &ArgMatches) -> Result<Self, ConfigError> {
        let mut s = Config::new();

        // Start off with the default configuration.
        s.merge(File::with_name("config/default"))?;

        // Add in settings from the environment (with a prefix of APP)
        // Eg.. `APP_DEBUG=1 ./target/app` would set the `debug` key
        s.merge(Environment::with_prefix("DIGESTIFLOW").separator("__"))?;

        // Add settings from command line.
        match matches.subcommand() {
            ("ingest", Some(m)) => {
                if m.is_present("quiet") {
                    s.set("quiet", true)?;
                }
                if m.is_present("quiet") {
                    s.set("verbose", true)?;
                }
                if m.is_present("threads") {
                    s.set("threads", m.value_of("threads").unwrap())?;
                }
                s.set(
                    "ingest.path",
                    m.values_of("path")
                        .expect("Problem getting paths from command line")
                        .map(|s| s.to_string())
                        .collect::<Vec<String>>(),
                )?;
                if m.is_present("register") {
                    s.set("ingest.register", true)?;
                }
                if m.is_present("update") {
                    s.set("ingest.update", true)?;
                }
                if m.is_present("analyze_adapters") {
                    s.set("ingest.analyze_adapters", true)?;
                }
                if m.is_present("post_adapters") {
                    s.set("ingest.post_adapters", true)?;
                }
            }
            _ => {
                return Err(ConfigError::Message(format!(
                    "Invalid command {}",
                    matches.subcommand().0
                )))
            }
        }

        // Deserialize and freeze configuration.
        s.try_into()
    }
}