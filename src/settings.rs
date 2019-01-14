//! Data structures and code for storing and handling settings from configuration file and
//! command line arguments.

use clap::ArgMatches;
use config::{Config, ConfigError, Environment, File};
use shellexpand;
use std::path::Path;

/// Configuration for the REST API in Digestiflow Web.
#[derive(Derivative, Deserialize)]
#[derivative(Debug)]
pub struct Web {
    /// The URL to Digestiflow Web. `$url/api` must be the URL to the API.
    pub url: String,
    /// The API authentication token.
    #[derivative(Debug = "ignore")]
    pub token: String,
}

impl Default for Web {
    /// Return default configuration regarding Digestfilow Web API.
    fn default() -> Self {
        return Self {
            url: "".to_string(),
            token: "".to_string(),
        };
    }
}

/// Arguments/configuration for the `ingest` command.
#[derive(Debug, Deserialize)]
pub struct IngestArgs {
    /// UUID of the project to import into.
    pub project_uuid: String,
    /// Vector of paths of flow cells to analyze.
    pub path: Vec<String>,
    /// Whether or not to register new flow cells via API.
    pub register: bool,
    /// Whether or not to update existing flow cells via API.
    pub update: bool,
    /// Whether or not to compute adapter sequence histograms.
    pub analyze_adapters: bool,
    /// Whether or not to post adapter sequence histogram via API.
    pub post_adapters: bool,
    /// String to use for machine operator when creating flow cell via API.
    pub operator: String,
    /// Number of tiles to sample.
    pub sample_tiles: i32,
    /// Number of reads to sample from each tile.
    pub sample_reads_per_tile: i32,
    /// Skip if sequencing status is a final state.
    pub skip_if_status_final: bool,
}

impl Default for IngestArgs {
    /// Return defaults for `ingest` command arguments.
    fn default() -> Self {
        return IngestArgs {
            project_uuid: "".to_string(),
            path: Vec::new(),
            register: true,
            update: true,
            analyze_adapters: true,
            post_adapters: true,
            operator: "".to_string(),
            sample_tiles: 1,
            sample_reads_per_tile: 0,
            skip_if_status_final: true,
        };
    }
}

/// Overall settings.
#[derive(Debug, Deserialize)]
pub struct Settings {
    /// Further increase log output verbosity,
    pub debug: bool,
    /// Increase log output verbosity.
    pub verbose: bool,
    /// Decrease log output to a minimum.
    pub quiet: bool,
    /// Number of threads to use for parallel processing.
    pub threads: i32,
    /// Seed value to use for random number generator.
    pub seed: u64,
    /// Whether or not to write out API token into log file.
    pub log_token: bool,
    /// Configuration regarding Digestiflow Web.
    pub web: Web,
    /// Arguments to the `ingest` command.
    pub ingest: IngestArgs,
}

impl Default for Settings {
    /// Return default settings.
    fn default() -> Self {
        return Self {
            debug: false,
            verbose: false,
            quiet: false,
            threads: 1,
            web: Web::default(),
            ingest: IngestArgs::default(),
            seed: 42,
            log_token: false,
        };
    }
}

impl Settings {
    /// Construct from `ArgMatches`.
    ///
    /// Will first load `~/.digestiflowrc.toml` and then consider the command line arguments
    /// that were parsed into `ArgMatches`.  Command line arguments take precedence over values
    /// from configuration file which take precedence over defaults.
    pub fn new(matches: &ArgMatches) -> Result<Self, ConfigError> {
        let mut s = Config::new();

        // Set defaults (currently explicit required, see for a future less-boilerplate option
        // https://github.com/mehcode/config-rs/issues/60)
        let default = Settings::default();

        s.set_default("debug", default.debug)?
            .set_default("verbose", default.verbose)?
            .set_default("quiet", default.quiet)?
            .set_default("threads", default.threads as i64)?
            .set_default("seed", default.seed as i64)?
            .set_default("log_token", default.log_token)?
            .set_default("web.token", default.web.token.clone())?
            .set_default("web.url", default.web.url.clone())?
            .set_default("ingest.project_uuid", default.ingest.project_uuid)?
            .set_default("ingest.path", default.ingest.path)?
            .set_default("ingest.register", default.ingest.register)?
            .set_default("ingest.update", default.ingest.update)?
            .set_default("ingest.analyze_adapters", default.ingest.analyze_adapters)?
            .set_default("ingest.post_adapters", default.ingest.post_adapters)?
            .set_default("ingest.operator", default.ingest.operator)?
            .set_default("ingest.sample_tiles", default.ingest.sample_tiles as i64)?
            .set_default(
                "ingest.skip_if_status_final",
                default.ingest.skip_if_status_final,
            )?
            .set_default(
                "ingest.sample_reads_per_tile",
                default.ingest.sample_reads_per_tile as i64,
            )?;

        // Next, load configuration file.
        let expanded = shellexpand::tilde("~/.digestiflowrc.toml")
            .into_owned()
            .to_string();
        if Path::new(&expanded).exists() {
            s.merge(File::with_name(&expanded))?;
        }

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
                if m.is_present("log_token") {
                    s.set("log_token", true)?;
                }
                if m.is_present("threads") {
                    s.set("threads", m.value_of("threads").unwrap())?;
                }
                if m.is_present("web_url") {
                    s.set("web.url", m.value_of("web_url").unwrap())?;
                }
                if m.is_present("project_uuid") {
                    s.set("ingest.project_uuid", m.value_of("project_uuid"))?;
                }
                s.set(
                    "ingest.path",
                    m.values_of("path")
                        .expect("Problem getting paths from command line")
                        .map(|s| s.to_string())
                        .collect::<Vec<String>>(),
                )?;
                if m.is_present("no_register") {
                    s.set("ingest.register", false)?;
                }
                if m.is_present("no_update") {
                    s.set("ingest.update", false)?;
                }
                if m.is_present("analyze_adapters") {
                    s.set("ingest.analyze_adapters", true)?;
                }
                if m.is_present("post_adapters") {
                    s.set("ingest.post_adapters", true)?;
                }
                if m.is_present("sample_reads_per_tile") {
                    s.set(
                        "ingest.sample_reads_per_tile",
                        m.value_of("sample_reads_per_tile"),
                    )?;
                }
                if m.is_present("analyze_if_state_final") {
                    s.set("ingest.skip_if_status_final", false)?;
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
