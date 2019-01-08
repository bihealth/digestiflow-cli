use clap::ArgMatches;
use config::{Config, ConfigError, Environment, File};
use shellexpand;
use std::path::Path;

#[derive(Derivative, Deserialize)]
#[derivative(Debug)]
pub struct Web {
    pub url: String,
    #[derivative(Debug = "ignore")]
    pub token: String,
}

impl Default for Web {
    fn default() -> Self {
        return Self {
            url: "".to_string(),
            token: "".to_string(),
        };
    }
}

#[derive(Debug, Deserialize)]
pub struct IngestArgs {
    pub project_uuid: String,
    pub path: Vec<String>,
    pub register: bool,
    pub update: bool,
    pub analyze_adapters: bool,
    pub post_adapters: bool,
    pub operator: String,
    pub sample_tiles: i32,
    pub sample_reads_per_tile: i32,
    /// Skip if sequencing status is a final state.
    pub skip_if_status_final: bool,
}

impl Default for IngestArgs {
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

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub debug: bool,
    pub verbose: bool,
    pub quiet: bool,
    pub threads: i32,
    pub seed: u64,
    pub log_token: bool,
    pub web: Web,
    pub ingest: IngestArgs,
}

impl Default for Settings {
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
            )?.set_default(
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
