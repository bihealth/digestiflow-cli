use super::errors::*;
use settings::Settings;

pub fn run(logger: &slog::Logger, settings: &Settings) -> Result<()> {
    info!(logger, "Running: digestiflow-cli-client ingest");
    info!(logger, "Options: {:?}", settings);

    Ok(())
}
