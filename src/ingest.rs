use std::fs::File;
use std::io::prelude::*;
use std::path::Path;

use sxd_document::dom::Document;
use sxd_document::parser;
use sxd_xpath::nodeset::Node;
use sxd_xpath::{evaluate_xpath, Value};

use super::errors::*;
use settings::Settings;

#[derive(Debug)]
enum FolderLayout {
    /// MiSeq, HiSeq 2000, etc. `runParameters.xml`
    MiSeq,
    /// MiniSeq, NextSeq etc. `RunParameters.xml`
    MiniSeq,
    /// HiSeq X
    HiSeqX,
}

fn guess_folder_layout(path: &Path) -> Result<FolderLayout> {
    let miseq_marker = path
        .join("Data")
        .join("Intensities")
        .join("BaseCalls")
        .join("L001")
        .join("C1.1");
    let hiseqx_marker = path.join("Data").join("Intensities").join("s.locs");
    let miniseq_marker = path
        .join("Data")
        .join("Intensities")
        .join("BaseCalls")
        .join("L001");

    if miseq_marker.exists() {
        Ok(FolderLayout::MiSeq)
    } else if hiseqx_marker.exists() {
        Ok(FolderLayout::HiSeqX)
    } else if miniseq_marker.exists() {
        Ok(FolderLayout::MiniSeq)
    } else {
        bail!("Could not guess folder layout from {:?}", path)
    }
}

#[derive(Debug)]
struct ReadDescription {
    pub number: i32,
    pub num_cycles: i32,
    pub is_index: bool,
}

#[derive(Debug)]
struct RunInfo {
    pub run_id: String,
    pub flowcell: String,
    pub instrument: String,
    pub date: String,
    pub lane_count: i32,
    pub reads: Vec<ReadDescription>,
}

fn process_xml_run_info(info_doc: &Document) -> Result<RunInfo> {
    let reads = if let Value::Nodeset(nodeset) =
        evaluate_xpath(&info_doc, "//Read").chain_err(|| "Problem finding Read tags")?
    {
        let mut reads = Vec::new();
        for node in nodeset.document_order() {
            if let Node::Element(elem) = node {
                reads.push(ReadDescription {
                    number: elem
                        .attribute("Number")
                        .expect("Problem accessing Number attribute")
                        .value()
                        .to_string()
                        .parse::<i32>()
                        .unwrap(),
                    num_cycles: elem
                        .attribute("NumCycles")
                        .expect("Problem accessing NumCycles attribute")
                        .value()
                        .to_string()
                        .parse::<i32>()
                        .unwrap(),
                    is_index: elem
                        .attribute("IsIndexedRead")
                        .expect("Problem accessing IsIndexedRead attribute")
                        .value()
                        == "Y",
                })
            } else {
                bail!("Read was not a tag!")
            }
        }
        reads
    } else {
        bail!("Problem getting Read elements")
    };

    Ok(RunInfo {
        run_id: evaluate_xpath(&info_doc, "//Run/@Id")
            .chain_err(|| "Problem reading //Run/@Id")?
            .into_string(),
        flowcell: evaluate_xpath(&info_doc, "//Flowcell/text()")
            .chain_err(|| "Problem reading //Flowcell/text()")?
            .into_string(),
        instrument: evaluate_xpath(&info_doc, "//Instrument/text()")
            .chain_err(|| "Problem reading //Instrument/text()")?
            .into_string(),
        date: evaluate_xpath(&info_doc, "//Date/text()")
            .chain_err(|| "Problem reading //Date/text()")?
            .into_string(),
        lane_count: evaluate_xpath(&info_doc, "//FlowcellLayout/@LaneCount")
            .chain_err(|| "Problem reading //FlowcellLayout/@LaneCount")?
            .into_number() as i32,
        reads: reads,
    })
}

#[derive(Debug)]
struct RunParameters {
    pub planned_reads: Vec<ReadDescription>,
    pub rta_version: String,
    pub run_number: i32,
    pub flowcell_slot: String,
    pub experiment_name: String,
}

fn process_xml_param_doc_miseq(info_doc: &Document) -> Result<RunParameters> {
    let reads = if let Value::Nodeset(nodeset) =
        evaluate_xpath(&info_doc, "//Read").chain_err(|| "Problem finding Read tags")?
    {
        let mut reads = Vec::new();
        for node in nodeset.document_order() {
            if let Node::Element(elem) = node {
                reads.push(ReadDescription {
                    number: elem
                        .attribute("Number")
                        .expect("Problem accessing Number attribute")
                        .value()
                        .to_string()
                        .parse::<i32>()
                        .unwrap(),
                    num_cycles: elem
                        .attribute("NumCycles")
                        .expect("Problem accessing NumCycles attribute")
                        .value()
                        .to_string()
                        .parse::<i32>()
                        .unwrap(),
                    is_index: elem
                        .attribute("IsIndexedRead")
                        .expect("Problem accessing IsIndexedRead attribute")
                        .value()
                        == "Y",
                })
            } else {
                bail!("Read was not a tag!")
            }
        }
        reads
    } else {
        bail!("Problem getting Read elements")
    };

    Ok(RunParameters {
        planned_reads: reads,
        rta_version: evaluate_xpath(&info_doc, "//RTAVersion/text()")
            .chain_err(|| "Problem getting RTAVersion element")?
            .into_string(),
        run_number: evaluate_xpath(&info_doc, "//ScanNumber/text()")
            .chain_err(|| "Problem getting ScanNumber element")?
            .into_number() as i32,
        flowcell_slot: if let Ok(elem) = evaluate_xpath(&info_doc, "//FCPosition/text()") {
            elem.into_string()
        } else {
            "A".to_string()
        },
        experiment_name: if let Ok(elem) = evaluate_xpath(&info_doc, "//ExperimentName/text()") {
            elem.into_string()
        } else {
            "".to_string()
        },
    })
}

fn process_xml_param_doc_miniseq(info_doc: &Document) -> Result<RunParameters> {
    let mut reads = Vec::new();
    let mut number = 1;

    if let Ok(value) = evaluate_xpath(&info_doc, "//PlannedRead1Cycles/text()") {
        reads.push(ReadDescription {
            number: number,
            num_cycles: value.into_number() as i32,
            is_index: false,
        });
        number += 1;
    }

    if let Ok(value) = evaluate_xpath(&info_doc, "//PlannedRead2Cycles/text()") {
        reads.push(ReadDescription {
            number: number,
            num_cycles: value.into_number() as i32,
            is_index: false,
        });
        number += 1;
    }

    if let Ok(value) = evaluate_xpath(&info_doc, "//PlannedIndex1ReadCycles/text()") {
        reads.push(ReadDescription {
            number: number,
            num_cycles: value.into_number() as i32,
            is_index: true,
        });
        number += 1;
    }

    if let Ok(value) = evaluate_xpath(&info_doc, "//PlannedIndex2ReadCycles/text()") {
        reads.push(ReadDescription {
            number: number,
            num_cycles: value.into_number() as i32,
            is_index: true,
        });
        number += 1;
    }

    Ok(RunParameters {
        planned_reads: reads,
        rta_version: evaluate_xpath(&info_doc, "//RTAVersion/text()")
            .chain_err(|| "Problem getting RTAVersion element")?
            .into_string(),
        run_number: evaluate_xpath(&info_doc, "//RunNumber/text()")
            .chain_err(|| "Problem getting RunNumber element")?
            .into_number() as i32,
        flowcell_slot: "A".to_string(), // always Slot A
        experiment_name: if let Ok(elem) = evaluate_xpath(&info_doc, "//ExperimentName/text()") {
            elem.into_string()
        } else {
            "".to_string()
        },
    })
}

fn process_xml(
    logger: &slog::Logger,
    path: &Path,
    folder_layout: FolderLayout,
    info_doc: &Document,
    param_doc: &Document,
    settings: &Settings,
) -> Result<()> {
    let run_info = process_xml_run_info(info_doc)?;
    debug!(logger, "RunInfo => {:?}", &run_info);

    let run_param = match folder_layout {
        FolderLayout::MiSeq => process_xml_param_doc_miseq(param_doc)?,
        FolderLayout::MiniSeq => process_xml_param_doc_miniseq(param_doc)?,
        _ => bail!(
            "Don't yet know how to parse folder layout {:?}",
            folder_layout
        ),
    };
    debug!(logger, "RunInfo => {:?}", &run_param);

    Ok(())
}

fn process_folder(logger: &slog::Logger, path: &Path, settings: &Settings) -> Result<()> {
    info!(logger, "Starting to process folder {:?}...", path);

    // Ensure that `RunInfo.xml` exists and try to guess folder layout.
    if !path.join("RunInfo.xml").exists() {
        error!(
            logger,
            "Path {:?}/RunInfo.xml does not exist! Skipping directory.", path
        );
        bail!("RunInfo.xml missing");
    }
    let folder_layout = match guess_folder_layout(path) {
        Ok(layout) => layout,
        Err(_e) => {
            error!(
                logger,
                "Could not guess folder layout from {:?}. Skipping.", path
            );
            bail!("Could not guess folder layout");
        }
    };

    // Parse the run info and run parameters XML files
    info!(logger, "Parsing XML files...");
    let info_pkg = {
        let mut xmlf =
            File::open(path.join("RunInfo.xml")).chain_err(|| "Problem reading RunInfo.xml")?;
        let mut contents = String::new();
        xmlf.read_to_string(&mut contents)
            .chain_err(|| "Problem reading XML from RunInfo.xml")?;
        parser::parse(&contents).chain_err(|| "Problem parsing XML from RunInfo.xml")?
    };
    let info_doc = info_pkg.as_document();

    let param_pkg = {
        let filename = match folder_layout {
            FolderLayout::MiSeq => "runParameters.xml",
            FolderLayout::MiniSeq => "RunParameters.xml",
            FolderLayout::HiSeqX => bail!("Cannot handle HiSeq X yet!"),
        };
        let mut xmlf = File::open(path.join(filename))
            .chain_err(|| format!("Problem reading {}", &filename))?;
        let mut contents = String::new();
        xmlf.read_to_string(&mut contents)
            .chain_err(|| format!("Problem reading XML from {}", &filename))?;
        parser::parse(&contents).chain_err(|| format!("Problem parsing XML from {}", &filename))?
    };
    let param_doc = param_pkg.as_document();

    // Process the XML files
    process_xml(logger, path, folder_layout, &info_doc, &param_doc, settings)?;

    info!(logger, "Done processing folder {:?}.", path);
    Ok(())
}

pub fn run(logger: &slog::Logger, settings: &Settings) -> Result<()> {
    info!(logger, "Running: digestiflow-cli-client ingest");
    info!(logger, "Options: {:?}", settings);

    let mut any_failed = false;
    for ref path in &settings.ingest.path {
        let path = Path::new(path);
        match process_folder(logger, &path, settings) {
            Err(e) => {
                any_failed = true;
                error!(logger, "Folder processing failed: {:?}", &e);
                warn!(
                    logger,
                    "Processing folder {:?} failed. Will go on with other paths but the program \
                     call will not have return code 0!",
                    &path
                );
            }
            _ => (),
        }
    }

    // parse folder name
    // parse folder contents
    //
    // try to get flowcell from API
    // if not exists:
    //    register flow cell
    // else if different:
    //    update flow cell

    // analyze read count
    // post to API

    if any_failed {
        bail!("Processing of at least one folder failed!")
    } else {
        Ok(())
    }
}
