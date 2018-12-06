use byteorder::{LittleEndian, ReadBytesExt};
use flate2::read::GzDecoder;
use glob::glob;
use std::collections::HashMap;
use std::fs::File;
// use std::io;
use std::io::prelude::*;
use std::path::Path;
use std::result;

use sxd_document::dom::Document;
use sxd_document::parser;
use sxd_xpath::nodeset::Node;
use sxd_xpath::{evaluate_xpath, Value};

use super::errors::*;
use settings::Settings;

use restson::{self, RestClient, RestPath};

/// Flow cell information from the DigestiFlow API.
#[derive(Debug, Serialize, Deserialize)]
struct DigestiflowFlowCell {
    pub sodar_uuid: Option<String>,
    pub run_date: String,
    pub run_number: i32,
    pub slot: String,
    pub vendor_id: String,
    pub label: String,
    pub manual_label: String,
    pub description: String,
    pub sequencing_machine: String,
    pub num_lanes: i32,
    pub operator: String,
    pub rta_version: i32,
    pub status_sequencing: String,
    pub status_conversion: String,
    pub status_delivery: String,
    pub delivery_type: String,
    pub planned_reads: String,
    pub current_reads: String,
}

// Restson: resolve flowcel by (instrument, run_number, flowcell).

struct ResolveFlowCellArgs {
    pub project_uuid: String,
    pub instrument: String,
    pub run_number: i32,
    pub flowcell: String,
}

impl<'a> RestPath<&'a ResolveFlowCellArgs> for DigestiflowFlowCell {
    fn get_path(args: &'a ResolveFlowCellArgs) -> result::Result<String, restson::Error> {
        Ok(format!(
            "api/flowcells/{}/resolve/{}/{}/{}/",
            &args.project_uuid, &args.instrument, args.run_number, &args.flowcell
        ))
    }
}

// Restson: PUT FlowCell for creation

struct ProjectArgs {
    project_uuid: String,
}

impl<'a> RestPath<&'a ProjectArgs> for DigestiflowFlowCell {
    fn get_path(args: &'a ProjectArgs) -> result::Result<String, restson::Error> {
        Ok(format!("api/flowcells/{}/", &args.project_uuid))
    }
}

// Restson: GET/PUT Flowcell by SODAR UUID.

struct ProjectFlowcellArgs {
    project_uuid: String,
    flowcell_uuid: String,
}

impl<'a> RestPath<&'a ProjectFlowcellArgs> for DigestiflowFlowCell {
    fn get_path(args: &'a ProjectFlowcellArgs) -> result::Result<String, restson::Error> {
        Ok(format!(
            "api/flowcells/{}/{}/",
            &args.project_uuid, &args.flowcell_uuid
        ))
    }
}

#[derive(Debug, Copy, Clone)]
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

fn string_description(read_descs: &Vec<ReadDescription>) -> String {
    read_descs
        .iter()
        .map(|x| format!("{}{}", x.num_cycles, if x.is_index { "B" } else { "T" }))
        .collect::<Vec<String>>()
        .join("")
}

#[derive(Debug)]
struct RunInfo {
    /// The long, full run ID.
    pub run_id: String,
    pub run_number: i32,
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

    let date = evaluate_xpath(&info_doc, "//Date/text()")
        .chain_err(|| "Problem reading //Date/text()")?
        .into_string();
    let date: String = format!("20{}-{}-{}", &date[0..2], &date[2..4], &date[4..6]);

    Ok(RunInfo {
        run_id: evaluate_xpath(&info_doc, "//Run/@Id")
            .chain_err(|| "Problem reading //Run/@Id")?
            .into_string(),
        run_number: evaluate_xpath(&info_doc, "//Run/@Number")
            .chain_err(|| "Problem reading //Run/@Number")?
            .into_number() as i32,
        flowcell: evaluate_xpath(&info_doc, "//Flowcell/text()")
            .chain_err(|| "Problem reading //Flowcell/text()")?
            .into_string(),
        instrument: evaluate_xpath(&info_doc, "//Instrument/text()")
            .chain_err(|| "Problem reading //Instrument/text()")?
            .into_string(),
        date: date,
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
) -> Result<(RunInfo, RunParameters)> {
    let run_info = process_xml_run_info(info_doc)?;
    debug!(logger, "RunInfo => {:?}", &run_info);

    let run_params = match folder_layout {
        FolderLayout::MiSeq => process_xml_param_doc_miseq(param_doc)?,
        FolderLayout::MiniSeq => process_xml_param_doc_miniseq(param_doc)?,
        _ => bail!(
            "Don't yet know how to parse folder layout {:?}",
            folder_layout
        ),
    };
    debug!(logger, "RunParameters => {:?}", &run_params);

    Ok((run_info, run_params))
}

fn build_flow_cell(
    run_info: &RunInfo,
    run_params: &RunParameters,
    settings: &Settings,
) -> DigestiflowFlowCell {
    DigestiflowFlowCell {
        sodar_uuid: None,
        run_date: run_info.date.clone(),
        run_number: run_info.run_number,
        slot: run_params.flowcell_slot.clone(),
        vendor_id: run_info.flowcell.clone(),
        label: run_params.experiment_name.clone(),
        num_lanes: run_info.lane_count,
        rta_version: if run_params.rta_version.starts_with(&"2") {
            2
        } else {
            1
        },
        planned_reads: string_description(&run_params.planned_reads),
        current_reads: string_description(&run_info.reads),
        manual_label: "".to_string(),
        description: "".to_string(),
        sequencing_machine: run_info.instrument.clone(),
        operator: settings.ingest.operator.clone(),
        status_sequencing: "initial".to_string(),
        status_conversion: "initial".to_string(),
        status_delivery: "initial".to_string(),
        delivery_type: "seq".to_string(),
    }
}

fn register_flowcell(
    logger: &slog::Logger,
    client: &mut RestClient,
    run_info: &RunInfo,
    run_params: &RunParameters,
    settings: &Settings,
) -> Result<DigestiflowFlowCell> {
    info!(logger, "Registering flow cell...");

    let flowcell = build_flow_cell(run_info, run_params, settings);
    debug!(logger, "Registering flowcell as {:?}", &flowcell);

    let args = ProjectArgs {
        project_uuid: settings.ingest.project_uuid.clone(),
    };
    let flowcell = client
        .post_capture(&args, &flowcell)
        .chain_err(|| "Problem registering data")?;
    println!("Registered flowcell: {:?}", &flowcell);

    info!(logger, "Done registering flow cell.");

    Ok(flowcell)
}

// fn tile_paths(
//     logger: &slog::Logger,
//     folder_layout: FolderLayout,
//     path: &Path,
//     lane: i32,
//     cycle: i32,
// ) -> Vec<Path> {
//     let tile_paths = Vec::new();
//     debug!(
//         logger,
//         "Paths to tiles for lane {} and cycle {} are {}", lane, cycle, tile_paths
//     );
//     file_paths
// }

fn sample_adapters(
    logger: &slog::Logger,
    path: &Path,
    desc: &ReadDescription,
    folder_layout: FolderLayout,
    settings: &Settings,
    start_cycle: i32,
) -> Result<()> {
    // Helper table for building strings below.
    let table: Vec<char> = vec!['A', 'C', 'G', 'T'];

    match folder_layout {
        FolderLayout::MiniSeq => {
            let path = path
                .join("Data")
                .join("Intensities")
                .join("BaseCalls")
                .join("L???");
            let lane_paths = glob(path.to_str().unwrap())
                .expect("Failed to read glob pattern")
                .map(|x| x.unwrap().to_str().unwrap().to_string())
                .collect::<Vec<String>>();

            for ref lane_path in &lane_paths {
                info!(logger, "Considering lane path {}", &lane_path);
                let mut reads: Vec<String> = Vec::new();

                for cycle in start_cycle..(start_cycle + desc.num_cycles) {
                    let cycle_path = Path::new(lane_path).join(format!("{:04}.bcl.bgzf", cycle));
                    info!(
                        logger,
                        "Opening file {}",
                        cycle_path.to_str().unwrap().to_string()
                    );

                    let mut file =
                        File::open(&cycle_path).chain_err(|| "Problem opening gzip file")?;
                    let mut gz_decoder = GzDecoder::new(file);
                    // Read number of bytes in file.
                    let mut buf = [0u32; 1];
                    let num_bytes = gz_decoder
                        .read_u32::<LittleEndian>()
                        .chain_err(|| "Problem reading byte count")?
                        as usize;
                    // Allocate array of strings if necessary.
                    if reads.is_empty() {
                        reads.resize(num_bytes, String::new());
                    }
                    // Read array with bases and quality values.
                    let mut buf = vec![0u8; num_bytes];
                    gz_decoder
                        .read(&mut buf[..])
                        .chain_err(|| "Problem reading payload")?;
                    // Strip quality from bases, set "N" where qualitiy is 0.
                    for i in 0..num_bytes {
                        if buf[i] == 0 {
                            reads[i].push('N');
                        } else {
                            reads[i].push(table[(buf[i] & 3) as usize]);
                        }
                    }
                }

                let mut counter: HashMap<String, u64> = HashMap::new();
                for read in &reads {
                    *counter.entry(read.clone()).or_insert(1u64) += 1;
                }

                for (seq, count) in counter.iter() {
                    if *count > 100 {
                        println!("{}\t{}", seq, count);
                    }
                }
            }
        }
        _ => bail!(
            "Don't know yet how to process folder layout {:?}",
            folder_layout
        ),
    };

    Ok(())
}

fn analyze_adapters(
    logger: &slog::Logger,
    run_info: &RunInfo,
    run_params: &RunParameters,
    path: &Path,
    folder_layout: FolderLayout,
    settings: &Settings,
) -> Result<()> {
    info!(logger, "Analyzing adapters...");

    let mut cycle = 0i32;
    for ref desc in &run_info.reads {
        if desc.is_index {
            sample_adapters(logger, path, &desc, folder_layout, settings, cycle)?;
        }
        cycle += desc.num_cycles;
    }

    info!(logger, "Done analyzing adapters.");
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

    // Process the XML files.
    let (run_info, run_params) = process_xml(logger, path, folder_layout, &info_doc, &param_doc)?;

    // Try to get the flow cell information from API.
    if settings.ingest.register || settings.ingest.update {
        let mut client = RestClient::new(&settings.web.url).unwrap();
        client
            .set_header("Authorization", &format!("Token {}", &settings.web.token))
            .chain_err(|| "Problem configuring REST client")?;
        let result: result::Result<DigestiflowFlowCell, restson::Error> =
            client.get(&ResolveFlowCellArgs {
                project_uuid: settings.ingest.project_uuid.clone(),
                instrument: run_info.instrument.clone(),
                run_number: run_info.run_number,
                flowcell: run_info.flowcell.clone(),
            });
        // Update or create if necessary.
        let flowcell: DigestiflowFlowCell = match result {
            Ok(flowcell) => {
                debug!(logger, "Flow cell found with value {:?}", &flowcell);
                if settings.ingest.update {
                    // TODO
                    // debug!(logger, "Updating flow cell...");
                    // update_flowcell(logger, &mut client, &run_info, &run_params, &settings);
                }
                flowcell
            }
            Err(restson::Error::HttpError(404, _msg)) => {
                debug!(logger, "Flow cell was not found!");
                if settings.ingest.register {
                    let flowcell =
                        register_flowcell(logger, &mut client, &run_info, &run_params, &settings)?;
                    debug!(logger, "Flow cell registered as {:?}", &flowcell);
                    flowcell
                } else {
                    info!(
                        logger,
                        "Flow cell was not found but you asked me not to \
                         register. Stopping here for this folder without \
                         error."
                    );
                    return Ok(());
                }
            }
            _ => bail!("Problem resolving flowcell"),
        };
    }

    if settings.ingest.analyze_adapters {
        analyze_adapters(
            logger,
            &run_info,
            &run_params,
            &path,
            folder_layout,
            &settings,
        )?;
    } else {
        info!(logger, "You asked me to not analyze adapters.");
    }

    info!(logger, "Done processing folder {:?}.", path);
    Ok(())
}

pub fn run(logger: &slog::Logger, settings: &Settings) -> Result<()> {
    info!(logger, "Running: digestiflow-cli-client ingest");
    info!(logger, "Options: {:?}", settings);

    // Bail out in case of missing project UUID.
    if settings.ingest.project_uuid.is_empty() {
        bail!("You have to specify the project UUID");
    }

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

    if any_failed {
        bail!("Processing of at least one folder failed!")
    } else {
        Ok(())
    }
}
