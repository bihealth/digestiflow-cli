use byteorder::{LittleEndian, ReadBytesExt};
use flate2::read::MultiGzDecoder;
use glob::glob;
use rand::{Rng, SeedableRng};
use rand_xorshift;
use rayon::prelude::*;
use std::cmp;
use std::collections::HashMap;
use std::env;
use std::fs::File;
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

mod api {
    use super::*;

    /// Flow cell information from the DigestiFlow API.
    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub struct FlowCell {
        pub sodar_uuid: Option<String>,
        pub run_date: String,
        pub run_number: i32,
        pub slot: String,
        pub vendor_id: String,
        pub label: Option<String>,
        pub manual_label: Option<String>,
        pub description: Option<String>,
        pub sequencing_machine: String,
        pub num_lanes: i32,
        pub operator: Option<String>,
        pub rta_version: i32,
        pub status_sequencing: String,
        pub status_conversion: String,
        pub status_delivery: String,
        pub delivery_type: String,
        pub planned_reads: Option<String>,
        pub current_reads: Option<String>,
    }

    // Restson: resolve flowcel by (instrument, run_number, flowcell).

    pub struct ResolveFlowCellArgs {
        pub project_uuid: String,
        pub instrument: String,
        pub run_number: i32,
        pub flowcell: String,
    }

    impl<'a> RestPath<&'a ResolveFlowCellArgs> for FlowCell {
        fn get_path(args: &'a ResolveFlowCellArgs) -> result::Result<String, restson::Error> {
            Ok(format!(
                "api/flowcells/{}/resolve/{}/{}/{}/",
                &args.project_uuid, &args.instrument, args.run_number, &args.flowcell
            ))
        }
    }

    // Restson: PUT FlowCell for creation

    pub struct ProjectArgs {
        pub project_uuid: String,
    }

    impl<'a> RestPath<&'a ProjectArgs> for FlowCell {
        fn get_path(args: &'a ProjectArgs) -> result::Result<String, restson::Error> {
            Ok(format!("api/flowcells/{}/", &args.project_uuid))
        }
    }

    // Restson: GET/PUT Flowcell by SODAR UUID.

    pub struct ProjectFlowcellArgs {
        pub project_uuid: String,
        pub flowcell_uuid: String,
    }

    impl<'a> RestPath<&'a ProjectFlowcellArgs> for FlowCell {
        fn get_path(args: &'a ProjectFlowcellArgs) -> result::Result<String, restson::Error> {
            Ok(format!(
                "api/flowcells/{}/{}/",
                &args.project_uuid, &args.flowcell_uuid
            ))
        }
    }

    /// Index histogram information from the DigestiFlow API.
    #[derive(Debug, Serialize, Deserialize)]
    pub struct LaneIndexHistogram {
        pub sodar_uuid: Option<String>,
        pub flowcell: String,
        pub lane: i32,
        pub index_read_no: i32,
        pub sample_size: usize,
        pub histogram: HashMap<String, usize>,
    }

    impl<'a> RestPath<&'a ProjectFlowcellArgs> for LaneIndexHistogram {
        fn get_path(args: &'a ProjectFlowcellArgs) -> result::Result<String, restson::Error> {
            Ok(format!(
                "api/indexhistos/{}/{}/",
                &args.project_uuid, &args.flowcell_uuid
            ))
        }
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
) -> api::FlowCell {
    api::FlowCell {
        sodar_uuid: None,
        run_date: run_info.date.clone(),
        run_number: run_info.run_number,
        slot: run_params.flowcell_slot.clone(),
        vendor_id: run_info.flowcell.clone(),
        label: Some(run_params.experiment_name.clone()),
        num_lanes: run_info.lane_count,
        rta_version: if run_params.rta_version.starts_with(&"2") {
            2
        } else {
            1
        },
        planned_reads: Some(string_description(&run_params.planned_reads)),
        current_reads: Some(string_description(&run_info.reads)),
        manual_label: None,
        description: None,
        sequencing_machine: run_info.instrument.clone(),
        operator: Some(settings.ingest.operator.clone()),
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
) -> Result<api::FlowCell> {
    info!(logger, "Registering flow cell...");

    let flowcell = build_flow_cell(run_info, run_params, settings);
    debug!(logger, "Registering flowcell as {:?}", &flowcell);

    let args = api::ProjectArgs {
        project_uuid: settings.ingest.project_uuid.clone(),
    };
    let flowcell = client
        .post_capture(&args, &flowcell)
        .chain_err(|| "Problem registering data")?;
    debug!(logger, "Registered flowcell: {:?}", &flowcell);

    info!(logger, "Done registering flow cell.");

    Ok(flowcell)
}

fn update_flowcell(
    logger: &slog::Logger,
    client: &mut RestClient,
    flowcell: &api::FlowCell,
    run_info: &RunInfo,
    run_params: &RunParameters,
    settings: &Settings,
) -> Result<api::FlowCell> {
    info!(logger, "Updating flow cell...");

    let rebuilt = build_flow_cell(run_info, run_params, settings);

    let flowcell = api::FlowCell {
        planned_reads: rebuilt.planned_reads.clone(),
        current_reads: rebuilt.current_reads.clone(),
        ..flowcell.clone()
    };
    info!(logger, "Will update flow cell");
    debug!(logger, "  {:?} => {:?}", &flowcell, &rebuilt);

    let args = api::ProjectFlowcellArgs {
        project_uuid: settings.ingest.project_uuid.clone(),
        flowcell_uuid: flowcell.sodar_uuid.clone().unwrap(),
    };
    client
        .put_capture(&args, &flowcell)
        .chain_err(|| "Problem updating")
}

/// A list of BCL files defining a stack of base calls for a tile.
#[derive(Debug)]
struct TileBclStack {
    /// The number of the lane that this stack is for.
    pub lane_no: i32,
    /// The paths to the BCL files.
    pub paths: Vec<String>,
}

/// For a given index read, a histogram of counts (probably cut to top 1% or so).
#[derive(Debug)]
struct IndexCounts {
    /// The index of the index.
    pub index_no: i32,
    /// The index of the lane.
    pub lane_no: i32,
    /// The number of reads read.
    pub sample_size: usize,
    /// The filtered histogram of read frequencies.
    pub hist: HashMap<String, usize>,
}

/// Analyze a single stack.
fn analyze_stacks(
    logger: &slog::Logger,
    lane_stacks: &Vec<Vec<TileBclStack>>,
    stack_no: usize,
    index_no: i32,
    settings: &Settings,
) -> Result<Vec<IndexCounts>> {
    lane_stacks
        .par_iter()
        .map(|ref stacks_for_lane| {
            let stack = &stacks_for_lane[stack_no];
            // Read in the bases from the bcl files.
            let bases = stack
                .paths
                .par_iter()
                .map(|ref path| {
                    // Open file
                    debug!(logger, "Processing file {}...", &path);
                    let file = File::open(&path).chain_err(|| "Problem opening gzip file")?;
                    let mut gz_decoder = MultiGzDecoder::new(file);

                    // Read number of bytes in file.
                    let num_bytes = gz_decoder
                        .read_u32::<LittleEndian>()
                        .chain_err(|| "Problem reading byte count")?
                        as usize;

                    // Read array with bases and quality values.
                    let num_bytes = if settings.ingest.sample_reads_per_tile > 0 {
                        cmp::min(num_bytes, settings.ingest.sample_reads_per_tile as usize)
                    } else {
                        num_bytes
                    };
                    let mut buf = vec![0u8; num_bytes];
                    gz_decoder
                        .read_exact(&mut buf)
                        .chain_err(|| "Problem reading payload")?;

                    // Build bases for each spot, use no-call if all bits are unset.
                    let table = vec!['A', 'C', 'G', 'T'];
                    let mut chars = Vec::new();
                    for i in 0..num_bytes {
                        if buf[i] == 0 {
                            chars.push('N');
                        } else {
                            chars.push(table[(buf[i] & 3) as usize]);
                        }
                    }
                    debug!(logger, "Done processing {}.", &path);

                    Ok(chars)
                })
                .collect::<Result<Vec<_>>>()?;

            // Build read sequences.
            debug!(logger, "Building read sequences.");
            let num_seqs = bases[0].len();
            let seqs = (0..num_seqs)
                .into_par_iter()
                .map(|i| {
                    let mut seq = String::new();
                    for j in 0..(bases.len()) {
                        seq.push(bases[j][i]);
                    }
                    seq
                })
                .collect::<Vec<String>>();
            debug!(logger, "Done building read sequences.");

            // TODO: parallelize counting?

            // Build histogram.
            let mut hist: HashMap<String, usize> = HashMap::new();
            for seq in &seqs {
                *hist.entry(seq.clone()).or_insert(1) += 1;
            }

            // Filter to top 1%.
            let mut filtered_hist = HashMap::new();
            for (seq, count) in hist {
                if count > num_seqs / 100 {
                    filtered_hist.insert(seq.clone(), count);
                }
            }
            debug!(logger, "=> filtered hist {:?}", &filtered_hist);

            Ok(IndexCounts {
                index_no: index_no,
                lane_no: stack.lane_no,
                sample_size: num_seqs,
                hist: filtered_hist,
            })
        })
        .collect()
}

fn find_file_stacks(
    _logger: &slog::Logger,
    folder_layout: FolderLayout,
    desc: &ReadDescription,
    path: &Path,
    start_cycle: i32,
) -> Result<Vec<Vec<TileBclStack>>> {
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

            let mut lane_stacks = Vec::new();
            for (lane_no, ref lane_path) in lane_paths.iter().enumerate() {
                let mut paths: Vec<String> = Vec::new();
                for cycle in start_cycle..(start_cycle + desc.num_cycles) {
                    paths.push(
                        Path::new(lane_path)
                            .join(format!("{:04}.bcl.bgzf", cycle))
                            .to_str()
                            .unwrap()
                            .to_string(),
                    );
                }
                lane_stacks.push(vec![TileBclStack {
                    lane_no: lane_no as i32 + 1,
                    paths: paths,
                }]);
            }

            Ok(lane_stacks)
        }
        FolderLayout::MiSeq => {
            let path = path
                .join("Data")
                .join("Intensities")
                .join("BaseCalls")
                .join("L???");
            let lane_paths = glob(path.to_str().unwrap())
                .expect("Failed to read glob pattern")
                .map(|x| x.unwrap().to_str().unwrap().to_string())
                .collect::<Vec<String>>();

            let mut tile_stacks = Vec::new();
            for (lane_no, ref lane_path) in lane_paths.iter().enumerate() {
                let mut lane_stacks = Vec::new();
                let path = Path::new(lane_path).join("C1.1").join("s_?_????.bcl.gz");
                for prototype in glob(path.to_str().unwrap()).unwrap() {
                    let path = prototype.unwrap();
                    let file_name = path.file_name().unwrap();
                    let mut paths: Vec<String> = Vec::new();
                    for cycle in start_cycle..(start_cycle + desc.num_cycles) {
                        let path = Path::new(lane_path)
                            .join(format!("C{}.1", cycle))
                            .join(file_name);
                        paths.push(path.to_str().unwrap().to_string());
                    }
                    lane_stacks.push(TileBclStack {
                        lane_no: lane_no as i32 + 1,
                        paths: paths,
                    });
                }
                tile_stacks.push(lane_stacks);
            }

            Ok(tile_stacks)
        }
        _ => bail!(
            "Don't know yet how to process folder layout {:?}",
            folder_layout
        ),
    }
}

/// Sample adapters for the given index read described in `desc` and return
/// `IndexCounts` for each lane.
fn sample_adapters(
    logger: &slog::Logger,
    path: &Path,
    desc: &ReadDescription,
    folder_layout: FolderLayout,
    settings: &Settings,
    index_no: i32,
    start_cycle: i32,
) -> Result<Vec<IndexCounts>> {
    // Depending on the directory layout, build stacks of files to get adapters from.
    // Through this abstraction, we can treat the different layouts the same in
    // extracting the adapters.
    info!(logger, "Getting paths to base call files...");
    let stacks = find_file_stacks(logger, folder_layout, desc, path, start_cycle)
        .chain_err(|| "Problem building paths to files")?;

    let mut rng = rand_xorshift::XorShiftRng::seed_from_u64(settings.seed);
    let stack_no = rng.gen_range(0, stacks[0].len());

    info!(logger, "Analyzing base call files...");
    let counts = analyze_stacks(logger, &stacks, stack_no, index_no, settings)
        .chain_err(|| "Problem with analyzing stacks")?;

    Ok(counts)
}

fn analyze_adapters(
    logger: &slog::Logger,
    flowcell: &api::FlowCell,
    client: &mut RestClient,
    run_info: &RunInfo,
    path: &Path,
    folder_layout: FolderLayout,
    settings: &Settings,
) -> Result<()> {
    info!(logger, "Analyzing adapters...");

    let mut index_no = 0i32;
    let mut cycle = 1i32; // always throw away first cycle
    for ref desc in &run_info.reads {
        if desc.is_index {
            index_no += 1;
            let index_counts = sample_adapters(
                logger,
                path,
                &desc,
                folder_layout,
                settings,
                index_no,
                cycle,
            )?;

            // Push results to API
            if settings.ingest.post_adapters {
                for (i, index_info) in index_counts.iter().enumerate() {
                    let lane_no = i + 1;
                    let api_hist = api::LaneIndexHistogram {
                        sodar_uuid: None,
                        flowcell: flowcell.sodar_uuid.clone().unwrap(),
                        lane: lane_no as i32,
                        index_read_no: index_no,
                        sample_size: index_info.sample_size,
                        histogram: index_info.hist.clone(),
                    };
                    client
                        .post(
                            &api::ProjectFlowcellArgs {
                                project_uuid: settings.ingest.project_uuid.clone(),
                                flowcell_uuid: flowcell.sodar_uuid.clone().unwrap(),
                            },
                            &api_hist,
                        )
                        .chain_err(|| "Could not update adapter on server")?
                }
            }
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
    let (run_info, run_params) = process_xml(logger, folder_layout, &info_doc, &param_doc)?;

    // Try to get the flow cell information from API.
    debug!(logger, "Connecting to \"{}\"", &settings.web.url);
    if settings.log_token {
        debug!(
            logger,
            "  (using header 'Authorization: Token {}')", &settings.web.token
        );
    }
    let mut client = RestClient::new(&settings.web.url).unwrap();
    client
        .set_header("Authorization", &format!("Token {}", &settings.web.token))
        .chain_err(|| "Problem configuring REST client")?;
    let result: result::Result<api::FlowCell, restson::Error> =
        client.get(&api::ResolveFlowCellArgs {
            project_uuid: settings.ingest.project_uuid.clone(),
            instrument: run_info.instrument.clone(),
            run_number: run_info.run_number,
            flowcell: run_info.flowcell.clone(),
        });
    let flowcell: api::FlowCell = if settings.ingest.register || settings.ingest.update {
        // Update or create if necessary.
        match result {
            Ok(flowcell) => {
                debug!(logger, "Flow cell found with value {:?}", &flowcell);
                if settings.ingest.update {
                    update_flowcell(
                        logger,
                        &mut client,
                        &flowcell,
                        &run_info,
                        &run_params,
                        &settings,
                    )?
                } else {
                    flowcell
                }
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
            _x => bail!("Problem resolving flowcell {:?}", &_x),
        }
    } else {
        // TODO: improve error handling
        result.expect("Flowcell not found but we are not supposed to register")
    };

    if settings.ingest.analyze_adapters {
        analyze_adapters(
            logger,
            &flowcell,
            &mut client,
            &run_info,
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
    env::set_var("RAYON_NUM_THREADS", format!("{}", settings.threads));

    // Bail out in case of missing project UUID.
    if settings.ingest.project_uuid.is_empty() {
        bail!("You have to specify the project UUID");
    }

    // Setting number of threads to use in Rayon.
    debug!(logger, "Using {} threads", settings.threads);
    env::set_var("RAYON_NUM_THREADS", format!("{}", settings.threads));

    let any_failed: bool = settings.ingest.path./*par_*/iter().map(|ref path| {
        let path = Path::new(path);
        match process_folder(logger, &path, settings) {
            Err(e) => {
                error!(logger, "Folder processing failed: {:?}", &e);
                warn!(
                    logger,
                    "Processing folder {:?} failed. Will go on with other paths but the program \
                     call will not have return code 0!",
                    &path
                );
                true // == any failed
            }
            _ => false,  // == any failed
        }
    }).any(|failed| failed);

    if any_failed {
        bail!("Processing of at least one folder failed!")
    } else {
        Ok(())
    }
}
