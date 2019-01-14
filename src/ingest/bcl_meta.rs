//! Code for accessing data in the raw output directories.

use std::path::Path;
use sxd_document::dom::Document;
use sxd_xpath::nodeset::Node;
use sxd_xpath::{evaluate_xpath, Value};

use super::super::errors::*;

#[derive(Debug, Copy, Clone)]
pub enum FolderLayout {
    /// MiSeq, HiSeq 2000, etc. `runParameters.xml`
    MiSeq,
    /// MiniSeq, NextSeq etc. `RunParameters.xml`
    MiniSeq,
    /// HiSeq X
    HiSeqX,
}

pub fn guess_folder_layout(path: &Path) -> Result<FolderLayout> {
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

#[derive(Debug, PartialEq)]
pub struct ReadDescription {
    pub number: i32,
    pub num_cycles: i32,
    pub is_index: bool,
}

pub fn string_description(read_descs: &Vec<ReadDescription>) -> String {
    read_descs
        .iter()
        .map(|x| format!("{}{}", x.num_cycles, if x.is_index { "B" } else { "T" }))
        .collect::<Vec<String>>()
        .join("")
}

#[derive(Debug)]
pub struct RunInfo {
    /// The long, full run ID.
    pub run_id: String,
    pub run_number: i32,
    pub flowcell: String,
    pub instrument: String,
    pub date: String,
    pub lane_count: i32,
    pub reads: Vec<ReadDescription>,
}

pub fn process_xml_run_info(info_doc: &Document) -> Result<RunInfo> {
    let reads = if let Value::Nodeset(nodeset) =
        evaluate_xpath(&info_doc, "//Read").chain_err(|| "Problem finding Read tags")?
    {
        let mut reads = Vec::new();
        for node in nodeset.document_order() {
            if let Node::Element(elem) = node {
                let num_cycles = elem
                    .attribute("NumCycles")
                    .expect("Problem accessing NumCycles attribute")
                    .value()
                    .to_string()
                    .parse::<i32>()
                    .unwrap();
                if num_cycles > 0 {
                    reads.push(ReadDescription {
                        number: elem
                            .attribute("Number")
                            .expect("Problem accessing Number attribute")
                            .value()
                            .to_string()
                            .parse::<i32>()
                            .unwrap(),
                        num_cycles: num_cycles,
                        is_index: elem
                            .attribute("IsIndexedRead")
                            .expect("Problem accessing IsIndexedRead attribute")
                            .value()
                            == "Y",
                    })
                }
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
pub struct RunParameters {
    pub planned_reads: Vec<ReadDescription>,
    pub rta_version: String,
    pub run_number: i32,
    pub flowcell_slot: String,
    pub experiment_name: String,
}

pub fn process_xml_param_doc_miseq(info_doc: &Document) -> Result<RunParameters> {
    let reads = if let Value::Nodeset(nodeset) =
        evaluate_xpath(&info_doc, "//Read").chain_err(|| "Problem finding Read tags")?
    {
        let mut reads = Vec::new();
        for node in nodeset.document_order() {
            if let Node::Element(elem) = node {
                let num_cycles = elem
                    .attribute("NumCycles")
                    .expect("Problem accessing NumCycles attribute")
                    .value()
                    .to_string()
                    .parse::<i32>()
                    .unwrap();
                if num_cycles > 0 {
                    reads.push(ReadDescription {
                        number: elem
                            .attribute("Number")
                            .expect("Problem accessing Number attribute")
                            .value()
                            .to_string()
                            .parse::<i32>()
                            .unwrap(),
                        num_cycles: num_cycles,
                        is_index: elem
                            .attribute("IsIndexedRead")
                            .expect("Problem accessing IsIndexedRead attribute")
                            .value()
                            == "Y",
                    })
                }
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

pub fn process_xml_param_doc_miniseq(info_doc: &Document) -> Result<RunParameters> {
    let mut reads = Vec::new();
    let mut number = 1;

    if let Ok(value) = evaluate_xpath(&info_doc, "//PlannedRead1Cycles/text()") {
        let num_cycles = value.into_number() as i32;
        if num_cycles != 0 {
            reads.push(ReadDescription {
                number: number,
                num_cycles: num_cycles,
                is_index: false,
            });
            number += 1;
        }
    }

    if let Ok(value) = evaluate_xpath(&info_doc, "//PlannedIndex1ReadCycles/text()") {
        let num_cycles = value.into_number() as i32;
        if num_cycles != 0 {
            reads.push(ReadDescription {
                number: number,
                num_cycles: num_cycles,
                is_index: true,
            });
            number += 1;
        }
    }

    if let Ok(value) = evaluate_xpath(&info_doc, "//PlannedIndex2ReadCycles/text()") {
        let num_cycles = value.into_number() as i32;
        if num_cycles != 0 {
            reads.push(ReadDescription {
                number: number,
                num_cycles: num_cycles,
                is_index: true,
            });
        }
    }

    if let Ok(value) = evaluate_xpath(&info_doc, "//PlannedRead2Cycles/text()") {
        let num_cycles = value.into_number() as i32;
        if num_cycles != 0 {
            reads.push(ReadDescription {
                number: number,
                num_cycles: num_cycles,
                is_index: false,
            });
            // number += 1;
        }
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

pub fn process_xml(
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

pub fn get_status_sequencing(
    run_info: &RunInfo,
    run_params: &RunParameters,
    path: &Path,
    current_status: &str,
) -> String {
    if current_status == "closed" || current_status == "failed" || current_status == "complete" {
        return current_status.to_string();
    } else if run_info.reads != run_params.planned_reads {
        return "failed".to_string();
    } else if path.join("RTAComplete.txt").exists() {
        return "complete".to_string();
    } else {
        return "in_progress".to_string();
    }
}
