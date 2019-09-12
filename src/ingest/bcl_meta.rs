//! Code for accessing data in the raw output directories.

use chrono::{NaiveDate, NaiveDateTime};
use std::path::Path;
use sxd_document::dom::Document;
use sxd_xpath::nodeset::Node;
use sxd_xpath::{evaluate_xpath, Value};

use super::super::errors::*;

#[derive(PartialEq, Eq, Debug, Copy, Clone)]
pub enum FolderLayout {
    /// MiSeq, HiSeq 2000, etc. `runParameters.xml`
    MiSeq,
    /// MiniSeq, NextSeq etc. `RunParameters.xml`
    MiniSeq,
    /// HiSeq X
    HiSeqX,
    /// NovaSeq
    NovaSeq,
}

pub fn guess_folder_layout(path: &Path) -> Result<FolderLayout> {
    let miniseq_markers = vec![
        path.join("Data")
            .join("Intensities")
            .join("BaseCalls")
            .join("L001"),
        path.join("RunParameters.xml"),
    ];
    let miseq_marker = vec![
        path.join("Data")
            .join("Intensities")
            .join("BaseCalls")
            .join("L001")
            .join("C1.1"),
        path.join("runParameters.xml"),
    ];
    let hiseqx_marker = vec![
        path.join("Data").join("Intensities").join("s.locs"),
        path.join("RunParameters.xml"),
    ];
    let novaseq_marker_any = vec![
        path.join("Data")
            .join("Intensities")
            .join("BaseCalls")
            .join("L001")
            .join("C1.1")
            .join("L001_1.cbcl"),
        path.join("Data")
            .join("Intensities")
            .join("BaseCalls")
            .join("L001")
            .join("C1.1")
            .join("L001_2.cbcl"),
    ];
    let novaseq_marker_all = vec![path.join("RunParameters.xml")];

    if novaseq_marker_all.iter().all(|ref m| m.exists())
        && novaseq_marker_any.iter().any(|ref m| m.exists())
    {
        Ok(FolderLayout::NovaSeq)
    } else if miseq_marker.iter().all(|ref m| m.exists()) {
        Ok(FolderLayout::MiSeq)
    } else if miniseq_markers.iter().all(|ref m| m.exists()) {
        Ok(FolderLayout::MiniSeq)
    } else if hiseqx_marker.iter().all(|ref m| m.exists()) {
        Ok(FolderLayout::HiSeqX)
    } else {
        bail!("Could not guess folder layout from {:?}", path)
    }
}

#[derive(Debug, PartialEq, Eq)]
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
        evaluate_xpath(&info_doc, "//RunInfoRead|//Read")
            .chain_err(|| "Problem finding Read or RunInfoRead tags")?
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
        bail!("Problem getting Read or RunInfoRead elements")
    };

    let xml_date = evaluate_xpath(&info_doc, "//Date/text()")
        .chain_err(|| "Problem reading //Date/text()")?
        .into_string();
    let date_string = if let Ok(good) = NaiveDate::parse_from_str(&xml_date, "%y%m%d") {
        good.format("%F").to_string()
    } else {
        if let Ok(good) = NaiveDateTime::parse_from_str(&xml_date, "%-m/%-d/%Y %-I:%M:%S %p") {
            good.format("%F").to_string()
        } else {
            bail!("Could not parse date from string {}", &xml_date);
        }
    };

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
        date: date_string,
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
        evaluate_xpath(&info_doc, "//RunInfoRead|//Read")
            .chain_err(|| "Problem finding Read or RunInfoRead tags")?
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
                bail!("Read or RunInfoRead was not a tag!")
            }
        }
        reads
    } else {
        bail!("Problem getting Read or RunInfoRead elements")
    };

    let rta_version = evaluate_xpath(&info_doc, "//RTAVersion/text()")
        .chain_err(|| "Problem getting RTAVersion element")?
        .into_string();
    let rta_version3 = evaluate_xpath(&info_doc, "//RtaVersion/text()")
        .chain_err(|| "Problem getting RTAVersion element")?
        .into_string();

    Ok(RunParameters {
        planned_reads: reads,
        rta_version: if !rta_version3.is_empty() {
            rta_version3[1..].to_string()
        } else {
            rta_version
        },
        run_number: evaluate_xpath(&info_doc, "//ScanNumber/text()")
            .chain_err(|| "Problem getting ScanNumber element")?
            .into_number() as i32,
        flowcell_slot: if let Ok(elem) = evaluate_xpath(&info_doc, "//FCPosition/text()") {
            let elem = elem.into_string();
            if elem.is_empty() {
                "A".to_string()
            } else {
                elem
            }
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
            number += 1;
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

    let rta_version = evaluate_xpath(&info_doc, "//RTAVersion/text()")
        .chain_err(|| "Problem getting RTAVersion element")?
        .into_string();
    let rta_version3 = evaluate_xpath(&info_doc, "//RtaVersion/text()")
        .chain_err(|| "Problem getting RTAVersion element")?
        .into_string();

    Ok(RunParameters {
        planned_reads: reads,
        rta_version: if !rta_version3.is_empty() {
            rta_version3[1..].to_string()
        } else {
            rta_version
        },
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
        FolderLayout::MiniSeq | FolderLayout::NovaSeq => process_xml_param_doc_miniseq(param_doc)?,
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
    } else if (!run_params.planned_reads.is_empty()) && (run_info.reads != run_params.planned_reads)
    {
        return "failed".to_string();
    } else if path.join("RTAComplete.txt").exists() {
        return "complete".to_string();
    } else {
        return "in_progress".to_string();
    }
}
