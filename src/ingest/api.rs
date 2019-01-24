//! Rust client code for the Digestiflow REST API.

use super::*;

use restson::{self, RestPath};

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

/// Restson arguments `resolve FlowCell by (instrument, run_number, flowcell)``.
pub struct ResolveFlowCellArgs {
    pub project_uuid: String,
    pub instrument: String,
    pub run_number: i32,
    pub flowcell: String,
}

impl<'a> RestPath<&'a ResolveFlowCellArgs> for FlowCell {
    fn get_path(args: &'a ResolveFlowCellArgs) -> result::Result<String, restson::Error> {
        Ok(format!(
            "api/flowcells/resolve/{}/{}/{}/{}/",
            &args.project_uuid, &args.instrument, args.run_number, &args.flowcell
        ))
    }
}

// Restson arguments: PUT FlowCell for creation
pub struct ProjectArgs {
    pub project_uuid: String,
}

impl<'a> RestPath<&'a ProjectArgs> for FlowCell {
    fn get_path(args: &'a ProjectArgs) -> result::Result<String, restson::Error> {
        Ok(format!("api/flowcells/{}/", &args.project_uuid))
    }
}

// Restson arguments: GET/PUT Flowcell by SODAR UUID.
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
    pub min_index_fraction: f64,
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

/// Querying index histogram list from DigestiFlow API.
#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum LaneIndexHistogramArray {
    Array(Vec<LaneIndexHistogram>),
}

impl<'a> RestPath<&'a ProjectFlowcellArgs> for LaneIndexHistogramArray {
    fn get_path(args: &'a ProjectFlowcellArgs) -> result::Result<String, restson::Error> {
        Ok(format!(
            "api/indexhistos/{}/{}/",
            &args.project_uuid, &args.flowcell_uuid
        ))
    }
}
