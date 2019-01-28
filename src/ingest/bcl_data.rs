//! Code for analyzing raw BCL data.

use byteorder::{LittleEndian, ReadBytesExt};
use flate2::read::MultiGzDecoder;
use glob::glob;
use rand::{Rng, SeedableRng};
use rand_xorshift;
use rayon::prelude::*;
use std::cmp;
use std::collections::HashMap;
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;

use super::super::errors::*;
use ingest::bcl_meta::*;
use settings::Settings;

/// A list of BCL files defining a stack of base calls for a tile.
#[derive(Debug)]
pub struct TileBclStack {
    /// The number of the lane that this stack is for.
    pub lane_no: i32,
    /// The paths to the BCL files.
    pub paths: Vec<String>,
}

/// For a given index read, a histogram of counts (probably cut to top 1% or so).
#[derive(Debug)]
pub struct IndexCounts {
    /// The index of the index.
    pub index_no: i32,
    /// The index of the lane.
    pub lane_no: i32,
    /// The number of reads read.
    pub sample_size: usize,
    /// The filtered histogram of read frequencies.
    pub hist: HashMap<String, usize>,
}

/// Load compressed BCL file.
fn load_bcl_gz(logger: &slog::Logger, path: &str, settings: &Settings) -> Result<Vec<u8>> {
    // Open file
    debug!(logger, "Processing compressed BCL file {}...", &path);
    let file = File::open(&path).chain_err(|| "Problem opening gzip file")?;
    let mut gz_decoder = MultiGzDecoder::new(file);

    // Read number of bytes in file.
    let num_bytes = gz_decoder
        .read_u32::<LittleEndian>()
        .chain_err(|| "Problem reading byte count")? as usize;

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

    Ok(buf)
}

/// Load compressed BCL file.
fn load_bcl(logger: &slog::Logger, path: &str, settings: &Settings) -> Result<Vec<u8>> {
    // Open file
    debug!(logger, "Processing uncompressed BCL file {}...", &path);
    let mut file = File::open(&path).chain_err(|| "Problem opening BCL file")?;

    // Read number of bytes in file.
    let num_bytes = file
        .read_u32::<LittleEndian>()
        .chain_err(|| "Problem reading byte count")? as usize;

    // Read array with bases and quality values.
    let num_bytes = if settings.ingest.sample_reads_per_tile > 0 {
        cmp::min(num_bytes, settings.ingest.sample_reads_per_tile as usize)
    } else {
        num_bytes
    };
    let mut buf = vec![0u8; num_bytes];
    file.read_exact(&mut buf)
        .chain_err(|| "Problem reading payload")?;

    Ok(buf)
}

/// Analyze a single stack.
pub fn analyze_stacks(
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
                    let buf = if path.ends_with(".gz") || path.ends_with(".bgzf") {
                        load_bcl_gz(logger, &path, settings)
                    } else {
                        load_bcl(logger, &path, settings)
                    }
                    .chain_err(|| "Problem loading BCL file.")?;

                    // Build bases for each spot, use no-call if all bits are unset.
                    let table = vec!['A', 'C', 'G', 'T'];
                    let mut chars = Vec::new();
                    for i in 0..buf.len() {
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
                if count as f64 > (num_seqs as f64) * settings.ingest.min_index_fraction {
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

pub fn find_file_stacks(
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
                for suffix in &["", ".gz"] {
                    let path = Path::new(lane_path)
                        .join("C1.1")
                        .join(format!("s_?_????.bcl{}", &suffix));
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
pub fn sample_adapters(
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
