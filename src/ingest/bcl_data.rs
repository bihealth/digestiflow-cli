//! Code for analyzing raw BCL data.

use byteorder::{LittleEndian, ReadBytesExt};
use flate2::read::{GzDecoder, MultiGzDecoder};
use glob::glob;
use rand::{Rng, SeedableRng};
use rand_xorshift;
use rayon::prelude::*;
use regex::Regex;
use std::cmp;
use std::collections::HashMap;
use std::fs::File;
use std::io::prelude::*;
use std::io::SeekFrom;
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

/// Information of q-value mapping.
struct QValBinInfo {
    /// quality score bin
    _from: u32,
    /// quality score
    _to: u32,
}

/// Information of offsets in `CBCL` file.
struct OffsetInfo {
    /// ID of tile
    _tile_no: u32,
    /// Number of clusters on tile
    num_clusters: u32,
    /// Uncompressed size of tile
    _uncompressed_size: u32,
    /// Compressed size of tile
    compressed_size: u32,
    /// 1: non-PF clusters are excluded, 0: non-PF clusters are not excluded.
    _non_pf_flag: bool,
}

/// Header from a `CBCL` file.
struct CbclHeader {
    /// Version of CBL file
    _version: u16,
    /// Size of the header
    header_size: u32,
    /// Number of bits per base call, digestiflow-cli only supports 2 here
    _bits_per_basecall: u8,
    /// Number of bits per q score, digestiflow-cli only supports 6 here
    _bits_per_qscore: u8,
    /// Information of q-value bins
    _q_val_bins: Vec<QValBinInfo>,
    /// Offset information of the gzip files inside the CBCL file.
    offset_infos: Vec<OffsetInfo>,
}

/// Read CBCL header
fn load_cbcl_header(_logger: &slog::Logger, path: &str) -> Result<CbclHeader> {
    let mut file =
        File::open(&path).chain_err(|| format!("Problem opening CBCL file {}", &path))?;

    let version = file
        .read_u16::<LittleEndian>()
        .chain_err(|| "Problem reading version")?;
    let header_size = file
        .read_u32::<LittleEndian>()
        .chain_err(|| "Problem reading header size")?;
    let bits_per_basecall = file
        .read_u8()
        .chain_err(|| "Problem reading bits per basecall")?;
    if bits_per_basecall != 2 {
        bail!(
            "Can only work with bits_per_basecall = 2, but was {}",
            bits_per_basecall
        );
    }
    let bits_per_qscore = file
        .read_u8()
        .chain_err(|| "Problem reading bits per qscore")?;
    if bits_per_qscore != 2 {
        bail!(
            "Can only work with bits_per_qscore = 2, but was {}",
            bits_per_qscore
        );
    }

    let num_bins = file
        .read_u32::<LittleEndian>()
        .chain_err(|| "Problem reading number of bins.")?;
    let mut q_val_bins = Vec::new();
    for _i in 0..num_bins {
        let from = file
            .read_u32::<LittleEndian>()
            .chain_err(|| "Problem reading from.")?;
        let to = file
            .read_u32::<LittleEndian>()
            .chain_err(|| "Problem reading to.")?;
        q_val_bins.push(QValBinInfo {
            _from: from,
            _to: to,
        });
    }

    let num_offset_infos = file
        .read_u32::<LittleEndian>()
        .chain_err(|| "Problem reading num offset infos.")?;
    let mut offset_infos = Vec::new();
    for _i in 0..num_offset_infos {
        let tile_no = file
            .read_u32::<LittleEndian>()
            .chain_err(|| "Problem reading tile number")?;
        let num_clusters = file
            .read_u32::<LittleEndian>()
            .chain_err(|| "Problem reading number of clusters")?;
        let uncompressed_size = file
            .read_u32::<LittleEndian>()
            .chain_err(|| "Problem reading uncompressed block size")?;
        let compressed_size = file
            .read_u32::<LittleEndian>()
            .chain_err(|| "Problem reading compressed block size")?;
        let non_pf_flag = file.read_u8().chain_err(|| "Problem reading non pf flag")?;
        let non_pf_flag = non_pf_flag != 0;
        offset_infos.push(OffsetInfo {
            _tile_no: tile_no,
            num_clusters,
            _uncompressed_size: uncompressed_size,
            compressed_size,
            _non_pf_flag: non_pf_flag,
        });
    }

    Ok(CbclHeader {
        _version: version,
        header_size,
        _bits_per_basecall: bits_per_basecall,
        _bits_per_qscore: bits_per_qscore,
        _q_val_bins: q_val_bins,
        offset_infos,
    })
}

/// Read `settings.ingest.sample_reads_per_tile` number of reads from the given tile.
fn load_from_cbcl(
    _logger: &slog::Logger,
    path: &str,
    header: &CbclHeader,
    tile_no: u32,
    settings: &Settings,
) -> Result<Vec<char>> {
    let table = vec!['A', 'C', 'G', 'T'];
    let tile_no = tile_no as usize;
    let mut result = Vec::new();

    let mut file = File::open(&path).chain_err(|| format!("Problem opening CBCL file {}", path))?;
    let mut offset = header.header_size as usize;
    for i in 0..tile_no {
        offset += header.offset_infos[i].compressed_size as usize;
    }
    file.seek(SeekFrom::Start(offset as u64))
        .chain_err(|| "Could not jump in CBCL file")?;
    let mut gz_decoder = GzDecoder::new(file);
    let num_bytes = cmp::min(
        header.offset_infos[tile_no].num_clusters,
        settings.ingest.sample_reads_per_tile as u32,
    );
    for j in 0..((num_bytes + 1) / 2) {
        let b: u8 = gz_decoder
            .read_u8()
            .chain_err(|| "Problem reading data byte")?;
        result.push(table[(b & 3) as usize]);
        if num_bytes > j * 2 {
            result.push(table[((b >> 4) & 3) as usize]);
        }
    }

    Ok(result)
}

/// Analyze a single stack.
pub fn analyze_stacks(
    logger: &slog::Logger,
    lane_stacks: &Vec<Vec<TileBclStack>>,
    stack_no: usize,
    index_no: i32,
    settings: &Settings,
) -> Result<Vec<IndexCounts>> {
    // Regular expression for detecting CBL file
    let cbcl_re =
        Regex::new(r"^(.*\.cbcl)!(\d+)$").chain_err(|| "Problem constructing Regex object")?;

    lane_stacks
        .par_iter()
        .map(|ref stacks_for_lane| {
            let stack = &stacks_for_lane[stack_no];
            // Read in the bases from the bcl files.
            let bases = stack
                .paths
                .par_iter()
                .map(|ref path| {
                    let chars = if cbcl_re.is_match(&path) {
                        // Because we know that the RE matches, the following two unwraps cannot
                        // fail.
                        let captures = cbcl_re.captures(&path).unwrap();
                        let cbcl_header = load_cbcl_header(logger, &captures[1])
                            .chain_err(|| "Loading CBL header failed")?;
                        load_from_cbcl(
                            logger,
                            &captures[1],
                            &cbcl_header,
                            captures[2].parse::<u32>().unwrap(),
                            settings,
                        )
                        .chain_err(|| "Problem loading CBCL tile")?
                    } else {
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

                        chars
                    };

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

/// Build tile-wise lists of files describing the BCL files for the given tile and each cycle.
///
/// Note that for CBCL files, we generate file names such as `"path/to/file.cbcl!${tile_no}"`.
pub fn find_file_stacks(
    _logger: &slog::Logger,
    folder_layout: FolderLayout,
    desc: &ReadDescription,
    path: &Path,
    start_cycle: i32,
) -> Result<Vec<Vec<TileBclStack>>> {
    // TODO: currently we cannot sample more than one stack...
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
        FolderLayout::NovaSeq => {
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
                let path = Path::new(lane_path)
                    .join("C1.1")
                    .join(format!("L???_?.cbcl"));
                for prototype in glob(path.to_str().unwrap()).unwrap() {
                    let path = prototype.unwrap();
                    let file_name = path
                        .file_name()
                        .unwrap()
                        .to_os_string()
                        .into_string()
                        .expect("Problem decoding string from OS")
                        + "!0";
                    let mut paths: Vec<String> = Vec::new();
                    for cycle in start_cycle..(start_cycle + desc.num_cycles) {
                        let path = Path::new(lane_path)
                            .join(format!("C{}.1", cycle))
                            .join(&file_name);
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
