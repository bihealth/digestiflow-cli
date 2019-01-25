[![Bioconda](https://img.shields.io/conda/dn/bioconda/digestiflow-cli.svg?label=Bioconda)](https://bioconda.github.io/recipes/digestiflow-cli/README.html)
[![Build Status](https://travis-ci.org/bihealth/digestiflow-cli.svg?branch=master)](https://travis-ci.org/bihealth/digestiflow-cli)

# Digestfilow CLI Client

The aim of this project is to provide a command line client for controlling Digestiflow via its REST API.
At the moment, the client only allows to create and update flow cell objects in Digestiflow Web from reading the directories created by Illumina sequencers.

## Usage

This assumes that you already have installed Digestiflow Web.
There, you must have created a project with the sequencing machines used in all of your flow cells.

### Configuration

First, create a `~/.digestiflowrc.toml` file with the global configuration and the content below.
Most importantly, configure the web API `url` and `token`.
The token can be created after logging into Digestiflow Web through the user icon at the top right and the menu item "API Tokens".

```toml
# Use 4 threads by by default.
threads = 4

[web]
# URL to your Digestiflow instance. "$url/api" must be the API entry URL.
url = "https://flowcells.example.org"
# The secret token to use for the the REST API, as created through the Web UI.
token = "secretsecretsecretsecretsecretsecretsecretsecretsecretsecretsecr"

[ingest]
# Create adapter histograms by default.
analyze_adapters = true
```

### Calling

To import the flow cells below `PATH` and `PATH2` into the project with UUID `UUID`, use the following command.

```bash
digestiflow-cli ingest --project-uuid --project UUID PATH PATH2
```

The command line help is available through

```bash
digestiflow-cli --help
digestiflow-cli ingest --help
```

## `digestiflow-ci ingest`

This command reads is given the UUID of a project in Digestiflow Web and one or more paths to flow cell directories.
For each of the directories, the tool will do the following:

1. Read in the meta information in the `RunParameters.xml` and `RunInfo.xml` files.
   This includes:
    - Information such as the read name, the sequencer vendor ID, the run number, and flow cell vendor ID.
    - The sequence of reads **planned** created, i.e., the template (read) and barcode (index) reads.
    - The sequencing process (**current** reads).
2. Query the Digestiflow API for a flow cell with the same (i) sequencing machine, (ii) run number, and (iii) flow cell vendor ID.
   a. If such a flow cell exists and the flow cell has state "initial" or "in progress" then the flow cell's information will be updated using the values from teh meta information files.
   b. If such a flow cell exists and the state is different then no update will be performed.
   b. If such a flow cell does not exist then a new one will be added.
3. If `--analyze-adapters` is given, query the Digestiflow API for index reads histograms for the retrieved or added flow cell from step 2.
   a. If there is histogram information for all expected index reads then no update will be performed.
      That is, if the flow cell has 8 lanes and the run creates 2 index reads then information for 16 index reads will be expected in total.
      Effectively, if the flow cell folder has been analyzed after all indices have been sequenced completely,  it is not reanalyzed.
   b. If the number of histograms is different, the index reads are read for one tile and a histogram is computed.
      This histogram shows how often a given index was seen.
      This information is used by Digestiflow Web for comparing and sanity checking the adapters expected from the sample sheet and the actually observed indices in the BCL file.
      Indices visible in 0.1% of all index reads or less will be ignored.
      After computing the index histograms, this information is posted to the Digestiflow API which makes it available to Digestiflow Web users.

The behaviour can be changed by using the following parameters:

- `--no-register` -- prevent CLI from registering new flow cells through the API in step 2.
- `--no-update` -- prevent CLI from updating existing flow cells through the API in step 2.
- `--update-if-state-final` -- update the flow cell meta information even if its state is not "initial" or "in progress".
- `--force-analyze-adapters` -- force the analysis of index reads even if full information already exists in step 3.
- `--sample-reads-per-tile` -- limit the number of reads read from the sample tile.

The remaining arguments are self-explanatory and explain logging verbosity, and thread to use for the analysis.
