# Digestfilow CLI Client

The aim of this project is to provide a command line client for controlling Digestiflow via its REST API.
At the moment, the client only allows to create and update flow cell objects in Digestiflow Web from reading the directories created by Illumina sequencers.

## Usage

This assumes that you already have installed Digestiflow Web.
There, you must have created a project with the sequencing machines used in all of your flow cells.

First, create a `~/.digestiflowrc.toml` file with the global configuration.
Most importantly, configure the we API `url` and `token`.

```bash
$ cat >~/.digestiflowrc.toml <<EOF
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
"EOF"
```

To import the flow cells below `PATH` and `PATH2` into the project with UUID `UUID`, use the following command.

```bash
$ digestiflow-cli ingest --project UUID PATH PATH2
```
