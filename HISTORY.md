# Digestiflow CLI Client Changelog

## v0.5.4

- Fixed problem with erroneously flagging successful runs as failed.

## v0.5.3

- Resolved issue with parsing NovaSeq RTA version (v3...)

## v0.5.2

- Supporting data generator by relaxing BCL file name pattern.
- Fixing update logic to ensure things are marked as complete properly.

## v0.5.1

- supporting RTA v3+ by removing some restrictions

## v0.5.0

- adding support for CBCL files (e.g., from NovaSeq)

## v0.4.1

- Allowing parsing of NovaSeq meta data.
- Note that parsing NovaSeq BCL files does not work yet.

## v0.4.0

- Removing number of REST client creations (to work around some DNS limitations).
- Setting default number of reads to analyze per tile to 1M.
- Adding support for uncompressed BCL files.

## v0.3.0

- Updating `README.md` with most current information.
- Adjusting update behaviour, changes command line.
- Updated formatting to new `rust-fmt` version.

## v0.2.0

- Registring indices seen in >0.1% of all reads by default now.
- Allowing to configure minimal fraction of reads to show an index for index to turn up in histogram.

## v0.1.1

- Adding lincense.

## v0.1.0

- First release.
  Everything is new.
