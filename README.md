# SyslogPreprocessor
Receives, stores, compiles and resends back this data.

## Installation
Before using:
 - Install dependencies with `go get` command.
 - Compile using `go build`

## Usage
Run the preprocessor generated. By default, the `settings.json` file is used. Specify the file to use with the `-config settings_file.json` option.

## Configuration
The configuration has to be done in the settings.json file (can be renamed to maintain different versions if needed). Details of the configuration are presented in the following:
 - `AppLogger` : the logging configuration of the app itself (not the preprocessed output). Useful for monitoring activity. Logging can be remote with `RemoteLogging` specified. The `APPNAME` entry provides a static arbitrary hostname for Syslog format.
 - `Emitter` : Specify the target for preprocessed Syslog events and the output format (`RFC3164`/`RFC5424`), output, and adjust queuesize if needed (high traffic sources for example).
 - `DbWorker` : `REGULARDUMPINGSEC` can be specified to send events received at regular interval instead of on trigger). If you needed to store the database in a specific location, or to adjust delays over SQLite worker settings. Modify these if you know what you are doing.
 - `Receiver` : The Syslog listener configuration for input events.
 - `Parser` : Settings for the parsing and prerpocesing of the events with the `Mapping`:
   - `MSGMERGE` : The common part of the logs to merge together
   - `MsgType` : Start (the event to start aggregation) / Stop (the event to delete previsously learned information) / Trigger (the event to dump all entries)
   - `MsgBlacklist` : log lines to ignore
   - `MsgData` : fields to extract from log
   - `MsgMetadata` : force original hostname to be added into logs
   - `MSGKEY` : The unique key to retrieve all logs
   - `MSGDATAENRICH` : List of lieds to be added to the log (complete override).
