Syncer
======

Abstract
--------
Syncer assumes responsibility of loading weather forecast data ("model run") into a WDB database, and updates the WDB2TS server connected to it if WDB is successfully updated. Syncer gets its input data from the Modelstatus REST API service.

Configuration
-------------
Configuration is done through a configuration file, normally found in `/etc/syncer.ini`. An example file is provided with inline comments documenting the features.

Command-line interface
----------------------
A command-line client to Syncer is provided, `syncerctl`. It can query status from Syncer, as well as send commands. It can also be used for producing machine-readable output for use in monitoring systems.

Program flow
------------
The program is started through an init script. It can also be run standalone. It does not fork. Syncer reads configuration from a file and determines which models to manage. It starts a local server which `syncerctl` can use to query status and send commands.

The main loop is as follows:

1. Iteration through all models. Check if models need new data sets (model runs), and fetch them from the Modelstatus REST API service.
2. Iteration through all models. Check if model run information from the Modelstatus service matches the model run loaded into WDB. WDB is not queried during this process; Syncer uses its own internal information to determine if the model runs matches. If the model runs do not match, load new data into WDB.
3. Iteration through all models. Check if model run information in WDB matches the information in WDB2TS. If WDB2TS has not been informed of a specific model run, an update is performed.
4. Sleep a configurable time while listening for `syncerctl` commands and ZeroMQ publish events from the Modelstatus service. Mark a model due for update if a new model run is reported by Modelstatus.

Mechanics of the Modelstatus query
----------------------------------
When a model is due for update, Syncer will perform the following steps. Any errors reported from the Modelstatus service will abort the process, and Syncer will retry the update at the next main loop iteration.

1. Request the latest model run for this specific model from the Modelstatus REST API service,
2. Iterate through the file list in the model run, and determine if it contains all the files needed, and
3. Schedule the model for load into WDB.

Mechanics of a WDB load
-----------------------
When a model run is scheduled to be loaded into WDB, Syncer will perform the following steps. Any errors will abort the process, and Syncer will retry loading at the next main loop iteration.

1. SSH into the WDB server.
2. Execute load program. The data set version (WDB option `--dataversion`) will increase every time a specific `data provider` and `reference time` combination is loaded.
3. Reads exit code to determine load status. A status code of non-zero means the load was unsuccessful, except duplicate key errors (codes 13 and 100).
4. Run the commands `wci.cacheQuery(...)` and `ANALYZE`.

Mechanics of a WDB2TS update
----------------------------
When WDB2TS needs updated information about a model run, Syncer will perform the following steps. Any errors will abort the process, and Syncer will retry the update at the next main loop iteration.

1. A WDB2TS status report is requested for each WDB2TS service endpoint.
2. Data providers for each service are matched against Syncer configuration.
3. Iterate through applicable services, and send update request for each `service` and `data provider` combination.

ZeroMQ subscription
-------------------
Syncer subscribes to events from the Modelstatus service, and will trigger a model run update when it receives an event; see Mechanics of the Modelstatus query. The main loop will continue as usual after the update.
