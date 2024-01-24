# Data Engineering Artists Data Pipeline

## Requirements
- Have Docker


## Description
This code gives you all the tools to run the specific DAG called `artist_data_to_redshift`.

What this DAG does is:

1. Fetch artists name from MusicBrainz API.
2. Pull some metrics for each artists using Spotify API. 
3. Ingest that data into our Redshift.


## Usage
It's easy, just do:

1. `make build`
2. `make run`
3. Enter `localhost:8080` in whatever browser you want.
4. Once inside, activate the DAG, wait for it to turn dark green and voila! The pipeline ran.
5. To kill everything, you can `make stop`


## HELP!
Run `make help`.
