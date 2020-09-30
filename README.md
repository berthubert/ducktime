# ducktime
a toy duckdb based timeseries database

Currently the 'ducktime' binary does nothing but fill a table with somewhat
plausible values.

Goal of this program is to figure out how DuckDB behaves under various
circumstances, and how to best feed it data. 

To compile, edit Makefile with the location of your DuckDB tree, and run
'make'.

To test, run: `./ducktime duckfilename 10000000`, and it will stream 10
million rows into `duckfilename`. The data simulates some typical timeseries
data.

Status & Features
-----------------
This is a toy. It might grow into something or not. Current features:

 * Schema adjusted automatically if you add new fields
 * Native storage of integers, doubles, strings
 * Uses the DuckDB "Bulk" API
 
I'm still exploring the best use of transactions and the Appender API to get the smoothest experience.
