# BigTable Query Example

## Purpose

This repository provides an example of reading from BigTable.

It uses the BigTable client core java library:

`"com.google.cloud.bigtable" % "bigtable-client-core" % "1.5.0"`


## Instructions

Write test queries in a tab-separated text file.

Expected fields are `startKey`, `endKey`, `testName`

See [queries.tsv](queries.tsv) for an example.

Build the assembly jar by running `sbt assembly`


## Usage

Upload the assembly jar to your GCE VM and run

`java -jar bigtable-query-assembly-0.1.0.jar project instance table columnfamily mycolumn queries.tsv 2>/dev/null`

`stdout` is sent to `/dev/null` to avoid printing the contents of each row.


## Example Output

```
$ java -jar bigtable-query-assembly-0.1.0.jar project instance table columnfamily mycolumn queries.tsv 2>/dev/null
Warming up BigTable client
warmup scan: Read 10000 rows in 1887 ms
warmup scan: Read 10000 rows in 507 ms
warmup scan: Read 10000 rows in 458 ms
warmup scan: Read 10000 rows in 404 ms
warmup scan: Read 10000 rows in 337 ms
warmup scan: Read 10000 rows in 320 ms
warmup scan: Read 10000 rows in 300 ms
warmup scan: Read 10000 rows in 168 ms
warmup scan: Read 10000 rows in 110 ms
warmup scan: Read 10000 rows in 103 ms
warmup scan: Read 10000 rows in 123 ms
warmup scan: Read 10000 rows in 119 ms
warmup scan: Read 10000 rows in 127 ms
warmup scan: Read 10000 rows in 120 ms
warmup scan: Read 10000 rows in 100 ms
warmup scan: Read 10000 rows in 137 ms
warmup scan: Read 10000 rows in 99 ms
warmup scan: Read 10000 rows in 129 ms
warmup scan: Read 10000 rows in 150 ms
warmup scan: Read 10000 rows in 97 ms
day: Read 1 rows in 21 ms from [s########3##20170101, s########3##20170102)
week: Read 7 rows in 10 ms from [s########3##20170101, s########3##20170108)
month: Read 24 rows in 9 ms from [s########3##20170101, s########3##20170201)
year: Read 290 rows in 15 ms from [s########3##20170101, s########3##20180101)
```