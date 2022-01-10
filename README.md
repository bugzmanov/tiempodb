Goals:
* InfluxDB compatible line protocol
* InfluxDB compatible query language (to be supported by grafana)


Ideas:
* SIMD instructions to do aggregations 
    * 
* NO mmap to support more stable latency and performance
    * http://cidrdb.org/cidr2022/papers/p13-crotty.pdf
* Fuzzy testing of storage engine:
    * https://fuzzcheck.neocities.org/
    * https://github.com/loiclec/fuzzcheck-rs
* Better CPU utilization in ingestion pipeline: network card -> CPUs
* Data sketches (maybe)


