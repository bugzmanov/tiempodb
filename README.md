Goals:
* InfluxDB compatible line protocol
* InfluxDB compatible query language (to be supported by grafana)

Plan: 
[ ] WAL
    [ ] Ingestion
    [ ] Recovery
[ ] Multi-threading for storage
[ ] Storage writer
[ ] Storage reader
[ ] Compaction
[ ] Query support
[ ] Index

Ideas:
* SIMD instructions to do aggregations 
    * 
* NO mmap to support more stable latency and performance
    * http://cidrdb.org/cidr2022/papers/p13-crotty.pdf
    * Use tiny_lfu (at least for index blocks):
        * https://arxiv.org/abs/1512.00727 
* Fuzzy testing of storage engine:
    * https://fuzzcheck.neocities.org/
    * https://github.com/loiclec/fuzzcheck-rs
* Better CPU utilization in ingestion pipeline: network card -> CPUs
* Data sketches (maybe)
* Use zerocopy for parsing:
    * https://docs.rs/zerocopy/latest/zerocopy/

* Try MemtableTrie ? 
    * https://github.com/blambov/cassandra/blob/CASSANDRA-17240/src/java/org/apache/cassandra/db/tries/MemtableTrie.md
