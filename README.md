Goals:
* InfluxDB compatible line protocol
* InfluxDB compatible query language (to be supported by grafana)

Plan: 
- [ ] WAL
   - [ ] Ingestion
   - [ ] Recovery
- [ ] Multi-threading for storage
- [ ] Storage writer
- [ ] Storage reader
- [ ] Compaction
- [ ] Query support
- [ ] Index

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

* Set up benches:
    * https://github.com/BurntSushi/cargo-benchcmp
    * https://docs.rs/dhat/latest/dhat/

* Check alternatives for bytes arrays:
    *  https://docs.rs/bstr/0.2.13/bstr/index.html
    *  https://docs.rs/bytes/1.1.0/bytes/

* Check low level networking (maybe):
    * https://github.com/rust-lang/socket2


Refs:
* replication:
    * https://blog.acolyer.org/2019/03/15/exploiting-commutativity-for-practical-fast-replication/
* tsdbs: 
    * https://fabxc.org/tsdb/
    * https://nakabonne.dev/posts/write-tsdb-from-scratch/
    * https://static-curis.ku.dk/portal/files/248553347/ByteSeries_v_final.pdf
    * https://www.usenix.org/system/files/conference/fast16/fast16-papers-andersen.pdf
    * https://www.vldb.org/pvldb/vol8/p1816-teller.pdf
