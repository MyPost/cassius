(ns cassius.test-types-thrift-schema
  (:use midje.sweet)
  (:require [cassius.types.thrift :refer :all])
  (:import [org.apache.cassandra.thrift CfDef KsDef ColumnDef]))

(fact "KsDef has the following properties"
  (get-thrift KsDef [:id-lookup keys sort])
  => ["cf_defs"
      "durable_writes"
      "name"
      "replication_factor"
      "strategy_class"
      "strategy_options"]

  (get-thrift KsDef [:type-lookup])
  => {"durable_writes" Boolean/TYPE
      "name" java.lang.String
      "replication_factor" Integer/TYPE
      "cf_defs" java.util.List
      "strategy_class" java.lang.String
      "strategy_options" java.util.Map})

(fact "ColumnDef has the following keys"
  (get-thrift ColumnDef [:type-lookup])
  => {"index_name" java.lang.String
      "index_options" java.util.Map
      "index_type" org.apache.cassandra.thrift.IndexType
      "name" java.nio.ByteBuffer
      "validation_class" java.lang.String})

(fact "CfDef has the following Keys"
  (get-thrift CfDef [:id-lookup keys sort])
  => ["bloom_filter_fp_chance"
      "caching"
      "column_metadata"
      "column_type"
      "comment"
      "compaction_strategy"
      "compaction_strategy_options"
      "comparator_type"
      "compression_options"
      "dclocal_read_repair_chance"
      "default_time_to_live"
      "default_validation_class"
      "gc_grace_seconds"
      "id"
      "index_interval"
      "key_alias"
      "key_cache_save_period_in_seconds"
      "key_cache_size"
      "key_validation_class"
      "keyspace"
      "max_compaction_threshold"
      "memtable_flush_after_mins"
      "memtable_flush_period_in_ms"
      "memtable_operations_in_millions"
      "memtable_throughput_in_mb"
      "merge_shards_chance"
      "min_compaction_threshold"
      "name"
      "populate_io_cache_on_flush"
      "read_repair_chance"
      "replicate_on_write"
      "row_cache_keys_to_save"
      "row_cache_provider"
      "row_cache_save_period_in_seconds"
      "row_cache_size"
      "speculative_retry"
      "subcomparator_type"
      "triggers"])
