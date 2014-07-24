(ns cassius.schema.definitions
  (:import [org.apache.cassandra.thrift IndexType]))

(def cassandra-column-types
  #{"Super" "Standard"})

(def cassandra-key-types
  {:ascii             "AsciiType"
   :bytes             "BytesType"
   :composite         "CompositeType"
   :counter           "CounterColumnType"
   :double            "DoubleType"
   :dynamic-composite "DynamicCompositeTYpe"
   :integer           "IntegerType"
   :lexical-uuid      "LeixcalUUIDType"
   :local-partitioner "LocalByPartionerType"
   :long              "LongType"
   :time-uuid         "TimeUUIDType"
   :utf-8             "UTF8Type"
   :uuid              "UUIDType"})

(def cassandra-data-types
  (let [vs (vals cassandra-key-types)
        ks (keys cassandra-key-types)]
    (merge (zipmap vs ks)
           (zipmap (map #(str "org.apache.cassandra.db.marshal." %) vs)
                   ks))))

(def columndef-defaults
  {:type :column-definition
   :validation_class "UTF8Type"
   :validation_type IndexType/KEYS})

(def columnfamilydef-defaults
  {:type :column-family-definition
   :max_compaction_threshold 32
   :key_validation_class "org.apache.cassandra.db.marshal.BytesType"
   :comment ""
   :compression_options {"sstable_compression" "org.apache.cassandra.io.compress.SnappyCompressor"}
   :read_repair_chance 0.1
   :caching "KEYS_ONLY"
   :compaction_strategy "org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy"
   :comparator_type "org.apache.cassandra.db.marshal.BytesType"
   :subcomparator_type "org.apache.cassandra.db.marshal.BytesType"
   :gc_grace_seconds 864000
   :replicate_on_write true
   :compaction_strategy_options {}
   :default_validation_class "org.apache.cassandra.db.marshal.BytesType"
   :min_compaction_threshold 4})

(def keyspacedef-defaults
  {:type :keyspace-definition
   :strategy_class "org.apache.cassandra.locator.SimpleStrategy"
   :strategy_options {"replication_factor" "3"}})
