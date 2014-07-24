(ns cassius.test-schema-replica
  (:use midje.sweet)
  (:require
   [cassius.protocols :refer :all]
   [cassius.schema.replica :refer :all])
  (:import [org.apache.cassandra.thrift
            CfDef KsDef ColumnDef IndexType]))


(fact "columndef->map and to-map are the same"
  (columndef->map
   (map->columndef {:name "Hello" :index_type :custom :index_name "hello_idx"}))
  => {:name "Hello" :validation_class "UTF8Type" :index_type :custom :index_name "hello_idx"}

  (to-map
   (from-map {:name "Hello"} ColumnDef))
  => {:name "Hello" :validation_class "UTF8Type"})


(fact "columnfamilydef->map"
  (columnfamilydef->map
   (map->columnfamilydef {:name "visitor"
                          :keyspace "zoo"
                          :column_metadata [{:name "name"}
                                            {:name "age"}]}))

  => {:keyspace "zoo"
      :name "visitor"
      :column_metadata [{:name "name" :validation_class "UTF8Type"}
                        {:name "age" :validation_class "UTF8Type"}]})

(fact "keyspacedef->map"
  (keyspacedef->map
   (map->keyspacedef {:name "zoo"
                      :strategy_class "org.apache.cassandra.locator.SimpleStrategy"
                      :cf_defs [{:name "visitor"
                                 :column_metadata [{:name "name" :index_type :custom :index_name "hello_idx"}
                                                   {:name "age"}]}
                                {:name "animals"}]}))
  => {:name "zoo"
      :strategy_class "org.apache.cassandra.locator.SimpleStrategy"
      :strategy_options {:replication_factor "3"}
      :cf_defs [{:keyspace "zoo"
                 :name "visitor"
                 :column_metadata [{:name "name" :validation_class "UTF8Type"
                                    :index_type :custom :index_name "hello_idx"}
                                   {:name "age" :validation_class "UTF8Type"}]}
                {:keyspace "zoo"
                 :name "animals"
                 :column_metadata []}]} )
