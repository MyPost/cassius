(ns cassius.test-types-thrift-data
  (:use midje.sweet)
  (:require [cassius.types.thrift :refer :all])
  (:import [org.apache.cassandra.thrift
            Column SuperColumn ColumnOrSuperColumn
            CounterColumn CounterSuperColumn
            Mutation Deletion
            KeySlice KeyRange
            SlicePredicate SliceRange]))

(fact "Column has the following properties"
  (get-thrift Column [:type-lookup])
  => {"name" java.nio.ByteBuffer,
      "ttl" Integer/TYPE,
      "value" java.nio.ByteBuffer,
      "timestamp" Long/TYPE})

(fact "SuperColumn has the following properties"
  (get-thrift SuperColumn [:type-lookup])
  => {"name" java.nio.ByteBuffer
      "columns" java.util.List})

(fact "ColumnOrSuperColumn has the following properties"
  (get-thrift ColumnOrSuperColumn [:type-lookup])
  => {"counter_super_column" CounterSuperColumn,
      "super_column" SuperColumn,
      "counter_column" CounterColumn,
      "column" Column})

(fact "CounterColumn has the following properties"
  (get-thrift CounterColumn [:type-lookup])
  => {"name" java.nio.ByteBuffer
      "value" Long/TYPE})

(fact "CounterSuperColumn has the following properties"
  (get-thrift CounterSuperColumn [:type-lookup])
  => {"name" java.nio.ByteBuffer
      "columns" java.util.List})

(fact "Mutation has the following properties"
  (get-thrift Mutation [:type-lookup])
  => {"column_or_supercolumn" ColumnOrSuperColumn
      "deletion" Deletion})

(fact "Deletion has the following properties"
  (get-thrift Deletion [:type-lookup])
  => {"super_column" java.nio.ByteBuffer,
      "predicate" SlicePredicate,
      "timestamp" Long/TYPE})

(fact "KeySlice has the following properties"
  (get-thrift KeySlice [:type-lookup])
  => {"key" java.nio.ByteBuffer, "columns" java.util.List})

(fact "KeyRange has the following properties"
  (get-thrift KeyRange [:type-lookup])
  => {"count" Integer/TYPE,
      "start_token" java.lang.String,
      "row_filter" java.util.List,
      "end_token" java.lang.String,
      "start_key" java.nio.ByteBuffer,
      "end_key" java.nio.ByteBuffer})

(fact "SliceRange has the following properties"
  (get-thrift SliceRange [:type-lookup])
  => {"count" Integer/TYPE
      "start" java.nio.ByteBuffer,
      "finish" java.nio.ByteBuffer,
      "reversed" Boolean/TYPE})

(fact "SlicePredicate has the following properties"
  (get-thrift SlicePredicate [:type-lookup])
  => {"column_names" java.util.List,
      "slice_range" SliceRange})
