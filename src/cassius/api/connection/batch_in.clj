(ns cassius.api.connection.batch-in
  (:require [cassius.net.command.keyspace :as ksp]
            [cassius.schema.outline :as sch]
            [cassius.data.outline :as c]
            [ribol.core :refer [raise manage on]])
  (:import [org.apache.cassandra.thrift ConsistencyLevel
            Column ColumnOrSuperColumn]))

;;(print (seq (.getBytes "name")))
