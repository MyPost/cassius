(ns cassius.data.replica
  (:require [cassius.common :refer [vectorize]]
            [cassius.protocols :refer :all]
            [cassius.types.thrift :as thr]
            [cassius.types.byte-buffer :as bb])
  (:import [org.apache.cassandra.thrift
            Column ColumnParent ColumnPath ConsistencyLevel
            ColumnOrSuperColumn Deletion
            InvalidRequestException KeyRange Mutation
            SliceRange SlicePredicate SuperColumn]))

(defn map->column [m]
  (thr/map->thrift m Column))

(defn map->supercolumn [m]
  (thr/map->thrift m SuperColumn
                   {:columns (fn [cols] (mapv map->column cols))}))

(defn map->columnmutation [m]
  (let [mut (Mutation.)
        csc (ColumnOrSuperColumn.)
        c (map->column m)]
    (.setColumn csc c)
    (.setColumn_or_supercolumn mut csc)
    mut))
