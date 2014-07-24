(ns cassius.net.command.delete
  (:require [cassius.common :refer [vectorize]]
            [cassius.protocols :refer [to-bbuff]]
            [cassius.types.byte-buffer]
            [cassius.net.connection :refer [client]]
            [cassius.net.command
             [keyspace :as ksp]
             [mutate :refer [batch-mutate]]
              [macros :refer [raise-on-invalid-request current-time-millis]]])
  (:import [org.apache.cassandra.thrift SlicePredicate ColumnPath Mutation Deletion]))

(defn delete-path [conn ks cp row]
  (let [t (current-time-millis conn)]
    (ksp/set-keyspace conn ks)
    (raise-on-invalid-request
     [conn ks nil :remove :column-family-not-found]
     (.remove (client conn) (to-bbuff row) cp t (:consistency conn)))))

(defn delete-row [conn ks cf row]
  (delete-path conn ks (ColumnPath. cf) row))

(defn delete-column-mutation [names t]
  (let [spred (doto (SlicePredicate.)
                (.setColumn_names (mapv to-bbuff (vectorize names))))
        del   (doto (Deletion.)
                (.setPredicate spred)
                (.setTimestamp t))
        mut   (doto (Mutation.)
                (.setDeletion del))]
    mut))

(defn delete-sub-column-mutation [super names t]
  (let [spred (doto (SlicePredicate.)
                (.setColumn_names (mapv to-bbuff (vectorize names))))
        del   (doto (Deletion.)
                (.setPredicate spred)
                (.setTimestamp t)
                (.setSuper_column (to-bbuff super)))
        mut   (doto (Mutation.)
                (.setDeletion del))]
    mut))

(defn delete-normal-columns
  [conn ks cf id cols]
  (let [batch {(to-bbuff id)
                {cf [(delete-column-mutation cols (current-time-millis conn))]}}]
    (batch-mutate conn ks batch)))

(defn delete-sub-columns
  [conn ks cf row col subcol]
  (let [cp (doto (ColumnPath. cf)
              (.setSuper_column (to-bbuff col))
              (.setColumn (to-bbuff subcol)))]
    (delete-path conn ks cp row)))
