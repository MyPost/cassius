(ns cassius.net.command.mutate
  (:require [cassius.net.connection :refer [client]]
              [cassius.net.command
               [keyspace :as ksp]
               [macros :refer [raise-on-data-error]]])
    (:import [org.apache.cassandra.thrift ConsistencyLevel]))

(defn normal-batch-mutate
  [conn ks mutation]
  (ksp/set-keyspace conn ks)
  (raise-on-data-error
   [conn ks :mutate {}]
   (.batch_mutate (client conn) mutation
                  (or (:consistency conn) ConsistencyLevel/ONE))))

(defn atomic-batch-mutate
  [conn ks mutation]
  (ksp/set-keyspace conn ks)
  (raise-on-data-error
   [conn ks :mutate {}]
   (.atomic_batch_mutate (client conn) mutation
                         (or (:consistency conn) ConsistencyLevel/ONE))))

(defn batch-mutate
  [conn ks mutation]
  (if (:atomic conn)
    (atomic-batch-mutate conn ks mutation)
    (normal-batch-mutate conn ks mutation)))
