(ns cassius.net.command.column-family
  (:require [ribol.core :refer [raise]]
            [cassius.common :refer [hash-map?]]
            [cassius.schema.outline :as ol]
            [cassius.net.connection :refer [client]]
            [cassius.net.command.keyspace :as ksp]
            [cassius.net.command.macros :refer [raise-on-invalid-request]])
  (:import [org.apache.cassandra.thrift CfDef]))

(defn prepare-columnfamily [ks cf]
  (cond (string? cf)
        (ol/string->columnfamilydef ks cf)

        (hash-map? cf)
        (ol/map->columnfamilydef ks cf)

        (instance? CfDef cf) cf

        :else (raise [:invalid-columnfamily {:definition cf}])))

(defn add-column-family
  [conn ks cf]
  (ksp/set-keyspace conn ks)
  (raise-on-invalid-request [conn ks cf :abortable :add :column-family-exists]
    (.system_add_column_family (client conn) (prepare-columnfamily ks cf)))
    [ks cf])

(defn drop-column-family
  [conn ks cf]
  (ksp/set-keyspace conn ks)
  (raise-on-invalid-request [conn ks cf :abortable :drop :column-family-not-found]
    (.system_drop_column_family (client conn) cf)
    [ks cf]))
