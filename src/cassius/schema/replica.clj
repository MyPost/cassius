(ns cassius.schema.replica
  (:import [org.apache.cassandra.thrift CfDef KsDef ColumnDef IndexType])
  (:require [cassius.protocols :refer [to-map from-map Mappable]]
            [cassius.common :refer [assoc-nil namify-keys]]
            [cassius.types.thrift :as thr]
            [cassius.schema.definitions :as d]))

(defn index-type->keyword [idx-type]
  (condp = idx-type
    IndexType/KEYS :keys
    IndexType/CUSTOM :custom
    IndexType/COMPOSITES :composites
    nil))

(defn columndef->map [^ColumnDef cdef]
  (thr/thrift->map cdef {:name String
                         :index_type index-type->keyword}))

(defn columnfamilydef->map [^CfDef cfdef]
  (thr/thrift->map cfdef {:column_metadata #(mapv columndef->map %)
                          :key_alias String}))

(defn keyspacedef->map [^KsDef ksdef]
  (thr/thrift->map ksdef {:cf_defs #(mapv columnfamilydef->map %)
                          :strategy_options #(into {} %)}))

(extend-protocol Mappable
  ColumnDef
  (to-map [cdef]
    (columndef->map cdef))

  CfDef
  (to-map [cfdef]
    (columnfamilydef->map cfdef))

  (to-map [ksdef]
    (keyspacedef->map ksdef)))

(defn keyword->index-type [kw]
  (condp = kw
    :keys IndexType/KEYS
    :custom IndexType/CUSTOM
    :composites IndexType/COMPOSITES
    IndexType/KEYS))

(defn map->columndef [m]
  {:pre  [(contains? m :name)
          (or (nil? (:validation_class m))
              (contains? d/cassandra-data-types (:validation_class m)))]}
  (let [{:keys [index_name index_type validation_class]} m
        m (assoc-nil m :validation_class "UTF8Type")
        m (if (and index_name index_type)
            (assoc  m :index_type (keyword->index-type index_type))
            (dissoc m :index_type :index_type))]
    (thr/map->thrift m ColumnDef)))

(defn map->columnfamilydef [m]
  {:pre  [(contains? m :name)
          (contains? m :keyspace)
          (or (nil? (:column_type m))
              (contains? d/cassandra-column-types (:column_type m)))]}
  (thr/map->thrift m CfDef {:column_metadata #(mapv map->columndef %)
                            :compression_options #(into {} %)
                            :compaction_strategy_options #(into {} %)}))

(defn map->keyspacedef [m]
  {:pre  [(contains? m :name)
          (contains? m :cf_defs)]}
  (let [m  (if-let [so (:strategy_options m)]
             (assoc m :strategy_options (namify-keys so))
             m)
        m (assoc-nil m
                     :strategy_class "org.apache.cassandra.locator.SimpleStrategy"
                     :strategy_options {"replication_factor" "3"})]
    (thr/map->thrift m KsDef
                     {:cf_defs
                      (fn [cfs]
                        (->> cfs
                             (map #(assoc % :keyspace (:name m)))
                             (mapv map->columnfamilydef)))})))

(defmethod from-map ColumnDef
  [m _]
  (map->columndef m))

(defmethod from-map CfDef
  [m _]
  (map->columnfamilydef m))

(defmethod from-map KsDef
  [m _]
  (map->keyspacedef m))
