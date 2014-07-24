(ns cassius.data.outline
  (:require [cassius.protocols :refer :all]
            [cassius.common :refer :all]
            [cassius.schema.definitions :as sch]
            [cassius.types.byte-buffer :as bb]
            [cassius.types.cassandra :as cass])
  (:import [org.apache.cassandra.thrift
            Column ColumnOrSuperColumn
            Mutation SuperColumn]))

(defn column-levels [m]
  (if-not (instance? clojure.lang.APersistentMap m)
    0
    (inc (column-levels (first (vals m))))))

(defn outline->columns [m]
  (mapv (fn [[k v]]
          (doto (Column.)
            (.setName (to-bbuff k))
            (.setValue (to-bbuff v))
            (.setTimestamp (System/currentTimeMillis))))
        m))

(defn outline->supercolumns [m]
  (mapv (fn [[k v]]
          (doto (SuperColumn.)
            (.setName (to-bbuff k))
            (.setColumns (outline->columns v))))
        m))

(defn outline->optioncolumns [m]
  (let [lvl (column-levels m)]
    (mapv
     (fn [opcol]
       (let [obj (ColumnOrSuperColumn.)]
         (condp = lvl
           1 (.setColumn obj opcol)
           2 (.setSuper_column obj opcol))
         obj))
     (condp = lvl
       1 (outline->columns m)
       2 (outline->supercolumns m)))))

(defn outline->mutation [cf m]
  (let [mfn (fn [opcol]
              (doto (Mutation.)
                (.setColumn_or_supercolumn opcol)))]
    (->> (map (fn [[k sbm]]
                [(to-bbuff k)
                 {cf (map mfn (outline->optioncolumns sbm))}])
              m)
         (into {}))))

(defn columnmap->outline [m]
 (let [entry [(get sch/cassandra-data-types (:validation_class m))]]
   [(:name m)
    (with-meta entry
      (-> m
          (dissoc :name :validation_class)
          (remove-same sch/columndef-defaults)))]))

(defn columnfamilymap->outline [m]
 (let [cols (->> (:column_metadata m)
                 (map columnmap->outline)
                 (into {}))
       cols (with-meta cols
              (-> m
                  (dissoc :column_metadata :name :keyspace)
                  (remove-same sch/columnfamilydef-defaults)))]
   {(:name m) cols}))

(defn keyspacemap->outline [m]
 (let [cfs (->> (:cf_defs m)
                (map columnfamilymap->outline)
                (into {}))
       cfs (with-meta cfs
             (-> m
                 (dissoc :cf_defs :name)
                 (remove-same sch/keyspacedef-defaults)))]
   {(:name m) cfs}))
