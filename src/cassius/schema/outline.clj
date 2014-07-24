(ns cassius.schema.outline
  (:require [cassius.common :refer :all]
            [cassius.schema.replica :as rp]
            [cassius.schema.definitions :as d]))

(defn columnmap->outline [m]
  (let [entry [(get d/cassandra-data-types (:validation_class m))]]
    [(:name m)
     (with-meta entry
       (-> m
           (dissoc :name :validation_class)
           (remove-same d/columndef-defaults)))]))

(defn columnfamilymap->outline [m]
  (let [cols (->> (:column_metadata m)
                  (map columnmap->outline)
                  (into {}))
        cols (with-meta cols
               (-> m
                   (dissoc :column_metadata :name :keyspace)
                   (remove-same d/columnfamilydef-defaults)))]
    {(:name m) cols}))

(defn keyspacemap->outline [m]
  (let [cfs (->> (:cf_defs m)
                 (map columnfamilymap->outline)
                 (into {}))
        cfs (with-meta cfs
              (-> m
                  (dissoc :cf_defs :name)
                  (remove-same d/keyspacedef-defaults)))]
    {(:name m) cfs}))

(defn expand-column-outline
  [name [vc :as v]]
  (merge-nil
   {:name name
    :validation_class (get d/cassandra-key-types vc)}
   (meta v)))

(defn expand-columnfamily-outline
  [keyspace name colmap]
  (merge-nil
   {:name name
    :keyspace keyspace
    :column_metadata (mapv #(apply expand-column-outline %) colmap)}
   (meta colmap)))

(defn expand-keyspace-outline
  [name cfmap]
  (merge-nil
   {:name name
    :cf_defs (mapv #(apply expand-columnfamily-outline name %) cfmap)}
   (meta cfmap)))

(defn outline [v]
  (cond (or (seq? v) (vector? v))
        (->> v
             (map keyspacemap->outline)

             (apply merge))
        (string? v)
        [(expand-keyspace-outline v {})]

        (hash-map? v)
        [(keyspacemap->outline v)]))

(defn expand [outline]
  (mapv #(apply expand-keyspace-outline %) outline))

(defn string->keyspacedef [ks]
  (-> (expand-keyspace-outline ks {})
      (rp/map->keyspacedef)))

(defn string->columnfamilydef
  [ks cf]
  (-> (string->columnfamilydef ks cf {})
      (rp/map->columnfamilydef)))

(defn map->columnfamilydef
  [ks cf]
  (-> cf
      (assoc :keyspace ks)
      (assoc-nil :cf_defs [])
      rp/map->columnfamilydef))

(defn outline->keyspacedefs [outline]
  (->> (expand outline)
       (map rp/map->keyspacedef)))
