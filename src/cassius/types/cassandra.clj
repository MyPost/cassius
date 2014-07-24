(ns cassius.types.cassandra
  (:import [org.apache.cassandra.thrift
            Column ColumnOrSuperColumn
            CounterColumn KeySlice
            Mutation SuperColumn])
  (:require [cassius.protocols :refer [from-bytes to-map from-map Mappable
                                       *default-key-encoding*
                                       *default-value-encoding*]]
            [cassius.types.thrift :as thr]
            [cassius.types.byte-buffer :as bb]))

(defn column->map
  ([^Column col] (column->map col *default-key-encoding* *default-value-encoding*))
  ([^Column col tname tvalue]
     {:name (from-bytes (.getName col) tname)
      :value (let [v (.getValue col)]
               (try (from-bytes v tvalue)
                    (catch Exception e
                      v)))
      :ttl (.getTtl col)
      :timestamp (.getTimestamp col)}))

(defn supercolumn->map
  ([^SuperColumn scol] (supercolumn->map scol *default-key-encoding* nil))
  ([^SuperColumn scol tname tmap]
     (let [cols  (.getColumns scol)
           names (map (fn [c]
                        (from-bytes (.getName c) tname))
                      cols)
           types (map #(or (get tmap %) *default-value-encoding*) names)]
       {:name (from-bytes (.getName scol) tname)
        :columns (mapv #(column->map %1 tname %2) cols types)})))

(defn optioncolumn->map
  ([^ColumnOrSuperColumn opcol] (optioncolumn->map opcol *default-key-encoding* nil))
  ([^ColumnOrSuperColumn opcol tname tmap]
     (let [subcol (or (.getSuper_column opcol)
                     (.getColumn opcol))]
       (condp = (type subcol)
         Column
         (let [tvalue (or (get tmap (from-bytes (.getName subcol) tname))
                          *default-value-encoding*)]
           (column->map subcol tname tvalue))

         SuperColumn
         (supercolumn->map subcol tname tmap)))))

(defn keyslice->map
  ([^KeySlice ksl] (keyslice->map ksl *default-key-encoding* nil))
  ([^KeySlice ksl tname tmap]
     {:key (from-bytes (.getKey ksl) tname)
      :columns (mapv #(optioncolumn->map % tname tmap)
                     (.getColumns ksl))}))

(extend-protocol Mappable
  Column
  (to-map [col]
    (column->map col))

  SuperColumn
  (to-map [scol]
    (supercolumn->map scol))

  ColumnOrSuperColumn
  (to-map [opcol]
    (optioncolumn->map opcol))

  KeySlice
  (to-map [ksl]
    (keyslice->map ksl)))
