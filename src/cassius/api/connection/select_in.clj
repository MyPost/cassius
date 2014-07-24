(ns cassius.api.connection.select-in
  (:require [cassius.common :refer [into-full hash-map?]]
            [cassius.api.connection.keys-in :refer [keys-in]]
            [cassius.api.connection.peek-in :refer [keyslice->outline-entry]]
            [cassius.net.command.retrieve :refer [retrieve-column-family]]
            [cassius.protocols :refer [from-bytes *default-key-encoding* *default-value-encoding*]]
            [cassius.types.byte-buffer]))

(defn filter-fn
  ([ks] (filter-fn ks identity))
  ([ks f]
      (cond (string? ks) #(= ks (f %))

            (set? ks) #(ks (f %))

            (instance? java.util.regex.Pattern ks)
            #(re-find ks (f %))

            (fn? ks)
            #(ks (f %)))))

(defn filter-keys [ksps ksi]
  (cond (or (nil? ksi) (= ksi '_)) ksps
        :else (filter (filter-fn ksi) ksps)))

(defn filter-rows [rows rowi]
  (cond (or (nil? rowi) (= rowi '_)) rows

        :else (filter (filter-fn rowi
                                 #(from-bytes (.getKey %) *default-key-encoding*))
                      rows)))

(defn filter-subcolumns [m subcoli]
  (cond (hash-map? m)
        (let [ks (keys m)
              fks (filter-keys ks subcoli)]
          (select-keys m fks))

        :else m))

(defn filter-columns [m coli subcoli]
  (let [ks (keys m)
        fks (filter-keys ks coli)]
    (->> fks
        (map (fn [k] [k (filter-subcolumns (get m k) subcoli)]))
        (into {}))))

(defn select-in-row-outline [[k m] [coli subcoli]]
  [k (filter-columns m coli subcoli)])

(defn select-in-column-family [conn ks cf [rowi & more]]
  (let [rows (retrieve-column-family conn ks cf)]
    (->> (filter-rows rows rowi)
         (map keyslice->outline-entry)
         (map #(select-in-row-outline % more))
         (into-full {}))))

(defn select-in-keyspace [conn ks [cfi & more]]
  (let [cfs (keys-in conn [ks])]
    (->> (filter-keys cfs cfi)
         identity
         (map (fn [cf]
                [cf (select-in-column-family conn ks cf more)]))
         (into-full {}))))

(defn select-in [conn [ksi & more]]
  (let [kss (keys-in conn)]
    (->> (filter-keys kss ksi)

         (map (fn [ks]
                [ks (select-in-keyspace conn ks more)]))
         (into-full {}))))
