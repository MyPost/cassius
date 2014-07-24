(ns cassius.api.connection.keys-in
  (:require [ribol.core :refer [raise]]
            [cassius.protocols :refer [to-map]]
            [cassius.types.cassandra]
            [cassius.net.command
             [keyspace :as ksp]
             [column-family :as cf]
             [retrieve :refer [retrieve-column-family retrieve-row]]]))

(defn get-keyspace-keys [conn]
  (->> (ksp/describe-keyspaces conn)
       (map (fn [x] (.getName x)))
       (filter #(not (get ksp/system-keyspace-names %)))))

(defn get-columnfamily-keys [conn ks]
  (if-let [ks (ksp/describe-keyspace conn ks)]
    (->> (.getCf_defs ks)
         (map (fn [x] (.getName x))))))

(defn get-row-keys [conn ks cf]
  (let [sls (retrieve-column-family conn ks cf)]
    (map (fn [x] (String. (.getKey x)))
         sls)))

(defn get-column-keys [conn ks cf row]
  (let [sls (retrieve-row conn ks cf row)]
    (->>  sls
          (map to-map)
          (map :name))))

(defn get-subcolumn-keys [conn ks cf row col]
  (let [sls (retrieve-row conn ks cf row)]
    (->>  sls
          (map to-map)
          (filter (fn [x] (= col (:name x))))
          first
          :columns
          (map :name))))

(defn keys-in
  ([conn] (get-keyspace-keys conn))
  ([conn arr]
     (condp = (count arr)
        0 (get-keyspace-keys conn)
        1 (apply get-columnfamily-keys conn arr)
        2 (apply get-row-keys conn arr)
        3 (apply get-column-keys conn arr)
        4 (apply get-subcolumn-keys conn arr)
        (raise :invalid-arguments "array selector can only have between 0 and 4 arguments"))))




(comment
  (>refresh)
  (def conn (net/connect))

  (get-column-keys conn "zoo" "kee" "12323126")
  (net/retrieve-row conn "zoo" "kee" "12323126")

  (map (fn [x] (.getName x)) (keys-in-db conn))

  (keys-in conn ["zoo"])
  (keys-in conn ["zoo" "kee" "12323126" "contentData" "oeuo"])

  (get-keyspace-keys conn)

  (get-columnfamily-keys conn "zoo")
  (get-row-keys conn "zoo" "oeuoe")

  (keys-in-db conn)

  (keys-in-ks conn "zoo")













)
