(ns cassius.protocols)

(def ^:dynamic *default-key-encoding* :utf-8)
(def ^:dynamic *default-value-encoding* :default)

(defprotocol Mappable
  (to-map [m] "Converts object into a hashmap"))

(defmulti from-map
  "Takes a map and converts it into type t"
  (fn [m t] t))

(defprotocol ByteBufferable
  (to-bbuff [v] "Converts object into a java.nio.ByteBuffer object"))

(defn to-bytes [v]
  (.array (to-bbuff v)))

(defmulti from-bytes
  "Takes a ByteArray and converts it into type t"
  (fn [bs t] t))

(defn from-bbuff [bb type]
  (from-bytes (.array bb) type))

(defprotocol IConnection
  (-connect [_])
  (-disconnect [_]))

(defmulti create (fn [m] (:type m)))

(defmethod create :default [m]
  (create (assoc m :type :pool)))

(defn connect
  ([conn] (-connect conn))
  ([host port & [opts]]
    (let [conn (-> opts
                   (assoc :host host :port port)
                   (create))]
      (-connect conn)
      conn)))

(defn disconnect [conn]
  (-disconnect conn)
  conn)

(defprotocol IMap
  (-put-in [db arr v])
  (-peek-in [db arr])
  (-keys-in  [db arr])
  (-drop-in [db arr])
  (-set-in [db arr v])
  (-select-in [db arr])
  (-mutate-in [db ks add-map del-vec]))

(defn put-in
  ([db v] (put-in db [] v))
  ([db arr v]
    (-put-in db arr v)
    db))

(defn peek-in
  ([db] (peek-in db []))
  ([db arr]
    (-peek-in db arr)))

(defn keys-in
  ([db] (keys-in db []))
  ([db arr]
    (-keys-in db arr)))

(defn drop-in
  ([db] (drop-in db []))
  ([db arr]
    (-drop-in db arr)
    db))

(defn set-in
  ([db v] (set-in db [] v))
  ([db arr v]
     (-set-in db arr v)
     db))

(defn select-in
  ([db] (select-in db []))
  ([db arr]
    (-select-in db arr)))

(defn mutate-in
  [db ks add-map del-vec]
  (-mutate-in db ks add-map del-vec)
  db)

(defprotocol IDatabase
  (-init-schema [db schema])
  (-schema      [db type]))

(defn init-schema
  ([db] (if-let [sch (:schema db)]
          (init-schema db sch)
          db))
  ([db schema]
    (-init-schema db schema)
    db))

(defn schema [db type]
  (-schema db type))
