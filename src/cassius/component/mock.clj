(ns cassius.component.mock
  (:require [cassius.protocols :refer :all ]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [infof]]))

(def ^:dynamic *dbs* (atom {}))

(defn purge-dbs
  ([] (reset! *dbs* {}))
  ([& ks] (swap! *dbs* apply dissoc ks)))

(defn list-dbs
  ([] (keys @*dbs*)))

(defn get-db [name]
  (if-let [db (get *dbs* name)]
    db
    (let [db (atom {})]
      (swap! *dbs* assoc name db)
      db)))

(defn hash-map? [x]
  (instance? clojure.lang.APersistentMap x))

(defn dissoc-in
  ([m [k & ks]]
     (if m
       (if-not ks
         (dissoc m k)
         (let [nm (dissoc-in (m k) ks)]
           (cond (empty? nm) (dissoc m k)
                 :else (assoc m k nm)))))))

(defn merge-nested
  ([m] m)
  ([m1 m2]
     (if-let [[k v] (first m2)]
       (cond (nil? (get m1 k))
             (recur (assoc m1 k v) (dissoc m2 k))

             (and (hash-map? v) (hash-map? (get m1 k)))
             (recur (assoc m1 k (merge-nested (get m1 k) v)) (dissoc m2 k))

             (not= v (get m1 k))
             (recur (assoc m1 k v) (dissoc m2 k))

             :else
             (recur m1 (dissoc m2 k)))
       m1))
  ([m1 m2 m3 & ms]
     (apply merge-nested (merge-nested m1 m2) m3 ms)))


(defrecord MockDatabase [meta]
  Object
  (toString [this]
    (format "#db.mock%s"
            (assoc meta :connected? (if @(:instance this) true false)
                   :name (or (:name meta)
                             (:name this)))))

  component/Lifecycle
  (start [this]
    (let [nm (or (:name meta)
                 (:name this)
                 (str (java.util.UUID/randomUUID)))]
      (reset! (:instance this) (get-db nm))
      (infof "Connecting to Mock Database %s" nm)
      (assoc this :name nm)))

  (stop [this]
    (infof "Closing connection to Database")
    (reset! (:instance this) nil)
    this)

  IMap
  (-put-in [db arr v]
    (let [im (if (empty? arr) v
                 (assoc-in {} arr v))]
      (swap! @(:instance db) merge-nested im)))
  (-peek-in [db arr]
    (if (empty? arr)
      (or (-> db :instance deref deref) {})
      (get-in (-> db :instance deref deref) arr)))
  (-keys-in  [db arr]
    (-> db (peek-in arr) keys))
  (-drop-in [db arr]
    (if (empty? arr)
      (reset! @(:instance db) {})
      (swap! @(:instance db) dissoc-in arr)))
  (-set-in [db arr v]
    (if (empty? arr)
      (reset! @(:instance db) v)
      (swap! @(:instance db) assoc-in arr v)))
  (-mutate-in [db ks add-map del-vec]
    (let [addm {ks add-map}]
      (swap! @(:instance db) merge-nested addm)
      (doseq [arr del-vec]
        (swap! @(:instance db) dissoc-in (cons ks arr))))))

(defmethod print-method MockDatabase [v w]
  (.write w (str v)))
