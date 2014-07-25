(ns cassius.core
  (:require [cassius.common :refer :all]
            [cassius.protocols]
            [cassius.api.connection]
            [cassius.api.pool]
            [cassius.component]
            [hara.namespace.import :refer [import]]
            [clojure.pprint :as pp])
  (:refer-clojure :exclude [import]))

(import cassius.protocols
        [create connect disconnect
         put-in peek-in keys-in drop-in set-in select-in mutate-in
         init-schema schema
         stream-in]

        cassius.component      [database])

(defn conn? [conn]
  (instance? cassius.protocols.IMap conn))

(defn diff-inserts
  ([m1 m2]
     (diff-inserts m1 m2 [] (atom {})))
  ([m1 m2 pv summary]
     (let [ks (keys m1)]
       (doseq [k ks]
         (let [v1 (get m1 k)
               v2 (get m2 k)]
           (cond (and (hash-map? v1)
                      (hash-map? v2))
                 (diff-inserts v1 v2 (conj pv k) summary)

                 (nil? v2)
                 (swap! summary conj [(conj pv k) v1]))))
       @summary)))

(defn diff-changes
  ([m1 m2]
     (diff-changes m1 m2 [] (atom {})))
  ([m1 m2 pv summary]
     (let [ks (keys m1)]
       (doseq [k ks]
         (let [v1 (get m1 k)
               v2 (get m2 k)]
           (cond (and (hash-map? v1)
                      (hash-map? v2))
                 (diff-changes v1 v2 (conj pv k) summary)

                 (nil? v2) nil

                 (not= v1 v2)
                 (swap! summary conj [(conj pv k) [v1 v2]]))))
       @summary)))

(defn diff [old new]
  (let [v+ (diff-inserts new old)
        v- (diff-inserts old new)
        v* (diff-changes old new)
        m (assoc-full {} :+ v+ :- v- :* v*)]
    (if-not (empty? m) m)))

(defn patch [conn changes]
  (if-let [adds (:+ changes)]
    (doseq [e adds]
      (apply put-in conn e)))
  (if-let [subs (:- changes)]
    (doseq [e subs]
      (drop-in conn (first e))))
  (if-let [reps (:* changes)]
    (doseq [e reps]
      (set-in conn (first e) (first (second 2)))))
  conn)

(defn rollback [conn changes]
  (if-let [adds (:- changes)]
    (doseq [e adds]
      (apply put-in conn e)))
  (if-let [subs (:+ changes)]
    (doseq [e subs]
      (drop-in conn (first e))))
  (if-let [reps (:* changes)]
    (doseq [e reps]
      (set-in conn (first e) (first e))))
  conn)

(defn restore
  ([conn source]
     (cond (atom? source)
           (apply restore @source)

           (conn? source)
           (let [sch  (schema source)
                 data (peek-in source)]
             (restore conn sch data))

           (and (vector? source) (= [String String] (map type source)))
           (let [sch (read-string (slurp (first source)))
                 data (read-string (slurp (second source)))]
             (restore conn sch data))))
  ([conn schema data]
     (drop-in conn)
     (init-schema schema)
     (set-in conn data)
     conn))

(defn pprint-str [m]
  (with-out-str (pp/pprint m)))

(defn backup [conn source]
  (let [sch  (schema conn)
        data (peek-in conn)]
    (cond (atom? source)
          (reset! source [sch data])

          (and (vector? source)
               (= [String String] (map type source)))
          (do (spit (first source)  (pprint-str sch))
              (spit (second source) (pprint-str data)))

          (conn? source)
          (restore source conn)))
  conn)
