(ns cassius.common)

(defn hash-map? [x]
  (instance? clojure.lang.APersistentMap x))

(defn atom? [x]
  (instance? clojure.lang.Atom x))

(defn assoc-nil
  ([m k v]
     (if (get m k) m (assoc m k v)))
  ([m k v & more]
     (apply assoc-nil (assoc-nil m k v) more)))

(defn assoc-full
  ([m k v]
      (if-not (empty? v)
        (assoc m k v)
        m))
  ([m k v & more]
     (apply assoc-full (assoc-full m k v) more)))

(defn into-full
  [m colls]
  (reduce (fn [m [k v]]
            (if-not (empty? v)
              (assoc m k v)
              m))
          m
          colls))

(defn vectorize [v]
  (cond (vector? v) v
        (seq? v) (vec v)
        :else [v]))

(defn remove-same
  ([m sub] (remove-same m sub =))
  ([m sub comp]
    (reduce (fn [out [k v]]
              (if (comp (get sub k) v) out
                  (assoc out k v)))
            {} m)))

(defn merge-nil
  ([to from]
     (reduce (fn [m [k v]]
               (assoc-nil m k v))
             to from))
  ([to from & more]
     (apply merge-nil (merge-nil to from) more)))

(defn select-keys-in [m sel]
  (cond (keyword? sel)
        (sel m)

        (vector? sel)
        (select-keys m sel)

        :else
        (throw (Exception. (str "Selector: " sel " can only be a keyword or vector")))))

(defn namify-keys [m]
  (reduce (fn [m [k v]]
            (assoc m (name k)

                   (if (instance? clojure.lang.APersistentMap v)
                     (namify-keys v)
                     v)))
          {} m))

(defn keyify-keys [m]
  (reduce (fn [m [k v]]
            (assoc m (if (keyword? k) k (keyword k))
                   (if (instance? clojure.lang.APersistentMap v)
                     (keyify-keys v)
                     v)))
          {} m))
