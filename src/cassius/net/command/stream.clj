(ns cassius.net.command.stream
  (:require [cassius.net.command.retrieve :as retrieve]
            [cassius.protocols :refer [to-bbuff]]))

(defn stream-column-family
  ([conn ks cf]
     (stream-column-family conn ks cf retrieve/EMPTY))
  ([conn ks cf start-key]
     (stream-column-family conn ks cf start-key retrieve/EMPTY))
  ([conn ks cf start-key end-key]
     (stream-column-family conn ks cf start-key end-key 100))
  ([conn ks cf start-key end-key batch-count]
      (let [res (retrieve/retrieve-column-family conn ks cf start-key end-key batch-count)]
        (if (> batch-count (count res))
          res
          (let [k (.getKey (last res))]
            (concat (butlast res)
                    (lazy-seq (stream-column-family conn ks cf k end-key batch-count))))))))

(defn column-name [option-column]
  (-> option-column
      ((fn [x] (if (.isSetSuper_column x)
                (.getSuper_column x)
                (.getColumn x))))
      (.getName)
      (to-bbuff)))

(defn stream-row
  ([conn ks cf row]
     (stream-row conn ks cf row retrieve/EMPTY))
  ([conn ks cf row start-key]
     (stream-row conn ks cf row start-key retrieve/EMPTY))
  ([conn ks cf row start-key end-key]
     (stream-row conn ks cf row start-key end-key false))
  ([conn ks cf row start-key end-key reverse?]
     (stream-row conn ks cf row start-key end-key reverse? 100))
  ([conn ks cf row start-key end-key reverse? batch-count]
      (let [res (retrieve/retrieve-row conn ks cf row start-key end-key reverse? batch-count)]
        (if (> batch-count (count res))
          res
          (let [k (column-name (last res))]
            (concat (butlast res)
                    (lazy-seq (stream-row conn ks cf row
                                             k end-key reverse? batch-count))))))))
