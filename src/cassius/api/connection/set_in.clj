(ns cassius.api.connection.set-in
  (:require [cassius.api.connection.put-in :refer [put-in]]
            [cassius.api.connection.drop-in :refer [drop-in]]))

(defn set-in
  ([conn v]
     (drop-in conn)
     (put-in conn v))
  ([conn arr v]
     (drop-in conn arr)
     (put-in conn arr v)))