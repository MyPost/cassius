(ns cassius.api.connection.drop-in
  (:require [cassius.net.command
             [keyspace :as ksp]
             [column-family :as cf]
             [delete :refer [delete-row delete-normal-columns delete-sub-columns]]]))

(defn drop-in
  ([conn]
    (drop-in conn []))
  ([conn arr]
     (condp = (count arr)
       0 (ksp/drop-all-keyspaces conn)
       1 (apply ksp/drop-keyspace conn arr)
       2 (apply cf/drop-column-family conn arr)
       3 (apply delete-row conn arr)
       4 (apply delete-normal-columns conn arr)
       5 (apply delete-sub-columns conn arr))))
