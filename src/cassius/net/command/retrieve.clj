(ns cassius.net.command.retrieve
  (:require [cassius.protocols :refer [to-bbuff]]
            [cassius.types.byte-buffer]
            [cassius.net.connection :refer [client]]
            [cassius.net.command
              [keyspace :as ksp]
              [macros :refer [raise-on-invalid-request]]])
  (:import [org.apache.cassandra.thrift SlicePredicate ColumnParent
                                        KeyRange SliceRange
                                        ConsistencyLevel]))

(def EMPTY (to-bbuff ""))

(defn retrive-slice [conn ks cf row ^SlicePredicate slp]
  (let [cp (ColumnParent. cf)]
    (ksp/set-keyspace conn ks)
    (raise-on-invalid-request
     [conn ks cf :retrieve :column-family-not-found]
     (.get_slice (client conn) (to-bbuff row) cp slp
                 (or (:consistency conn) ConsistencyLevel/ONE)))))

(defn retrieve-range-slices
  ([conn ks cf ^SlicePredicate slp]
     (retrieve-range-slices conn ks cf slp EMPTY EMPTY Integer/MAX_VALUE))
  ([conn ks cf ^SlicePredicate slp start-key end-key count]
     (let [cp (ColumnParent. cf)
           kr (doto (KeyRange.)
                (.setStart_key start-key)
                (.setEnd_key end-key)
                (.setCount count))]
       (ksp/set-keyspace conn ks)
       (raise-on-invalid-request
        [conn ks cf :retrieve :column-family-not-found]
        (.get_range_slices (client conn) cp slp kr
                           (or (:consistency conn) ConsistencyLevel/ONE))))))

(defn retrieve-row
  ([conn ks cf row]
   (retrieve-row conn ks cf row EMPTY EMPTY false Integer/MAX_VALUE))
  ([conn ks cf row start-key end-key reverse? count]
     (let [slr (SliceRange. start-key
                            end-key
                            reverse? count)
           slp (doto (SlicePredicate.)
                  (.setSlice_range slr))]
       (retrive-slice conn ks cf row slp))))

(defn retrieve-column
  [conn ks cf row col]
  (let [COL (to-bbuff col)
        slr (SliceRange. COL
                         COL
                         false 1)
        slp (doto (SlicePredicate.)
              (.setSlice_range slr))]
    (retrive-slice conn ks cf row slp)))

(defn retrieve-column-family
  ([conn ks cf]
     (retrieve-column-family conn ks cf EMPTY))
  ([conn ks cf start-key]
     (retrieve-column-family conn ks cf start-key EMPTY))
  ([conn ks cf start-key end-key]
     (retrieve-column-family conn ks cf start-key end-key Integer/MAX_VALUE))
  ([conn ks cf start-key end-key max-results]
      (let [cp  (ColumnParent. cf)
            slr (SliceRange. EMPTY EMPTY
                             false Integer/MAX_VALUE)
            slp (doto (SlicePredicate.)
              (.setSlice_range slr))]
        (retrieve-range-slices conn ks cf slp start-key end-key max-results))))
