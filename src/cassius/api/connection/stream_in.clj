(ns cassius.api.connection.stream-in
  (:require [cassius.net.command.stream :as stream]
            [cassius.net.command.retrieve :as retrieve]
            [cassius.api.connection.peek-in :refer [optioncolumn->outline-entry
                                                    keyslice->outline-entry]]))

(defn stream-in
  [conn [ks cf row :as args] opts]
  (cond (or (nil? ks) (nil? cf))
        (throw (Exception. "<ks> and <cf> cannot be nil."))

        (nil? row)
        (map #(keyslice->outline-entry % (:tname opts)
                                       (:tmap opts))
             (stream/stream-column-family
              conn ks cf
              (or (:start opts) retrieve/EMPTY)
              (or (:end opts) retrieve/EMPTY)
              (or (:batch opts) 100)))

        :else
        (map #(optioncolumn->outline-entry % (:tname opts)
                                           (:tmap opts))
             (stream/stream-row
              conn ks cf row
              (or (:start opts) retrieve/EMPTY)
              (or (:end opts) retrieve/EMPTY)
              (if (nil? (:reverse opts)) true (:reverse opts))
              (or (:batch opts) 100)))))
