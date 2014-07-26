(ns cassius.api.connection.stream-in
  (:require [cassius.net.command.stream :as stream]
            [cassius.net.command.retrieve :as retrieve]
            [cassius.api.connection.peek-in :refer [optioncolumn->outline-entry
                                                    keyslice->outline-entry
                                                    default-encodings]]
            [cassius.protocols :refer [to-bbuff]]))

(defn stream-in
  [conn [ks cf row :as args] opts]
  (let [[tname tmap] (default-encodings conn ks cf)]
    (cond (or (nil? ks) (nil? cf))
          (throw (Exception. "<ks> and <cf> cannot be nil."))

          (nil? row)
          (map #(binding [cassius.protocols/*default-value-encoding*
                          (or (-> conn :value-type)
                              cassius.protocols/*default-value-encoding*)]
                  (keyslice->outline-entry % tname tmap))
               (stream/stream-column-family
                conn ks cf
                (to-bbuff (or (:start opts) ""))
                (to-bbuff (or (:end opts) ""))
                (or (:batch opts) 100)))

          :else
          (map #(binding [cassius.protocols/*default-value-encoding*
                          (or (-> conn :value-type)
                              cassius.protocols/*default-value-encoding*)]
                  (optioncolumn->outline-entry % tname tmap))
               (stream/stream-row
                conn ks cf row
                (to-bbuff (or (:start opts) ""))
                (to-bbuff (or (:end opts) ""))
                (if (nil? (:reverse opts)) true (:reverse opts))
                (or (:batch opts) 100))))))
