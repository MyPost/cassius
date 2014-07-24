(ns cassius.test-component
  (:use midje.sweet)
  (:require [cassius.core :refer :all]
            [com.stuartsierra.component :as component]))

(fact
  (def settings (read-string (slurp "settings.edn")))

  (def db (component/start (database settings)))

  (-> db
      (drop-in)
      (peek-in))
  => {}

  (-> db
      (put-in {"crystals" {"species" {"citrine" {"price" "$400"}}}})
      (peek-in))
  => {"crystals" {"species" {"citrine" {"price" "$400"}}}}

  (-> db
      (drop-in)
      (mutate-in "crystals"
                 {"species" {"citrine" {"DATA" {"price" "$400" "value" "2"}}}}
                 [])
      (mutate-in "crystals"
                 {}
                 [["species" "citrine" "DATA" "price"]])
      (peek-in))
  => {"crystals" {"species" {"citrine" {"DATA" {"value" "2"}}}}}

  (component/stop db))
