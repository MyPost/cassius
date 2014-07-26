(ns cassius.test-component
  (:use midje.sweet)
  (:require [cassius.core :refer :all]
            [com.stuartsierra.component :as component]))

(fact
  (def settings (assoc (read-string (slurp "settings.edn"))
                  :schema {"crystals" {"species" {"price" [:double]
                                                  "sold"  [:date]}}}))

  (def db (component/start (database settings)))

  (-> db
      (drop-in)
      (peek-in))
  => {}


  (-> db
      (put-in {"crystals" {"species" {"citrine" {"price" 400.00 "sold" #inst "1970-01-01T00:00:00.000-00:00"}}}})
      (peek-in))
  => {"crystals" {"species" {"citrine" {"sold" #inst "1970-01-01T00:00:00.000-00:00", "price" 400.0}}}}


  (-> db
      (drop-in)
      (mutate-in "crystals"
                 {"species" {"citrine" {"DATA-1" {"price" 400.00 "value" "2"}}}}
                 [])
      (mutate-in "crystals"
                 {"species" {"citrine" {"DATA-2" {"price" 500.00 "value" "2"}}}}
                 [])
      (peek-in))
  => {"crystals" {"species" {"citrine" {"DATA-2" {"value" "2", "price" 500.0}, "DATA-1" {"value" "2", "price" 400.0}}}}}

  (stream-in db ["crystals" "species"])
  => [["citrine" {"DATA-1" {"value" "2", "price" 400.0}
                  "DATA-2" {"value" "2", "price" 500.0}}]]
  (stream-in db ["crystals" "species" "citrine"])
  => [["DATA-2" {"value" "2", "price" 500.0}]
      ["DATA-1" {"value" "2", "price" 400.0}]])

  (component/stop db)
