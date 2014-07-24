(ns cassius.component.database
  (:require [cassius.protocols :refer :all ]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [infof]]))


(defn component-command [db command & args]
  (apply command @(:instance db) args))

(defrecord Database [meta]
  Object
  (toString [this]
    (format "#db%s"
            (assoc meta :connected? (if @(:instance this) true false))))

  component/Lifecycle
  (start [this]
    (infof "Connecting to Database on %s:%s" (:host meta) (:port meta))
    (reset! (:instance this) (connect (:host meta) (:port meta) (assoc meta :type :pool)))
    this)

  (stop [this]
    (infof "Closing connection to Database")
    (disconnect @(:instance this))
    (reset! (:instance this) nil)
    this)

  IMap
  (-put-in [db arr v]
    (component-command db put-in arr v))
  (-peek-in [db arr]
    (component-command db peek-in arr))
  (-keys-in  [db arr]
    (component-command db keys-in arr))
  (-drop-in [db arr]
    (component-command db drop-in arr))
  (-set-in [db arr v]
    (component-command db set-in arr v))
  (-select-in [db arr]
    (component-command db select-in arr))
  (-mutate-in [db ks add-map del-vec]
    (component-command db mutate-in ks add-map del-vec))

  IDatabase
  (-init-schema [db schema]
    (component-command db init-schema schema))
  (-schema      [db type]
    (component-command db schema type)))

(defmethod print-method Database [v w]
  (.write w (str v)))
