(ns cassius.component
  (:require [cassius.component.database]
            [cassius.component.mock]
            [com.stuartsierra.component :as component])
  (:import cassius.component.database.Database
           cassius.component.mock.MockDatabase))

(defmulti database :type)

(defmethod database :default [meta] (if (empty? meta)
                                      (database {:type :mock})
                                      (database (assoc meta :type :database))))
(defmethod database :mock  [meta]
  (assoc (MockDatabase. meta)
    :instance (atom nil)))

(defmethod database :database [meta]
  (assoc (Database. meta)
    :instance (atom nil)))
