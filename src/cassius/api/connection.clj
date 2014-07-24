(ns cassius.api.connection
  (:require [cassius.common :refer :all]
            [cassius.net.connection :refer [client]]
            [cassius.net.command.keyspace :as ksp]
            [cassius.protocols :refer [IMap IDatabase]]
            [cassius.schema.outline :as sch-ol]
            [cassius.schema.replica :as sch-rp]
            [cassius.api.connection.set-in :refer [set-in]]
            [cassius.api.connection.drop-in :refer [drop-in]]
            [cassius.api.connection.keys-in :refer [keys-in]]
            [cassius.api.connection.mutate-in :refer [mutate-in]]
            [cassius.api.connection.put-in :refer [put-in]]
            [cassius.api.connection.peek-in :refer [peek-in]]
            [cassius.api.connection.select-in :refer [select-in]]
            [ribol.core :refer [raise]])
  (:import [org.apache.cassandra.thrift Cassandra$Client]))

(defn init-schema-conn
  [conn schema]
  (cond (vector? schema)
        (doseq [m schema]
          (ksp/add-keyspace conn
                            (sch-rp/map->keyspacedef m)))

        (hash-map? schema)
        (init-schema-conn conn (sch-ol/expand schema))

        :else
        (raise [:wrong-input {:input schema}])))

(defn schema-conn
  ([conn] (schema-conn conn nil))
  ([conn type]
     (condp = type
       :outline
       (->> (schema-conn conn nil)
            (sch-ol/outline))

       (->> (ksp/user-keyspaces conn)
            (mapv sch-rp/keyspacedef->map)))))

(extend-protocol IMap
  cassius.net.connection.Connection
  (-put-in [conn arr v]
    (put-in conn arr v))
  (-peek-in [db arr]
    (peek-in db arr))
  (-keys-in  [db arr]
    (keys-in db arr))
  (-drop-in [db arr]
    (drop-in db arr))
  (-set-in [db arr v]
    (set-in db arr v))
  (-select-in [db arr]
    (select-in db arr))
  (-mutate-in [db ks add-map del-vec]
    (mutate-in db ks add-map del-vec)))

(extend-protocol IDatabase
  cassius.net.connection.Connection
  (-init-schema [db schema]
    (init-schema-conn db schema))
  (-schema      [db type]
    (schema-conn db type)))
