(ns cassius.api.pool
  (:require [cassius.net.connection :as conn]
            [cassius.protocols :refer :all])
  (:import [org.apache.commons.pool2 PooledObjectFactory]
           [org.apache.commons.pool2.impl GenericObjectPool DefaultPooledObject]))

(defn pool-command [pool command & args]
  (let [conn (.borrowObject @(:instance pool))]
    (try
      (apply command conn args)
      (finally
        (.returnObject @(:instance pool) conn)))))

(defn connection-factory [this]
  (reify PooledObjectFactory
    (makeObject [_] (DefaultPooledObject. (connect (:host this) (:port this)
      (assoc this :type :connection))))
    (activateObject [_ conn])
    (validateObject [_ conn] (if-let [inst (-> conn .getObject :instance deref)]
                               (-> inst :tr .isOpen)))
    (passivateObject [_ conn])
    (destroyObject [_ conn] (disconnect (.getObject conn)))))

(defrecord ConnectionPool [host port]
  IConnection
  (-connect [pool]
    (if @(:instance pool) (disconnect pool))
    (reset! (:instance pool)
            (GenericObjectPool. (connection-factory pool))))
  (-disconnect [pool]
    (when @(:instance pool)
      (.close @(:instance pool))
      (reset! (:instance pool) nil)))

  IMap
  (-put-in [pool arr v]
    (pool-command pool put-in arr v))
  (-peek-in [pool arr]
    (pool-command pool peek-in arr))
  (-keys-in  [pool arr]
    (pool-command pool keys-in arr))
  (-drop-in [pool arr]
    (pool-command pool drop-in arr))
  (-set-in [pool arr v]
    (pool-command pool set-in arr v))
  (-select-in [pool arr]
    (pool-command pool select-in arr))
  (-mutate-in [pool ks add-map del-vec]
    (pool-command pool mutate-in ks add-map del-vec))

  IDatabase
  (-init-schema [pool schema]
    (pool-command pool init-schema schema))
  (-schema      [pool type]
    (pool-command pool schema type))

  IStream
  (-stream-in [pool arr opts]
    (pool-command pool stream-in arr opts)))

(defmethod create :pool [m]
  (assoc (map->ConnectionPool m) :instance (atom nil)))
