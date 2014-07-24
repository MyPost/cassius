(ns cassius.types.thrift
  (:require [cassius.common :refer :all]
            [cassius.types.coerce :refer [coerce]]
            [clojure.reflect]
            [ribol.core :refer [raise]]))

(def ^:dynamic *cache* (atom {}))

(defn class-for [type]
  (condp = type
    'boolean Boolean/TYPE
    'int  Integer/TYPE
    'short Short/TYPE
    'long Long/TYPE
    'float Float/TYPE
    'double Double/TYPE
    (Class/forName (name type))))

(defn take-while-seq
  ([n f coll] (take-while-seq n f coll n []))
  ([n f [x & xs :as more] i output]
     (cond (empty? more) output

           (or (empty? xs) (< i 0)) output

           (f x) (recur n f xs n (conj output x))

           :else
           (recur n f xs (dec i) (conj output x)))))

(defn thrift-fields [obj]
  (->> (range)
       (map (fn [i] (.fieldForId obj i)))
       (take-while-seq 4 identity)
       (filter identity)))

(defn thrift-description [class]
  (let [nil-obj  (.newInstance class)
        fields   (thrift-fields nil-obj)
        ids      (map (fn [x] (.getThriftFieldId x)) fields)
        defaults (map (fn [x] (.getFieldValue nil-obj x)) fields)
        names    (map (fn [x] (.getFieldName x)) fields)
        nset     (set names)
        types    (->> class
                      clojure.reflect/reflect
                      :members
                      (filter (fn [x] (get nset (str (:name x)))))
                      (map (fn [x] [(str (:name x)) (class-for (:type x))]))
                      (into {}))]
    {:type class
     :id-lookup (zipmap names ids)
     :field-lookup (zipmap names fields)
     :default-lookup (zipmap names defaults)
     :type-lookup types}))

(defn get-thrift
 ([class]
  (if-let [desc (get @*cache* class)]
    desc
    (let [desc (thrift-description class)]
      (swap! *cache* assoc class desc)
      desc)))
 ([class chain]
    ((apply comp (reverse chain)) (get-thrift class))))

(defn map->thrift
  ([m]
     (if-let [class (:cassius.class (meta m))]
       (map->thrift m class nil)
       (raise [:no-class])))
  ([m class] (map->thrift m class nil))
  ([m class mfn]
     (if-not (get (supers class) org.apache.thrift.TBase)
       (throw (Exception. (str "Class " class " is not a Thrift Class"))))

     (let [obj      (.newInstance class)
           field-lu (:field-lookup (get-thrift class))
           type-lu  (:type-lookup (get-thrift class))
           mfn      (namify-keys mfn)]
       (reduce (fn [obj [k value]]
                 (if-let [field (get field-lu (name k))]
                   (let [v (get m k)
                         nk (name k)
                         f (get mfn nk)
                         v (if f (f v) v)
                         t (get type-lu nk)
                         v (coerce v t)]
                     (.setFieldValue obj field v)))
                 obj)
               obj m))))

(defn thrift->map
  ([obj] (thrift->map obj nil))
  ([obj mfn]
     (let [class (class obj)
           field-lu  (:field-lookup   (get-thrift class))
           defaults  (:default-lookup (get-thrift class))
           mfn       (namify-keys mfn)]
       (-> (reduce (fn [m [k field]]
                     (let [v (.getFieldValue obj field)
                           f (get mfn k)
                           v (cond (fn? f) (f v)
                                   (class? f) (coerce v f)
                                   :else v)]
                       (assoc m k v)))
                   {} field-lu)
           (remove-same defaults)
           (keyify-keys)
           (with-meta {:cassius.class class}))))
  ([obj mfn defaults]
     (let [m (thrift->map obj mfn)
           mt (meta m)]
       (with-meta
         (remove-same m defaults)
         mt))))
