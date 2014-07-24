(ns cassius.test-types-thrift
  (:use midje.sweet)
  (:require [cassius.types.thrift :refer :all])
(:import org.apache.cassandra.thrift.Column))

(fact "class-for returns the class represented by a symbol"

  (class-for 'int) => Integer/TYPE

  (class-for 'boolean) => Boolean/TYPE

  (class-for 'java.lang.String) => java.lang.String)


(fact "take-while-seq is like take, but when it fails `n` checks in a row, it will stop taking"
  (take-while-seq 0 identity [1 2 3 nil]) => [1 2 3]

  (take-while-seq 0 identity [1 nil 3 nil]) => [1 nil]

  (take-while-seq 1 identity [1 nil 3 nil]) => [1 nil 3])


(fact "thrift-fields gets all fields in a thrift Class"
  (->> (thrift-fields (Column.))
       (map (fn [f] (.getFieldName f))))
  => ["name" "value" "timestamp" "ttl"]

  (->> (thrift-fields (Column.))
       (map (fn [f] (.getThriftFieldId f))))
  => [1 2 3 4]

  (->> (thrift-fields (Column.))
       (map (fn [f] (.getFieldValue (Column.) f))))
  => [nil nil 0 0]
)

(fact "thrift-description returns a list of descriptors for a Thrift Class"
  (keys (thrift-description Column))
  => [:type :id-lookup :field-lookup :default-lookup :type-lookup]

  (thrift-description Column)
  => (contains
       {:type org.apache.cassandra.thrift.Column,
        :default-lookup {"name" nil, "timestamp" 0, "ttl" 0, "value" nil},
        :id-lookup {"name" 1, "timestamp" 3, "ttl" 4, "value" 2},
        :type-lookup {"name" java.nio.ByteBuffer,
                      "timestamp" Long/TYPE
                      "ttl"  Integer/TYPE
                      "value" java.nio.ByteBuffer}}))

(fact "get-thrift uses a cached version of a Thrift class"
  (get-thrift Column [:type])
  => org.apache.cassandra.thrift.Column

  (get-thrift Column [:id-lookup keys sort])
  => ["name" "timestamp" "ttl" "value"])


(fact "map->thrift takes a map and turn it into a Thrift Object"
  (let [obj (map->thrift {:name "Description"
                          :timestamp 100000
                          :ttl 100
                          :value "This is a Test"}
                        Column)]
    (String. (.getName obj)) => "Description"
    (.getTimestamp obj) => 100000
    (.getTtl obj) => 100
  (String. (.getValue obj)) => "This is a Test"))


(fact "thrift->map takes a thrift object and turns it into a map"
  (let [obj (map->thrift {:name "Description"
                          :timestamp 100000
                          :ttl 100
                          :value "This is a Test"}
                          Column)]
    (thrift->map obj {:name (fn [i] (String. i))
                      :value (fn [i] (String. i))})
    => {:name "Description"
        :timestamp 100000
        :ttl 100
      :value "This is a Test"}))
