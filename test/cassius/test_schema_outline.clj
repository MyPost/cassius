(ns cassius.test-schema-outline
  (:use midje.sweet)
  (:require
   [cassius.protocols :refer :all]
   [cassius.schema.outline :refer :all])
  (:import [org.apache.cassandra.thrift
            CfDef KsDef ColumnDef IndexType]))

(fact "columnmap->outline"
  (columnmap->outline
   {:name "age", :validation_class "UTF8Type"})
  => ["age" [:utf-8]])

(fact "columnfamilymap->outline"
  (columnfamilymap->outline
   {:keyspace "zoo",
    :name "visitor",
    :column_metadata [{:name "name", :validation_class "UTF8Type"}
                      {:name "age", :validation_class "UTF8Type"}]})
  => {"visitor" {"name" [:utf-8], "age" [:utf-8]}})


(fact "keyspacemap->outline"
  (keyspacemap->outline
   {:name "zoo",
    :strategy_class "org.apache.cassandra.locator.SimpleStrategy",
    :strategy_options {:replication_factor "3"},
    :cf_defs [{:keyspace "zoo",
               :name "visitor",
               :column_metadata [{:name "name", :validation_class "UTF8Type"}
                                 {:name "age", :validation_class "UTF8Type"}]}
              {:keyspace "zoo",
               :name "animals",
               :column_metadata []}]})

  => {"zoo" {"visitor" {"name" [:utf-8],
                        "age" [:utf-8]},
             "animals" {}}})

(fact "outline"
  (outline
   [{:name "zoo",
       :cf_defs [{:name "visitor",
                  :keyspace "zoo",
                  :column_metadata [{:name "name",
                                     :validation_class "UTF8Type"}
                                    {:name "age",
                                     :validation_class "UTF8Type"}]}]}])
  => {"zoo" {"visitor" {"name" [:utf-8]
                        "age" [:utf-8]}}})

(fact "expand column"
  (expand-column-outline "age" ^{:index_name "age" :index_type IndexType/KEYS} [:utf-8])
  => {:index_name "age",
      :index_type IndexType/KEYS,
      :name "age",
      :validation_class "UTF8Type"})



(fact "expand column-family"
  (expand-columnfamily-outline
   "zoo" "visitor"
   {"name" [:utf-8]
    "age"  [:utf-8]})
  => {:name "visitor",
      :keyspace "zoo",
      :column_metadata [{:name "age",
                         :validation_class "UTF8Type"}
                        {:name "name",
                         :validation_class "UTF8Type"}]})

(fact "expand keyspace"
  (expand-keyspace-outline
   "zoo" {"visitor" {"name" [:utf-8]
                     "age"  [:utf-8]}})
  => {:name "zoo",
      :cf_defs [{:name "visitor",
                 :keyspace "zoo",
                 :column_metadata [{:name "age",
                                    :validation_class "UTF8Type"}
                                   {:name "name",
                                    :validation_class "UTF8Type"}
                                   ]}]})


(fact "expand"
  (expand
   {"zoo" {"visitor" {"name" [:utf-8]
                      "age"  [:utf-8]}}})
  => [{:name "zoo",
       :cf_defs [{:name "visitor",
                  :keyspace "zoo",
                  :column_metadata [{:name "age",
                                     :validation_class "UTF8Type"}
                                    {:name "name",
                                     :validation_class "UTF8Type"}
                                    ]}]}])




(fact "forward and backward transforms"
  (let [m {"zoo" {"visitor" {"name" [:utf-8]
                             "age" [:utf-8]}
                  "animals" {}}}]
    (outline (expand m))
    => m))
