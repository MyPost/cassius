(defproject au.com.auspost/cassius "0.1.14-SNAPSHOT"
  :description "Cassandra as a Big Nested Map"
  :url "https://git.npe.apdm.local/core-tools/cassius"
  :license {:name "Apache License - v2.0"
              :url "http://www.apache.org/licenses/LICENSE-2.0.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [im.chit/ribol "0.4.0"]
                 [im.chit/hara.namespace.import "2.1.0"]
                 [com.taoensso/nippy "2.5.2"]
                 [com.eaio.uuid/uuid "3.2"]
                 [com.stuartsierra/component "0.2.1"]
                 [com.taoensso/timbre "3.1.6"]
                 [org.apache.commons/commons-pool2 "2.2"]
                 [org.apache.cassandra/cassandra-all "2.0.9"]]
  :profiles {:dev {:dependencies [[midje "1.6.3"]]
                   :plugins [[lein-midje "3.1.1"]]}}
  :documentation {:files {"docs/index"
                         {:input "test/midje_doc/cassius_guide.clj"
                          :title "cassius"
                          :sub-title "Cassandra as a big nested map"
                          :author "Chris Zheng"
                          :email  "chris.zheng@auspost.com.au"}}})