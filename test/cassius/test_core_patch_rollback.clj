(ns cassius.test-core-patch-rollback
  (:use midje.sweet)
  (:require [cassius.core :refer :all]))

[[:chapter {:title "Testing and Rollbacks"}]]

[[:section {:title "Initialization"}]]

"Where cassius really shines is its abstraction of the database to a map. This allows all clojure functions to be used on the output of the cassius operations."

(fact
 (def settings (read-string (slurp "settings.edn")))
 (def host (or (:host settings) "localhost"))
 (def port (or (:port settings) 9160))
 (def conn (connect host port (assoc settings :atomic true))))

(facts

  "Initialize database and set up instance `i0`:"

  (-> conn
      (drop-in)
      (peek-in))
  => {}


  (def i0 (peek-in conn))

  "`put-in` some data and and set up instance `i1`:"

  (-> conn
      (put-in ["zoo" "kee"]
              {"A" {"stage" "1"}
               "B" {"stage" "1"}})
      (peek-in))
  => {"zoo" {"kee" {"A" {"stage" "1"},
                    "B" {"stage" "1"}}}}

  (def i1 (peek-in conn))

  "`set-in` some data and and set up instance `i2`:"

  (-> conn
      (set-in  ["zoo" "kee"]
                  {"C" {"stage" "2"}
                   "D" {"stage" "2"}})
      (peek-in))
  => {"zoo" {"kee" {"C" {"stage" "2"}
                    "D" {"stage" "2"}}}}

  (def i2 (peek-in conn))

  "`put-in` some more data and and set up instance `i3`:"

  (-> conn
      (put-in ["zoo" "kee"]
               {"A" {"stage" "3"}
                "B" {"stage" "3"}})
      (peek-in))
  => {"zoo" {"kee" {"A" {"stage" "3"},
                    "B" {"stage" "3"},
                    "C" {"stage" "2"},
                    "D" {"stage" "2"}}}}

  (def i3 (peek-in conn)))


[[:section {:title "Diffs"}]]

(facts
  "We can use the `diff` function to compute differences between instances. `d01`, `d12` and `d23` are defined in this way:"

  (def d01 (diff i0 i1)

    )
  d01
  => {:+ {["zoo"] {"kee" {"A" {"stage" "1"}, "B" {"stage" "1"}}}}}

  (def d12 (diff i1 i2))
  d12
  => {:- {["zoo" "kee" "B"] {"stage" "1"},
          ["zoo" "kee" "A"] {"stage" "1"}},
      :+ {["zoo" "kee" "D"] {"stage" "2"},
          ["zoo" "kee" "C"] {"stage" "2"}}}

  (def d23 (diff i2 i3))
  d23
  => {:+ {["zoo" "kee" "B"] {"stage" "3"},
          ["zoo" "kee" "A"] {"stage" "3"}}})

[[:section {:title "Patching"}]]

(facts
  "Starting with an empty database, we can now reconstruct each instance in time by applying patches:"

  (-> conn
      (drop-in)
      (peek-in))
  => {}

  "Applying the first patch `d01`, then checking that there is no difference between the patched database and the instance `i1`:"

  (-> conn
      (patch d01)
      (peek-in))
  => {"zoo" {"kee" {"A" {"stage" "1"},
                    "B" {"stage" "1"}}}}


  (diff i1 (peek-in conn))
  => nil

  "Applying the first rollback `d01`, then checking that there is no difference between the patched database and the initial empty state `i0`:"

  (-> conn
      (rollback d01)
      (peek-in))
  => {}

  (diff i0 (peek-in conn))
  => nil

  "Chaining patches will give up back `i3` if all of them are applied in order:"

  (-> conn
      (patch d01)
      (patch d12)
      (peek-in))
  => {"zoo" {"kee" {"C" {"stage" "2"}
                    "D" {"stage" "2"}}}}

  (-> conn
      (patch d23)
      (peek-in))
  => {"zoo" {"kee" {"A" {"stage" "3"},
                    "B" {"stage" "3"},
                    "C" {"stage" "2"},
                    "D" {"stage" "2"}}}}

  (diff i3 (peek-in conn))
  => nil)

(facts "Concurrent threads with connection pool will have no errors"
  (let [errs (atom 0)]
    (->> (mapv
          #(future
             (try
               (put-in conn {"zoo" {"kee" {(str %) {"stage" (str %)}}}})
               (drop-in conn ["zoo" "kee" (str %)])
               (catch Throwable t
                 (swap! errs inc))))
          (range 1000))
         (map deref))
    @errs)0
  => 0)

(facts "Concurrent threads with no connection pool will incur errors"
  (let [conn2 (connect host port {:type :connection})
        errs  (atom 0)]
    (->> (mapv
          #(future
             (try
               (put-in conn2 {"zoo" {"kee" {(str %) {"stage" (str %)}}}})
               (drop-in conn2 ["zoo" "kee" (str %)])
               (catch Throwable t
                 (swap! errs inc))))
          (range 1000))
         (map deref))
    @errs)
  => #(not= 0 %))

;; TEARDOWN

"We complete teardown by emptying the cassandra database."

(drop-in conn)

(comment

  (def a (peek-in (connect "localhost" 9160)))
  (def b (peek-in (connect "localhost" 29160)))

  (def dab (diff a b))


  (def a1 (dissoc (get a "dmb") "ContentStorage"))
  (def b1 (dissoc (get b "dmb") "ContentStorage"))

  (def dab1 (diff a1 b1)))
