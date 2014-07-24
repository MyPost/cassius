# cassius

[![Build Status](https://travis-ci.org/MyPost/cassius.png?branch=master)](https://travis-ci.org/MyPost/cassius)

Cassandra as a big nested map.

## Installation

In your `project.clj` file, add to your dependencies:

```clojure
[au.com.auspost/cassius "0.1.13"]
```

## Overview

Cassius is a clojure wrapper around cassandra's thrift interface. It treats cassandra as a big mutable nested hashmap and provides the following abstractions:

 - cassandra data and schema can be represented as values and clojure maps.
 - keyspaces, column families, rows and columns can be abstracted as nested map layers
 - supercolumns are just one extra level of nesting
 
The library has been used for both mocking and for higher level abstractions on top of cassandra. An ORM has been built and used internally at MyPost to deal with legacy cassandra data.

## Motivation

A lot of ideas of cassius were gleaned from reading source code, mostly from [casyn](https://github.com/mpenet/casyn) and [clj-hector](https://github.com/pingles/clj-hector).

The library was motivated by an inability to reason about what changes the existing monolithic system was doing to the underlying database. In order to move away from the existing system into more agile architecture, the team had to be careful about reworking features without breaking functionality. The typical work of code migration would end up looking something like this:

   1. Get the current state of the DB
   2. Run some legacy code (java)
   3. See what has changed in the DB
   4. Rewrite that change in clojure

`cassius` was designed as a tool for developers to reason about and test changes to cassandra. It also has the following features:

 - Written in thrift in order to support supercolumns in the legacy system.
 - Abstracts the database in such a way that tests are easy to write and easy to read.
 - Uses of conditional restarts for more control of error states.
 - Option to use the component/Lifecycle framework. Mock DB is included for testing purposes.
 - Uses the Hashmap as a protocol, defined in `cassius.protocols/IMap`.
    - Methods: `put-in`, `peek-in`, `keys-in`, `drop-in`, `set-in`, `select-in` and `mutate-in`

## Usage

#### From Scratch

Cassius can be a little bit overpowered if the developer is not careful. Make sure to backup important data when playing with around with the library:

```clojure
(use 'cassius.core)

(def db (connect "localhost" 9160)) ;; dbects via the thrift interface
(drop-in db) ;; WARNING!!! clears the entire database 
(peek-in db) ;; => {}, brand new database
```

#### Adding and Retrieving Data

Starting with an empty database, we can put data into cassandra using `put-in`, as well as look at the state of cassandra using `peek-in`:

```clojure
(put-in  db {"app" {"user" {"1" {"age"  "10"}}}})
(peek-in db ["app" "user"])
;; => {"1" {"age" "10"}}

(put-in  db {"app" {"user" {"1" {"name"  "Andy"}}}})
(peek-in db)
;; => {"app" {"user" {"1" {"age" "10" "name"  "Andy"}}}}
```

We can use a vector to set nested structures to place value at any depth:

```clojure
(put-in db ["app" "user"]
        {"3"
         {"age"  "30" "name" "Chris"}})

(peek-in db)
;; => {"app" {"user" {"1" {"age" "10" "name" "Andy"}, 
                      "3" {"age" "30" "name" "Chris"}}}}

(put-in  db ["app" "user" "2"] {"age"  "20" "name" "Bob"})
(peek-in db ["app" "user"])
;; => {"app" {"user" {"1" {"age" "10" "name" "Andy"}, 
;;                    "2" {"age" "20" "name" "Bob"}, 
;;                    "3" {"age" "30" "name" "Chris"}}}}
````

`keys-in` will list the keys at that nesting level

```clojure
(keys-in db ["app" "user"])
;; => ["1" "2" "3"]

(keys-in db ["app" "user" "1"])
;; => ["age" "name"]
```

#### Deleting

`drop-in` is used to delete data:

```clojure
(drop-in db ["app" "user" "3" "age"])
(peek-in db ["app" "user" "3"])
;; => {"name" "Chris"}}}

(drop-in db ["app" "user" "2"])
(peek-in db ["app" "user"])
;; =>  {"1" {"age" "10" "name" "Andy"}, 
;;      "3" {"name" "Chris"}}
```

#### Updating

Two functions - `put-in` and `set-in` - allow manipulation of cassandra state. The use of `put-in` can be seen earlier while `set-in` actually sets the value of the data at that level of nesting. `set-in` will drop any data within that level of nesting

```clojure
(set-in db ["app" "user"]
         {"4" {"name" "Dave" "age"  "40"}})

(peek-in db)
=> {"app" {"user" {"4" {"name" "Dave" "age" "40"}}}}
```

## Patching and Rollback
Because of the simplicity with which cassandra data is represented, it is easy to manipulate data based upon patching and rolling back db changes:

Initialize database and set up instance i0:

```clojure
(def i0
  (-> db
      (drop-in)
      (peek-in)))
```

`put-in` some data and and set up instance i1:

```clojure
(def i1
  (-> db
      (put-in ["zoo" "kee"]
              {"A" {"stage" "1"}
               "B" {"stage" "1"}}))
```

`set-in` some data and and set up instance i2:

```clojure
(def i2
  (-> db
      (set-in ["zoo" "kee"]
              {"C" {"stage" "2"}
               "D" {"stage" "2"}})
    (peek-in)))
```

`put-in` some more data and and set up instance i3:

```clojure
(def i3
  (-> db
      (put-in ["zoo" "kee"]
              {"A" {"stage" "3"}
               "B" {"stage" "3"}})
      (peek-in)))
```

#### Diff

We can use the diff function to compute differences between instances. d01, d12 and d23 are defined in this way:

```clojure
(def d01 (diff i0 i1))

d01
;; => {:+ {["zoo"] {"kee" {"A" {"stage" "1"}, "B" {"stage" "1"}}}}}
```

We do the same for `d12` and `d13`

```clojure
(def d12 (diff i1 i2))
d12
;; => {:- {["zoo" "kee" "B"] {"stage" "1"},
;;         ["zoo" "kee" "A"] {"stage" "1"}},
;;     :+ {["zoo" "kee" "D"] {"stage" "2"},
;;         ["zoo" "kee" "C"] {"stage" "2"}}}

(def d23 (diff i2 i3))
d23
;; => {:+ {["zoo" "kee" "B"] {"stage" "3"},
aa         ["zoo" "kee" "A"] {"stage" "3"}}}
```

#### Patching

Starting with an empty database, we can now reconstruct each instance in time by applying patches:

```clojure
(-> db
    (drop-in)
    (peek-in))
;; => {}
```

Applying the patch d01 will transition cassandra from i0 to i1:

```clojure
(-> db
    (patch d01)
    (peek-in))
;; => {"zoo" {"kee" {"A" {"stage" "1"},
;;                   "B" {"stage" "1"}}}}
```

Applying the rollback d01 will transition cassandra from i1 to i0:

```clojure
(-> conn
    (rollback d01)
    (peek-in))
;; => {}
```

Chaining patches d01, d12 and d23 in order will restore cassandra to i3:

```clojure
(-> conn
    (patch d01)
    (patch d12)
    (peek-in))
;; => {"zoo" {"kee" {"C" {"stage" "2"}
;;                   "D" {"stage" "2"}}}}

(-> conn
    (patch d23)
    (peek-in))
;; => {"zoo" {"kee" {"A" {"stage" "3"},
;;                   "B" {"stage" "3"},
;;                   "C" {"stage" "2"},
;;                   "D" {"stage" "2"}}}}
```

We can confirm that there is no difference between i3 and the current state of cassandra.

```clojure
(diff i3 (peek-in conn))
;;=> nil
```

## Mocking

TBD

## TODOs
 
 - Write Examples for Mocking and Testing 
 - Native Filtering and Search

## Contributors

 - Chris Zheng     (Australia Post)
 - Sergey Marakhov (Australia Post)
 - Tushar Pokle    (Australia Post)

## License

Copyright © 2014 Australia Postal Corporation

Distributed under the Apache License v2.0
