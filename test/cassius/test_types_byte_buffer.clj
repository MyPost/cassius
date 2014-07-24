(ns cassius.test-types-byte-buffer
  (:use midje.sweet)
  (:require
   [cassius.protocols :refer :all]
   [cassius.types.byte-buffer]
   :reload))

(fact "to-bbuff and from-bbuff"
  (from-bbuff (to-bbuff "abcd") String)
  => "abcd"

  (from-bbuff (to-bbuff "abcd") Integer)
  => 1633837924)

(fact "to-bytes"
  (->> (to-bytes "abcd")
       seq
       (map char))
  => [\a \b \c \d])

(fact "vectors are special"
  (from-bytes (to-bytes ["1234ef"]) :raw)
  => ["1234ef"])
