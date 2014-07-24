(ns cassius.test-types-coerce
  (:use midje.sweet)
  (:require [cassius.types.coerce :refer :all]))

(fact "coerce-string turns a string into the same type"

  (coerce-string "false" Boolean)
  => false

  (coerce-string "1" Short)
  => 1)


(fact "coerce turns an object into any type. Used for coercing maps into thrift ByteBuffers and back again."
  (coerce "abcd" String)
  => "abcd")
