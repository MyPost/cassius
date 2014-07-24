(ns cassius.types.byte-buffer
  (:require [taoensso.nippy :as nippy]
            [cassius.protocols :refer :all])
  (:import
   [org.apache.cassandra.db.marshal
    UTF8Type Int32Type IntegerType AsciiType AbstractType
    BytesType DecimalType DoubleType LongType FloatType UUIDType LexicalUUIDType DateType
    BooleanType CompositeType ListType MapType SetType EmptyType]
   [java.nio ByteBuffer]
   [com.eaio.uuid UUID UUIDGen]))

(defn hexify "Convert byte sequence to hex string"
  [coll]
  (let [hex [\0 \1 \2 \3 \4 \5 \6 \7 \8 \9 \a \b \c \d \e \f]]
      (letfn [(hexify-byte [b]
        (let [v (bit-and b 0xFF)]
          [(hex (bit-shift-right v 4)) (hex (bit-and v 0x0F))]))]
        (apply str (mapcat hexify-byte coll)))))

(defn unhexify "Convert hex string to byte sequence" [s]
      (letfn [(unhexify-2 [c1 c2]
                 (unchecked-byte
                  (+ (bit-shift-left (Character/digit c1 16) 4)
                     (Character/digit c2 16))))]
     (map #(apply unhexify-2 %) (partition 2 s))))

(extend-protocol ByteBufferable
  (Class/forName "[B")
  (to-bbuff [b]
    (ByteBuffer/wrap b))

  ByteBuffer
  (to-bbuff [b] b)

  String
  (to-bbuff [s]
    (.decompose UTF8Type/instance s))

  Boolean
  (to-bbuff [b]
    (.decompose BooleanType/instance b))

  Long
  (to-bbuff [l]
    (.decompose LongType/instance l))

  Integer
  (to-bbuff [i]
    (.decompose Int32Type/instance i))

  BigInteger
  (to-bbuff [i]
    (.decompose IntegerType/instance i))

  BigDecimal
  (to-bbuff [i]
    (.decompose DecimalType/instance i))

  Double
  (to-bbuff [d]
    (.decompose DoubleType/instance d))

  Float
  (to-bbuff [f]
    (.decompose FloatType/instance f))

  java.util.Date
  (to-bbuff [d]
    (.decompose DateType/instance d))

  java.util.UUID
  (to-bbuff [u]
    (.decompose UUIDType/instance u))

  clojure.lang.Keyword
  (to-bbuff [k]
    (to-bbuff (name k)))

  clojure.lang.PersistentVector
  (to-bbuff [v]
    (ByteBuffer/wrap (byte-array (unhexify (first v)))))

  Object
  (to-bbuff [o]
    (-> o nippy/freeze-to-bytes ByteBuffer/wrap))

  nil
  (to-bbuff [b]
    (.decompose EmptyType/instance b)))

(defn compose
  [type-instance b]
  (try (.compose ^AbstractType type-instance
                 (ByteBuffer/wrap b))
       (catch org.apache.cassandra.serializers.MarshalException e
         (from-bytes b :default))))

(defmethod from-bytes java.nio.ByteBuffer [b _]
  (java.nio.ByteBuffer/wrap b))

(defmethod from-bytes :utf-8 [b _] (compose UTF8Type/instance b))
(defmethod from-bytes :ascii [b _] (compose AsciiType/instance b))
(defmethod from-bytes String [b _] (from-bytes b :utf-8))

(defmethod from-bytes :long [b _] (compose LongType/instance b))
(defmethod from-bytes Long [b _] (from-bytes b :long))

(defmethod from-bytes :float  [b _] (compose FloatType/instance b))
(defmethod from-bytes Float [b _] (from-bytes b :float))

(defmethod from-bytes :double [b _] (compose DoubleType/instance b))
(defmethod from-bytes Double [b _] (from-bytes b :double))

(defmethod from-bytes :int [b _] (compose Int32Type/instance b))
(defmethod from-bytes Integer [b _] (from-bytes b :int))

(defmethod from-bytes :bigint [b _] (compose IntegerType/instance b))
(defmethod from-bytes BigInteger [b _] (from-bytes b :bigint))

(defmethod from-bytes :bigdec [b _] (compose DecimalType/instance b))
(defmethod from-bytes BigInteger [b _] (from-bytes b :bigdec))

(defmethod from-bytes :boolean [b _] (compose BooleanType/instance b))
(defmethod from-bytes Boolean [b _] (from-bytes b :boolean))

(defmethod from-bytes :date [b _] (compose DateType/instance b))
(defmethod from-bytes java.util.Date [b _] (from-bytes b :date))

(defmethod from-bytes :uuid [b _] (compose UUIDType/instance b))
(defmethod from-bytes :lexical-uuid [b _] (from-bytes b :uuid))
(defmethod from-bytes java.util.UUID [b _] (from-bytes b :uuid))

(defmethod from-bytes :keyword [_  b] (keyword (from-bytes :utf-8 b)))
(defmethod from-bytes clojure.lang.Keyword [b _] (from-bytes b :keyword))

(defmethod from-bytes :time-uuid [b _] (UUID. (from-bytes :utf-8 b)))

(defmethod from-bytes :object [b _] (nippy/thaw-from-bytes b))
(defmethod from-bytes Object [b _] (from-bytes b :object))

(defmethod from-bytes :raw [b _]
  [(hexify (seq b))])

(defmethod from-bytes :default [b _] b)
