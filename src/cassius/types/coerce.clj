(ns cassius.types.coerce
  (:require [cassius.protocols :refer :all]
            [cassius.types.byte-buffer :as bb]))

(defn coerce-string [v type]
  (condp = type
    String                 v
    java.nio.ByteBuffer    (to-bbuff v)

    Boolean                (read-string v)
    Boolean/TYPE           (read-string v)

    Short                  (Short/parseShort v)
    Short/TYPE             (Short/parseShort v)

    Integer                (Integer/parseInt v)
    Integer/TYPE           (Integer/parseInt v)

    Long                   (Long/parseLong v)
    Long/TYPE              (Long/parseLong v)

    Float                  (Float/parseFloat v)
    Float/TYPE             (Float/parseFloat v)

    Double                 (Double/parseDouble v)
    Double/TYPE            (Double/parseDouble v)

    BigInteger             (BigInteger. v)
    BigDecimal             (BigDecimal. v)

    clojure.lang.Keyword   (keyword v)
    clojure.lang.Ratio     (read-string v)

    java.util.UUID         (java.util.UUID/fromString v)
    java.net.URI          (java.net.URI. v)
    java.util.Date         (read-string (str "#inst \"" v "\""))))

(defn coerce [v type]
  (cond (instance? type v) v

        (instance? java.nio.ByteBuffer v)
        (from-bbuff v type)

        (instance? (Class/forName "[B") v)
        (from-bytes v type)

        (string? v)
        (coerce-string v type)

        :else
        (condp = type
          java.nio.ByteBuffer  (to-bbuff v)
          Boolean/TYPE         (boolean v)
          Short/TYPE           (short v)
          Integer/TYPE         (int v)
          Long/TYPE            (long v)
          Double/TYPE          (double v)
          Float/TYPE           (float v)
          v)))
