(ns mongrove.generative.scalar-generators
  (:require
    [clj-time.coerce :as tc]
    [clj-time.core :as time]
    [clojure.java.io :as io]
    [clojure.test.check.generators :as gen])
  (:import
    org.bson.BsonNull
    (org.bson.types
      Decimal128
      ObjectId)))

;; BSON types
;; numerics : double, int-32, long-64, decimal-128
;; others : string, object, array, Binary data, objectID
;; boolean, date, null, regex, javascript, symbol, timestamp,
;; minKey, maxKey
;;
;; Covered : null, double, long, int, boolean, string,
;; date, array, object, binData (uuid), decimal, objectID

;; Not doing
;; timestamp -> this is for internal use according to mongo-docs
;; javascript -> not doing : https://stackoverflow.com/questions/39155290/what-is-javascript-with-scope-in-mongodb

(def scalar-types
  #{:null :int :long :double :decimal
    :string :uuid :date :timestamp :boolean :objectId})

;; Generators for scalar BSON types

(defn gen-null
  [& _]
  (gen/return (BsonNull.)))


(defn gen-int
  ([]
   gen/small-integer)
  ([spec]
   (if (and (:min spec) (:max spec))
     (gen/choose (:min spec) (:max spec))
     gen/small-integer)))


(defn gen-predictable-int
  [& _]
  (gen/choose -10 10))


(defn gen-double
  ([]
   gen/double)
  ([spec]
   (gen/double* spec)))


(defn gen-predictable-double
  [& _]
  (gen/elements (for [x (range -10 10)
                      y (range -15 15)]
                  (if (= y 0)
                    0
                    (double (/ x y))))))


(defn gen-long
  ([]
   gen/large-integer)
  ([spec]
   (gen/large-integer* spec)))


;; This is just one output of gen/large-integer captured here !
(let [longs #{0 -227 -25789 -545 1 39 4517084524 186050149 -126 -1 15 125599954 -761 -37612091189
              4012429 -152 -971 -60480 -34 -3 -17 6 -31 -471 3 5122 -1703739 82 57 -296393994836
              14823 14 1975 -3770 2578 62341854 1704 29470 -5 -56063}]
  (defn gen-predictable-long
    [& _]
    (gen/elements longs)))


(defn gen-decimal
  ([]
   (gen/fmap #(Decimal128. ^long %) (gen-long)))
  ([spec]
   (gen/fmap #(Decimal128. ^long %) (gen-long spec))))


(defn gen-predictable-decimal
  [& _]
  (gen/fmap #(Decimal128. ^long %) (gen-predictable-long)))


(defn gen-string
  ([]
   gen/string-ascii)
  ([spec]
   (gen/fmap #(subs % 0 (min (or (:size spec)
                                 Long/MAX_VALUE)
                             (count %)))
             gen/string-ascii)))


;; Put whatever strings you want in the following file
(let [strings (with-open [rdr (io/reader (io/resource "tests/strings.clj"))]
                (reduce conj [] (line-seq rdr)))]
  (defn gen-predictable-string
    [& _]
    (gen/elements strings)))


(defn gen-objectId
  [& _]
  (gen/return (ObjectId.)))


(def gen-timestamp
  (gen/fmap #(+ % (tc/to-long (time/now)))
            (gen/choose 100000000 200000000)))


(let [important-dates #{(tc/from-string "2014-11-26T12:34:56")
                        (tc/from-string "2018-04-14T12")
                        (tc/from-string "2020-02-01T12:34:56")
                        (tc/from-string "2015-01-14T12:34:56")}]
  (def gen-predictable-timestamp
    (gen/fmap tc/to-long (gen/elements important-dates))))


(def gen-date
  (gen/fmap #(tc/from-long %) gen-timestamp))


(def gen-predictable-date
  (gen/fmap #(tc/from-long %) gen-predictable-timestamp))


(let [type-spec->gen-map {:null (partial gen-null)
                          :int (partial gen-predictable-int)
                          :double (partial gen-predictable-double)
                          :decimal (partial gen-predictable-decimal)
                          :long (partial gen-predictable-long)
                          :string (partial gen-predictable-string)
                          :uuid (fn [& _] gen/uuid)
                          :date (fn [& _] gen-predictable-date)
                          :timestamp (fn [& _] gen-predictable-timestamp)
                          :boolean (fn [& _] gen/boolean)
                          :objectId (partial gen-objectId)}]
  (defn scalar-type->generator
    ([type]
     ((type-spec->gen-map type)))
    ([type spec]
     ((type-spec->gen-map type) spec))))
