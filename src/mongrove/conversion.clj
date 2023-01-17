(ns mongrove.conversion
  (:import
    (clojure.lang
      IPersistentMap
      Keyword
      Named
      Ratio)
    (com.mongodb
      BasicDBList
      DBObject
      DBRef)
    (java.util
      Date
      List
      Map
      Set)
    org.bson.Document
    (org.bson.types
      Decimal128)))


(defprotocol ConvertToBsonDocument

  (^org.bson.Document to-bson-document
    [input]
    "Converts given piece of Clojure data to BasicDBObject MongoDB Java driver uses"))


(extend-protocol ConvertToBsonDocument
  nil
  (to-bson-document [input]
    nil)

  String
  (to-bson-document [^String input]
    input)

  Boolean
  (to-bson-document [^Boolean input]
    input)

  java.util.Date
  (to-bson-document [^java.util.Date input]
    input)

  java.time.LocalDate
  (to-bson-document [^java.time.LocalDate input]
    input)

  java.time.LocalDateTime
  (to-bson-document [^java.time.LocalDateTime input]
    input)

  java.time.Instant
  (to-bson-document [^java.time.Instant input]
    input)

  Ratio
  (to-bson-document [^Ratio input]
    (double input))

  Keyword
  (to-bson-document [^Keyword input] (.getName input))

  Named
  (to-bson-document [^Named input] (.getName input))

  IPersistentMap
  (to-bson-document [^IPersistentMap input]
    (let [o (Document.)]
      (doseq [[k v] input]
        (.append o (to-bson-document k) (to-bson-document v)))
      o))

  List
  (to-bson-document [^List input] (map to-bson-document input))

  Set
  (to-bson-document [^Set input] (map to-bson-document input))

  Object
  (to-bson-document [input]
    input))


(defprotocol ConvertFromBsonDocument

  (from-bson-document
    [input keywordize]
    "Converts given DBObject instance to a piece of Clojure data"))


(extend-protocol ConvertFromBsonDocument
  nil
  (from-bson-document [input keywordize] input)

  Object
  (from-bson-document [input keywordize] input)

  Decimal128
  (from-bson-document [^Decimal128 input keywordize]
    (.bigDecimalValue input))

  List
  (from-bson-document [^List input keywordize]
    (vec (map #(from-bson-document % keywordize) input)))

  BasicDBList
  (from-bson-document [^BasicDBList input keywordize]
    (vec (map #(from-bson-document % keywordize) input)))

  DBRef
  (from-bson-document [^DBRef input keywordize]
    input)

  Document
  (from-bson-document [^Document input keywordize]
    (reduce (if keywordize
              (fn [m ^String k]
                (assoc m (keyword k) (from-bson-document (.get input k) true)))
              (fn [m ^String k]
                (assoc m k (from-bson-document (.get input k) false))))
            {} (.keySet input))))
