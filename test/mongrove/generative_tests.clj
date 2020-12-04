(ns mongrove.generative-tests
  (:require
    [clj-time.core :as ct]
    [clojure.spec.alpha :as s]
    [clojure.test :refer :all]
    [clojure.test.check.clojure-test :refer [defspec]]
    [clojure.test.check.generators :as gen]
    [clojure.tools.logging :as ctl]
    [com.gfredericks.test.chuck.clojure-test :refer [checking]]
    [mongrove.core :as mcore]
    [mongrove.generative.data :as data-gen]
    [mongrove.generative.mongo :as gen-mongo]
    [mongrove.generative.query :as qgen]
    [mongrove.generative.scalar-generators :as sgen]
    [mongrove.generative.update :as ugen])
  (:import
    java.util.UUID
    (org.bson.types
      ObjectId)))


(defn insert-test
  [mongo-new mongo-old dbname coll data]
  (gen-mongo/insert-data mongo-new dbname coll data)
  (gen-mongo/insert-data mongo-old dbname coll data)
  (let [db-data (gen-mongo/get-all-data mongo-new dbname coll)]
    (is (= 1 (count db-data)) "insert 1 document only")
    (is (= db-data
           (gen-mongo/get-all-data mongo-old dbname coll)) "insert equal documents")))


(defn query-test
  [mongo-new mongo-old dbname coll query]
  (ctl/debug "Running the query : " query)
  (let [result-new (gen-mongo/run-query mongo-new dbname coll query)
        result-old (gen-mongo/run-query mongo-old dbname coll query)]
    (is (= result-new result-old))))


(defn update-test
  [mongo-new mongo-old dbname coll query update-doc]
  (let [old-result-new (gen-mongo/run-query mongo-new dbname coll query)
        old-result-old (gen-mongo/run-query mongo-old dbname coll query)]
    (ctl/debug "Old results "
               (map #(select-keys % [:oid :age]) old-result-old)
               (map #(select-keys % [:oid :age]) old-result-new))
    (gen-mongo/run-update mongo-new dbname coll query update-doc {})
    (gen-mongo/run-update mongo-old dbname coll query update-doc {})
    (let [result-new (gen-mongo/run-query mongo-new dbname coll query)
          result-old (gen-mongo/run-query mongo-old dbname coll query)]
      (ctl/debug "New results "
                 (map #(select-keys % [:oid :age]) result-old)
                 (map #(select-keys % [:oid :age]) result-new))
      (is (= old-result-new old-result-old))
      (is (= result-new result-old)))))


(comment

  (def mongo-new (gen-mongo/connect [{:host "localhost"
                                      :port 27017
                                      :opts {:read-preference :primary}}]))


  (def mongo-old (gen-mongo/connect [{:host "shiva.local"
                                      :port 27017
                                      :opts {:read-preference :primary}}]))

  ;; Using test.spec for specifying shape of the data
  (def spec-for-boolean (s/with-gen #(instance? Boolean %)
                          #(gen/boolean)))

  (defn gen-back-date
    []
    (gen/fmap #(ct/minus (ct/now) (ct/hours %)) (gen/choose 0 24)))

  (defn gen-forward-date
    []
    (gen/fmap #(ct/plus (ct/now) (ct/hours %)) (gen/choose 0 24)))

  (def spec-for-back-date
    (s/with-gen #(instance? org.joda.time.DateTime %) gen-back-date))

  (def spec-for-forward-date
    (s/with-gen #(instance? org.joda.time.DateTime %) gen-forward-date))

  (def spec-for-long-int (s/with-gen #(instance? Long %)
                           #(gen/large-integer)))

  (s/def ::first-name (s/and string?
                             #(seq %)
                             #(> 10 (count %))))
  (s/def ::last-name string?)
  (s/def ::authenticated spec-for-boolean)
  (s/def ::age (s/int-in 90 100))

  (s/def ::dob spec-for-back-date)

  (s/def ::sat-score (s/double-in :min 0 :max 5))
  (s/def ::ranks (s/coll-of (s/int-in 1 10) :kind vector? :max-count 10))
  (s/def ::entropy spec-for-long-int)
  (s/def ::id (s/with-gen #(instance? java.util.UUID %)
                (fn []
                  gen/uuid)))
  (s/def ::sid (s/with-gen #(instance? ObjectId %)
                 sgen/gen-objectId))
  (s/def ::nothing (s/with-gen #(instance? org.bson.BsonNull %)
                     sgen/gen-null))
  (s/def ::cities (s/coll-of #{:pune :mumbai :delhi :jaipur :vadodra :ahmedabad
                               :phoenix :san-jose :detroit}
                             :kind vector?
                             :min-count 1
                             :distinct true))
  (def object-ids (set (for [_ (range 100)]
                         (.toString (java.util.UUID/randomUUID)))))
  (s/def ::oid (s/with-gen string?
                 (fn []
                   (gen/fmap #(first (shuffle %)) (gen/return object-ids)))))
  (s/def ::addresses (s/coll-of string? :kind vector? :max-count 10))

  (s/def ::friend (s/keys :req-un [::first-name ::last-name ::age]))

  (s/def ::friends (s/coll-of ::friend :kind vector?))

  (s/def ::expiry spec-for-forward-date)
  (s/def ::doi spec-for-back-date)
  (s/def ::passport-info (s/keys :req-un [::id ::doi ::expiry ::cities]))

  (s/def ::person (s/keys :req-un [::first-name ::last-name ::dob
                                   ::authenticated ::id ::oid ::cities ::friends
                                   ::passport-info ::age ::ranks]
                          :opt-un [::sat-score ::entropy
                                   ::sid ::nothing ::addresses]))

  (data-gen/insert-data-from-spec mongo-new "some-db" "some-coll" ::person 4)

  (gen-mongo/get-all-data mongo-new "some-db" "some-coll")
  )


(deftest mongo-insert-tests
  (let [mongo-new (gen-mongo/connect [{:host "localhost"
                                       :port 27017
                                       :opts {:read-preference :primary}}])
        mongo-old (gen-mongo/connect [{:host "shiva.local"
                                       :port 27017
                                       :opts {:read-preference :primary}}])
        dbname (str "helshift_mongo_test_" (UUID/randomUUID))
        coll "test-insert"
        data-count 100]
    (checking "Insert" data-count
              [data (s/gen ::person)]
              (insert-test mongo-new mongo-old dbname coll data)
              (mcore/drop-database (mcore/get-db mongo-new dbname))
              (mcore/drop-database (mcore/get-db mongo-old dbname)))))


(deftest mongo-query-tests
  (let [mongo-new (gen-mongo/connect [{:host "localhost"
                                       :port 27017
                                       :opts {:read-preference :primary}}])
        mongo-old (gen-mongo/connect [{:host "shiva.local"
                                       :port 27017
                                       :opts {:read-preference :primary}}])
        dbname (str "helshift_mongo_test_" (UUID/randomUUID))
        coll "test-query"
        run-count 10
        data-count 100]
    (checking "Query : { field: { $eq: id-value } }" run-count
              [query (qgen/gen-query-from-spec :$eq ::oid)
               data (gen/vector (s/gen ::person) data-count)]
              (gen-mongo/bulk-insert-data mongo-new dbname coll data)
              (gen-mongo/bulk-insert-data mongo-old dbname coll data)
              (query-test mongo-new mongo-old dbname coll query)
              (mcore/drop-database (mcore/get-db mongo-new dbname))
              (mcore/drop-database (mcore/get-db mongo-old dbname)))
    (checking "Query : { field: { $gte: int-value } }" run-count
              [query (qgen/gen-query-from-spec :$gte ::age)
               data (gen/vector (s/gen ::person) data-count)]
              (gen-mongo/bulk-insert-data mongo-new dbname coll data)
              (gen-mongo/bulk-insert-data mongo-old dbname coll data)
              (query-test mongo-new mongo-old dbname coll query)
              (mcore/drop-database (mcore/get-db mongo-new dbname))
              (mcore/drop-database (mcore/get-db mongo-old dbname)))
    (checking "Query : { field: { $gt: int-value } }" run-count
              [query (qgen/gen-query-from-spec :$gt ::age)
               data (gen/vector (s/gen ::person) data-count)]
              (gen-mongo/bulk-insert-data mongo-new dbname coll data)
              (gen-mongo/bulk-insert-data mongo-old dbname coll data)
              (query-test mongo-new mongo-old dbname coll query)
              (mcore/drop-database (mcore/get-db mongo-new dbname))
              (mcore/drop-database (mcore/get-db mongo-old dbname)))
    (checking "Query : { field: { $lte: date-value } }" run-count
              [query (qgen/gen-query-from-spec :$lte ::dob)
               data (gen/vector (s/gen ::person) data-count)]
              (gen-mongo/bulk-insert-data mongo-new dbname coll data)
              (gen-mongo/bulk-insert-data mongo-old dbname coll data)
              (query-test mongo-new mongo-old dbname coll query)
              (mcore/drop-database (mcore/get-db mongo-new dbname))
              (mcore/drop-database (mcore/get-db mongo-old dbname)))
    (checking "Query : { field: { $lt: date-value } }" run-count
              [query (qgen/gen-query-from-spec :$lt ::dob)
               data (gen/vector (s/gen ::person) data-count)]
              (gen-mongo/bulk-insert-data mongo-new dbname coll data)
              (gen-mongo/bulk-insert-data mongo-old dbname coll data)
              (query-test mongo-new mongo-old dbname coll query)
              (mcore/drop-database (mcore/get-db mongo-new dbname))
              (mcore/drop-database (mcore/get-db mongo-old dbname)))
    (checking "Query : { field: { $ne: array-value } }" run-count
              [query (qgen/gen-query-from-spec :$ne ::cities)
               data (gen/vector (s/gen ::person) data-count)]
              (gen-mongo/bulk-insert-data mongo-new dbname coll data)
              (gen-mongo/bulk-insert-data mongo-old dbname coll data)
              (query-test mongo-new mongo-old dbname coll query)
              (mcore/drop-database (mcore/get-db mongo-new dbname))
              (mcore/drop-database (mcore/get-db mongo-old dbname)))
    (checking "Query : { field {:$size value}}" run-count
              [query (qgen/gen-query-from-spec :$size ::cities)
               data (gen/vector (s/gen ::person) data-count)]
              (gen-mongo/bulk-insert-data mongo-new dbname coll data)
              (gen-mongo/bulk-insert-data mongo-old dbname coll data)
              (query-test mongo-new mongo-old dbname coll query)
              (mcore/drop-database (mcore/get-db mongo-new dbname))
              (mcore/drop-database (mcore/get-db mongo-old dbname)))))


(deftest mongo-logical-query-tests
  (let [mongo-new (gen-mongo/connect [{:host "localhost"
                                       :port 27017
                                       :opts {:read-preference :primary}}])
        mongo-old (gen-mongo/connect [{:host "shiva.local"
                                       :port 27017
                                       :opts {:read-preference :primary}}])
        dbname (str "helshift_mongo_test_" (UUID/randomUUID))
        coll "test-logical"
        run-count 10
        data-count 100]
    (checking "Query : {:$or [{ field-1 {:$op1 value-1}} { field-2 {:$op2 value-2 }}]}" run-count
              [query (qgen/gen-logical-query-from-spec :$or [{:op :$gt
                                                              :spec ::age}
                                                             {:op :$lt
                                                              :spec ::dob}])
               data (gen/vector (s/gen ::person) data-count)]
              (gen-mongo/bulk-insert-data mongo-new dbname coll data)
              (gen-mongo/bulk-insert-data mongo-old dbname coll data)
              (query-test mongo-new mongo-old dbname coll query)
              (mcore/drop-database (mcore/get-db mongo-new dbname))
              (mcore/drop-database (mcore/get-db mongo-old dbname)))
    (checking "Query : {:$and [{ field-1 {:$op1 value-1 }} { field-2 {:$op2 value-2 }}]}" run-count
              [query (qgen/gen-logical-query-from-spec :$and [{:op :$lt
                                                               :spec ::age}
                                                              {:op :$exists
                                                               :spec ::sat-score}])
               data (gen/vector (s/gen ::person) data-count)]
              (gen-mongo/bulk-insert-data mongo-new dbname coll data)
              (gen-mongo/bulk-insert-data mongo-old dbname coll data)
              (query-test mongo-new mongo-old dbname coll query)
              (mcore/drop-database (mcore/get-db mongo-new dbname))
              (mcore/drop-database (mcore/get-db mongo-old dbname)))
    (let [gen-date-range (qgen/gen-logical-query-from-spec :$and
                                                           [{:op :$gt :spec ::dob}
                                                            {:op :$lt :spec ::dob}])
          date-range-spec (s/with-gen (fn [q]
                                        (ct/after? (get-in q [:dob :$lt])
                                                   (get-in q [:dob :$gt])))
                                      (fn []
                                        gen-date-range))]
      (checking "Query : { field {:$gt start-date, :$lt end-date}}" run-count
                [query (s/gen date-range-spec)
                 data (gen/vector (s/gen ::person) data-count)]
                (gen-mongo/bulk-insert-data mongo-new dbname coll data)
                (gen-mongo/bulk-insert-data mongo-old dbname coll data)
                (query-test mongo-new mongo-old dbname coll query)
                (mcore/drop-database (mcore/get-db mongo-new dbname))
                (mcore/drop-database (mcore/get-db mongo-old dbname))))
    (checking "Query : { field {:$not {:$exists value}}}" run-count
              [query (qgen/gen-not-query-from-spec :$exists ::entropy)
               data (gen/vector (s/gen ::person) data-count)]
              (gen-mongo/bulk-insert-data mongo-new dbname coll data)
              (gen-mongo/bulk-insert-data mongo-old dbname coll data)
              (query-test mongo-new mongo-old dbname coll query)
              (mcore/drop-database (mcore/get-db mongo-new dbname))
              (mcore/drop-database (mcore/get-db mongo-old dbname)))))


(deftest mongo-in-query-tests
  (let [mongo-new (gen-mongo/connect [{:host "localhost"
                                       :port 27017
                                       :opts {:read-preference :primary}}])
        mongo-old (gen-mongo/connect [{:host "shiva.local"
                                       :port 27017
                                       :opts {:read-preference :primary}}])
        dbname (str "helshift_mongo_test_" (UUID/randomUUID))
        coll "test-in-query"
        run-count 10
        data-count 100]
    (checking "Query : { field: { $in: [value, value...] } }" run-count
              [query (qgen/gen-query-from-spec :$in ::oid)
               data (gen/vector (s/gen ::person) data-count)]
              (gen-mongo/bulk-insert-data mongo-new dbname coll data)
              (gen-mongo/bulk-insert-data mongo-old dbname coll data)
              (query-test mongo-new mongo-old dbname coll query)
              (mcore/drop-database (mcore/get-db mongo-new dbname))
              (mcore/drop-database (mcore/get-db mongo-old dbname)))
    (checking "Query : { field: { $nin: [value, value...] } }" run-count
              [query (qgen/gen-query-from-spec :$nin ::cities)
               data (gen/vector (s/gen ::person) data-count)]
              (gen-mongo/bulk-insert-data mongo-new dbname coll data)
              (gen-mongo/bulk-insert-data mongo-old dbname coll data)
              (query-test mongo-new mongo-old dbname coll query)
              (mcore/drop-database (mcore/get-db mongo-new dbname))
              (mcore/drop-database (mcore/get-db mongo-old dbname)))))


(deftest mongo-update-tests
  (let [mongo-new (gen-mongo/connect [{:host "localhost"
                                       :port 27017
                                       :opts {:read-preference :primary}}])
        mongo-old (gen-mongo/connect [{:host "shiva.local"
                                       :port 27017
                                       :opts {:read-preference :primary}}])
        dbname (str "helshift_mongo_test_" (UUID/randomUUID))
        coll "test-updates"
        run-count 10
        data-count 100]
    (checking "Update : {:$inc : {field : value}} " run-count
              [query (qgen/gen-query-from-spec :$gt ::age)
               update-doc (ugen/gen-update-doc-from-spec :$inc ::age)
               data (gen/vector (s/gen ::person) data-count)]
              (gen-mongo/bulk-insert-data mongo-new dbname coll data)
              (gen-mongo/bulk-insert-data mongo-old dbname coll data)
              (update-test mongo-new mongo-old dbname coll query update-doc)
              (mcore/drop-database (mcore/get-db mongo-new dbname))
              (mcore/drop-database (mcore/get-db mongo-old dbname)))
    (checking "Update : {:$set : {field1 : value1}} " run-count
              [query (qgen/gen-query-from-spec :$eq ::oid)
               update-doc (ugen/gen-update-doc-from-spec :$set ::age)
               data (gen/vector (s/gen ::person) data-count)]
              (gen-mongo/bulk-insert-data mongo-new dbname coll data)
              (gen-mongo/bulk-insert-data mongo-old dbname coll data)
              (update-test mongo-new mongo-old dbname coll query update-doc)
              (mcore/drop-database (mcore/get-db mongo-new dbname))
              (mcore/drop-database (mcore/get-db mongo-old dbname)))
    (checking "Update : {:$set : {field1 : value1, field2 : value2...}}" run-count
              [query (qgen/gen-query-from-spec :$eq ::oid)
               update-doc (ugen/gen-update-docs-from-spec :$set [{:spec ::age}
                                                                 {:spec ::cities}
                                                                 {:spec ::first-name}])
               data (gen/vector (s/gen ::person) data-count)]
              (gen-mongo/bulk-insert-data mongo-new dbname coll data)
              (gen-mongo/bulk-insert-data mongo-old dbname coll data)
              (update-test mongo-new mongo-old dbname coll query update-doc)
              (mcore/drop-database (mcore/get-db mongo-new dbname))
              (mcore/drop-database (mcore/get-db mongo-old dbname)))
    (checking "Update : {:$unset : [field1, field2...]}" run-count
              [query (qgen/gen-query-from-spec :$eq ::oid)
               update-doc (ugen/gen-update-docs-from-spec :$unset [{:spec ::cities}
                                                                   {:spec ::addresses}])
               data (gen/vector (s/gen ::person) data-count)]
              (gen-mongo/bulk-insert-data mongo-new dbname coll data)
              (gen-mongo/bulk-insert-data mongo-old dbname coll data)
              (update-test mongo-new mongo-old dbname coll query update-doc)
              (mcore/drop-database (mcore/get-db mongo-new dbname))
              (mcore/drop-database (mcore/get-db mongo-old dbname)))
    (checking "Update : {:$addToSet : {field1 : [value1, value2...]}} " run-count
              [query (qgen/gen-query-from-spec :$gt ::age)
               update-doc (ugen/gen-update-doc-from-spec :$addToSet ::ranks)
               data (gen/vector (s/gen ::person) data-count)]
              (gen-mongo/bulk-insert-data mongo-new dbname coll data)
              (gen-mongo/bulk-insert-data mongo-old dbname coll data)
              (update-test mongo-new mongo-old dbname coll query update-doc)
              (mcore/drop-database (mcore/get-db mongo-new dbname))
              (mcore/drop-database (mcore/get-db mongo-old dbname)))))


(deftest mongo-push-pull-tests
  (let [mongo-new (gen-mongo/connect [{:host "localhost"
                                       :port 27017
                                       :opts {:read-preference :primary}}])
        mongo-old (gen-mongo/connect [{:host "shiva.local"
                                       :port 27017
                                       :opts {:read-preference :primary}}])
        dbname (str "helshift_mongo_test_" (UUID/randomUUID))
        coll "test-updates"
        run-count 10
        data-count 100]
    (checking "Update : {:$pull {:field {:$op value}}}" run-count
              [query (qgen/gen-query-from-spec :$gt ::age)
               update-doc (ugen/gen-update-doc-from-spec :$pull
                                                         ::ranks
                                                         :condition
                                                         {:op :$in
                                                          :spec ::cities})
               data (gen/vector (s/gen ::person) data-count)]
              (gen-mongo/bulk-insert-data mongo-new dbname coll data)
              (gen-mongo/bulk-insert-data mongo-old dbname coll data)
              (update-test mongo-new mongo-old dbname coll query update-doc)
              (mcore/drop-database (mcore/get-db mongo-new dbname))
              (mcore/drop-database (mcore/get-db mongo-old dbname)))
    (checking "Update : {:$pullAll {:field [value1, value2...]}}" run-count
              [query (qgen/gen-query-from-spec :$gt ::age)
               update-doc (ugen/gen-update-doc-from-spec :$pullAll ::ranks)
               data (gen/vector (s/gen ::person) data-count)]
              (gen-mongo/bulk-insert-data mongo-new dbname coll data)
              (gen-mongo/bulk-insert-data mongo-old dbname coll data)
              (update-test mongo-new mongo-old dbname coll query update-doc)
              (mcore/drop-database (mcore/get-db mongo-new dbname))
              (mcore/drop-database (mcore/get-db mongo-old dbname)))
    (checking "Update : {:$push {:field [value1, value2...]}}" run-count
              [query (qgen/gen-query-from-spec :$eq ::oid)
               update-doc (ugen/gen-update-doc-from-spec :$push ::cities)
               data (gen/vector (s/gen ::person) data-count)]
              (gen-mongo/bulk-insert-data mongo-new dbname coll data)
              (gen-mongo/bulk-insert-data mongo-old dbname coll data)
              (update-test mongo-new mongo-old dbname coll query update-doc)
              (mcore/drop-database (mcore/get-db mongo-new dbname))
              (mcore/drop-database (mcore/get-db mongo-old dbname)))
    (checking "Update : {:$push {:field {:$each [value1, value2...] :$position pos}}}" run-count
              [query (qgen/gen-query-from-spec :$eq ::oid)
               update-doc (ugen/gen-update-doc-from-spec :$push ::cities :each? true :position 2)
               data (gen/vector (s/gen ::person) data-count)]
              (gen-mongo/bulk-insert-data mongo-new dbname coll data)
              (gen-mongo/bulk-insert-data mongo-old dbname coll data)
              (update-test mongo-new mongo-old dbname coll query update-doc)
              (mcore/drop-database (mcore/get-db mongo-new dbname))
              (mcore/drop-database (mcore/get-db mongo-old dbname)))))
