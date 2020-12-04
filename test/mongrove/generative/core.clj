(ns mongrove.generative.core
  (:require
    [clj-time.core :as ct]
    [clojure.spec.alpha :as s]
    [clojure.test.check.generators :as gen]
    [mongrove.generative.data :as data-gen]
    [mongrove.generative.mongo :as gen-mongo]
    [mongrove.generative.query :as qgen]
    [mongrove.generative.scalar-generators :as sgen]
    [mongrove.generative.update :as ugen])
  (:import
    (org.bson.types
      ObjectId)))


(defonce test-db "gen-test-db")

(defonce test-collection "gen-test-collection")


(defn run-query
  "Run queries for operator.
  `coll` : collection
  `op` : operator
  `type` : one of the scalar-types here : `scalar-type->generator`
  `field` : field in the object. Ideally should be one of the fields in `domain-object-fields`"
  [conn db coll op type field]
  (let [query (gen/generate (qgen/gen-query op type field))]
    (prn "Running query : " query)
    (gen-mongo/run-query conn db coll query)))


(defn run-query-from-spec
  "Run queries for operator.
  `coll` : collection
  `op` : operator
  `field-spec` : spec which defines the shape of data"
  [conn db coll op field-spec]
  (let [queries (gen/generate (qgen/gen-query-from-spec op field-spec))]
    (prn "Running queries : " queries)
    (if (seq? queries)
      (map #(gen-mongo/run-query conn coll %)
           queries)
      (gen-mongo/run-query conn db coll queries))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Insert data

(comment
  (def mongo4 (gen-mongo/connect [{:host "localhost"
                                   :port 27017
                                   :opts {:read-preference :primary}}]))


  (def mongo3 (gen-mongo/connect [{:host "shiva.local"
                                   :port 27017
                                   :opts {:read-preference :primary}}]))

  (gen-mongo/delete-all-data mongo4 test-db test-collection)
  (gen-mongo/get-all-data mongo4 test-db test-collection)

  ;; Using test.spec for specifying shape of the data
  (def spec-for-boolean (s/with-gen #(instance? Boolean %)
                          #(gen/boolean)))
  (defn gen-date
    []
    (gen/fmap #(ct/minus (ct/now) (ct/hours %)) (gen/choose 0 24)))

  (def spec-for-date
    (s/with-gen #(instance? org.joda.time.DateTime %) gen-date))

  (def spec-for-long-int (s/with-gen #(instance? Long %)
                           #(gen/large-integer)))

  (s/def ::first-name (s/and string?
                             #(seq %)
                             #(> 10 (count %))))
  (s/def ::last-name string?)
  (s/def ::authenticated spec-for-boolean)
  (s/def ::age (s/int-in 90 100))
  (s/def ::dob spec-for-date)
  (s/def ::sat-score (s/double-in :min 0 :max 5))
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
  (s/def ::addresses (s/coll-of string? :kind vector? :max-count 10))

  (s/def ::friend (s/keys :req-un [::first-name ::last-name ::age]))

  (s/def ::friends (s/coll-of ::friend :kind vector?))

  (s/def ::expiry spec-for-date)
  (s/def ::doi spec-for-date)
  (s/def ::passport-info (s/keys :req-un [::id ::doi ::expiry ::cities]))

  (s/def ::person (s/keys :req-un [::first-name ::last-name ::dob
                                   ::authenticated ::id ::cities ::friends
                                   ::passport-info ::age]
                          :opt-un [::sat-score ::entropy
                                   ::sid ::nothing ::addresses]))

  (run-query mongo4 test-db test-collection :$gt :date :dob)
  (run-query mongo4 test-db test-collection :$gt :int :age)
  (run-query mongo4 test-db test-collection :$gt :string :first-name)

  ;; Create data from a given spec
  (data-gen/gen-data-from-spec ::person 2)

  ;; insert some data into DB
  (data-gen/insert-data-from-spec test-db test-collection ::person 4)


  )

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Query data

(comment

  (run-query mongo4 test-db test-collection :$gt :date :dob)
  ;; This query may not return any data since the range of ints
  ;; is so large ! But we already have a way to constrain it !

  (run-query mongo4 test-db test-collection :$eq :int :age)
  (run-query mongo4 test-db test-collection :$gt :string :first-name)
  (run-query mongo4 test-db test-collection :$exists :string :first-name)

  ;; Let's use specs !
  (run-query-from-spec mongo4 test-db test-collection :$eq ::age)

  (run-query-from-spec mongo4 test-db test-collection :$gt ::addresses)
  (run-query-from-spec mongo4 test-db test-collection :$gt ::passport-info)


  (run-query-from-spec mongo4 test-db test-collection :$size ::passport-info)
  )

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; $in, $nin

(comment
  (run-query mongo4 test-db test-collection :$in :int :age)

  (run-query-from-spec mongo4 test-db test-collection :$in ::age)

  (run-query-from-spec mongo4 test-db test-collection :$in ::passport-info)

  (run-query-from-spec mongo4 test-db test-collection :$nin ::cities)
  )


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; $and, $or, $nor

(defn run-logical-query
  [conn db coll op exprs]
  (let [query (gen/generate (qgen/gen-logical-query op exprs))]
    (prn "Running query : " query)
    (gen-mongo/run-query conn db coll query)))


(defn run-logical-query-from-spec
  [conn db coll op specs]
  (let [query (gen/generate (qgen/gen-logical-query-from-spec op specs))]
    (prn "Running query : " query)
    (gen-mongo/run-query conn db coll query)))


(defn run-not-query
  [conn db coll op type field]
  (let [query (gen/generate (qgen/gen-not-query op type field))]
    (prn "Running query : " query)
    (gen-mongo/run-query conn db coll query)))


(defn run-not-query-from-spec
  [conn db coll op spec]
  (let [query (gen/generate (qgen/gen-not-query-from-spec op spec))]
    (prn "Running query : " query)
    (gen-mongo/run-query conn db coll query)))


(comment
  (def exprs* [{:op :$gt, :type :int, :field :age} {:op :$eq, :type :date, :field :dob}])
  (run-logical-query mongo4 "" test-collection :$nor exprs*)

  (def field-specs* [{:op :$gt
                      :spec ::dob}
                     {:op :$lt
                      :spec ::dob}])
  (run-logical-query-from-spec mongo4 test-db test-collection :$and field-specs*)

  (run-not-query-from-spec mongo4 test-db test-collection :$gt ::age)

  (run-not-query mongo4 test-db test-collection :$gt :int :age)
  )


;; Next we generate a query to apply the update on
;; This query can come from the gen-query mechanisms

(defn run-update
  [conn
   db
   coll
   {qop :op qtype :type qfield :field :as query-info}
   {:keys [op type field each? position condition] :as update-info}]
  (let [query (qgen/gen-query qop qtype qfield)
        update-doc (ugen/gen-update-doc op type field
                                        :each? each?
                                        :position position
                                        :condition condition)
        update-options (ugen/gen-update-options)]
    (prn query update-doc update-options)
    (gen-mongo/run-update conn db coll query update-doc update-options)))


(defn run-update-from-spec
  [conn
   db
   coll
   {qop :op qspec :spec :as query-info}
   {:keys [op spec each? position condition] :as update-info}]
  (let [query (gen/generate (qgen/gen-query-from-spec qop qspec))
        update-doc (gen/generate (ugen/gen-update-doc-from-spec op spec
                                                                :each? each?
                                                                :position position
                                                                :condition condition))
        update-options (ugen/gen-update-options)]
    (prn query update-doc update-options)
    (gen-mongo/run-update conn db coll query update-doc update-options)))


(defn run-updates-from-specs
  [conn
   db
   coll
   {qop :op qspec :spec :as query-info}
   update-op
   update-specs]
  (let [query (gen/generate (qgen/gen-query-from-spec qop qspec))
        update-docs (gen/generate (ugen/gen-update-docs-from-spec update-op update-specs))
        update-options (ugen/gen-update-options)]
    (prn query {update-op update-docs} update-options)
    (gen-mongo/run-update conn db coll query {update-op update-docs} update-options)))


(comment
  (run-update mongo4
              test-db
              test-collection
              {:op :$gt :type :int :field :age}
              {:op :$inc :type :int :field :age})

  ;; $push / $addToSet
  (run-update mongo4
              test-db
              test-collection
              {:op :$gt :type :int :field :age}
              {:op :$push :type :int :field :age :each? false :position 0})


  (run-update-from-spec mongo4
                        test-db
                        test-collection
                        {:op :$gt :spec ::age}
                        {:op :$inc :spec ::age})
  ;; $addToSet
  (run-update-from-spec mongo4
                        test-db
                        test-collection
                        {:op :$gt :spec ::age}
                        {:op :$addToSet :spec ::cities
                         :each? true
                         :position 0})

  ;; $push
  (run-update-from-spec mongo4
                        test-db
                        test-collection
                        {:op :$gt :spec ::age}
                        {:op :$push :spec ::cities
                         :each? true
                         :position 0})


  (run-update-from-spec mongo4
                        test-db
                        test-collection
                        {:op :$gt :spec ::age}
                        {:op :$pullAll :spec ::cities})


  ;; $pull
  (run-update-from-spec mongo4
                        test-db
                        test-collection
                        {:op :$gt :spec ::age}
                        {:op :$pull
                         :spec ::passport-info
                         :condition {:op :$in
                                     :spec ::cities}})

  ;; $set
  (run-update-from-spec mongo4
                        test-db
                        test-collection
                        {:op :$gt :spec ::age}
                        {:op :$set :spec ::cities})

  ;; $unset
  (run-update-from-spec mongo4
                        test-db
                        test-collection
                        {:op :$gt :spec ::age}
                        {:op :$unset :spec ::first-name})

  ;; $set multiple fields
  (run-updates-from-specs mongo4
                          test-db
                          test-collection
                          {:op :$gt :spec ::age}
                          :$set
                          [{:spec ::age}
                           {:spec ::cities}
                           {:spec ::first-name}])

  )
