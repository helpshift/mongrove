(ns ^{:author "Rhishikesh Joshi <rhishikeshj@gmail.com>", :doc "A clojure wrapper over the official MongoDB java reactive driver"} mongrove.reactive
  (:refer-clojure :exclude [update])
  (:require
    [clojure.core.async :as async]
    [clojure.set :as cset]
    [clojure.tools.logging :as ctl]
    [mongrove.conversion :as conversion]
    [mongrove.core :as core]
    [mongrove.utils :as utils :refer [client-settings
                                      write-concern-map
                                      ->projections]])
  (:import
    (com.mongodb
      WriteConcern)
    (com.mongodb.client.model
      CreateCollectionOptions
      IndexOptions
      Indexes
      UpdateOptions)
    (com.mongodb.reactivestreams.client
      ClientSession
      FindPublisher
      MongoClient
      MongoClients
      MongoCollection
      MongoDatabase)
    org.bson.Document
    org.bson.conversions.Bson
    (org.reactivestreams
      Publisher
      Subscriber)))


(defmulti connect
  "Initialize the MongoDB Connection. Mongo Client Opts, if any, are taken from
  the first server-spec. Ref. `client-settings' fn for a list of default opts
  that are applied to the Mongo Client."
  {:arglists '([:direct {:host host :port port :opts opts}]
               [:replica-set [{:host host :port port :opts opts} & more]])}
  (fn [conn-type _] (keyword conn-type)))


(defmethod connect :replica-set
  [_ server-specs]
  (let [hosts (map #(select-keys % [:host :port]) server-specs)
        opts (assoc (:opts (first server-specs) {})
                    :hosts hosts)
        settings (client-settings opts)
        conn (MongoClients/create settings)]
    conn))


(defmethod connect :direct
  [_ server-spec]
  (let [hosts [(select-keys server-spec [:host :port])]
        opts (assoc (:opts server-spec {})
                    :hosts hosts)
        settings (client-settings opts)
        conn (MongoClients/create settings)]
    conn))


(defmethod connect :default
  [_ _]
  nil)


(defn ^:public-api ^MongoDatabase get-db
  "Get the database object given a name."
  [^MongoClient client db-name]
  (.getDatabase client db-name))


(defn ^:public-api get-databases
  "Get all databases available in the given mongo server."
  ([^MongoClient client]
   (get-databases client nil))
  ([^MongoClient client ^ClientSession session]
   (conversion/from-bson-document (seq (if session
                                         (.listDatabases client session)
                                         (.listDatabases client)))
                                  true)))


(defn ^:public-api get-database-names
  "Get all database names available in the given mongo server."
  ([^MongoClient client]
   (get-database-names client nil))
  ([^MongoClient client ^ClientSession session]
   (if session
     (.listDatabaseNames client session)
     (.listDatabaseNames client))))


(defn ^:public-api ^MongoCollection get-collection
  "Get the collection object from given db"
  ([^MongoDatabase db ^String coll write-concern]
   {:pre [(write-concern-map write-concern)]}
   (.withWriteConcern (.getCollection db coll)
                      ^WriteConcern (get write-concern-map write-concern)))
  ([^MongoDatabase db ^String coll]
   (get-collection db coll :majority)))


(defn ^:public-api get-collection-names
  "Returns names of all the collections for the given db"
  ([^MongoDatabase db]
   (get-collection-names db nil))
  ([^MongoDatabase db ^ClientSession session]
   (if session
     (.listCollectionNames db session)
     (.listCollectionNames db))))


(defn ^:public-api drop-collection
  "Drop the given collection."
  ([^MongoDatabase db ^String coll]
   (drop-collection db nil coll))
  ([^MongoDatabase db ^ClientSession session ^String coll]
   (let [collection (get-collection db coll)]
     (if session
       (.drop collection session)
       (.drop collection)))))


(defn ^:public-api drop-database
  "Drop the given database."
  ([^MongoDatabase db]
   (drop-database db nil))
  ([^MongoDatabase db ^ClientSession session]
   (if session
     (.drop db session)
     (.drop db))))


(defn ^:public-api insert
  "Insert a document into the database. If inserting in bulk, provide a vector of
   maps and set multi? as true."
  [^MongoDatabase db ^String coll docs
   & {multi? :multi? write-concern :write-concern session :session
      :or {multi? false write-concern :majority}}]
  {:pre [(or (nil? write-concern)
             (write-concern-map write-concern))]}
  (let [collection (get-collection db coll write-concern)
        bson-docs (conversion/to-bson-document docs)]
    (if multi?
      (if session
        (.insertMany collection session bson-docs)
        (.insertMany collection bson-docs))
      (if session
        (.insertOne collection session bson-docs)
        (.insertOne collection bson-docs)))))


(defn ^:public-api fetch-one
  "Fetch a single document depending on query.
   Optionally return (or exclude) only a subset of the fields."
  ([^MongoDatabase db ^String coll query & {:keys [only exclude session]
                                            :or {only [] exclude []}}]
   (let [collection (get-collection db coll)
         bson-query (conversion/to-bson-document query)
         iterator (doto ^FindPublisher
                   (if session
                     (.find ^MongoCollection collection session bson-query)
                     (.find ^MongoCollection collection bson-query))
                    (.projection (->projections only exclude)))]
     (.first ^FindPublisher iterator))))


(defn ^:public-api query
  "Perform an arbitrary query on a collection.
   Optionally sort, limit, paginate, fetch a subset of fields.
  Note: Beware of the queries where limit > batch-size which can lead to a
  situation where 1 batch is received and processed and if for another batch
  if mongod server goes down at the same time, then cursor is lost and query
  will fail with the Cursor exception"
  [^MongoDatabase db ^String coll query & {:keys [sort-by limit only exclude skip one?
                                                  batch-size session]
                                           :or {skip 0 limit 10 one? false only []
                                                exclude [] batch-size 10000}}]
  (let [collection (get-collection db coll)
        bson-query (conversion/to-bson-document query)
        sort (when sort-by
               (reduce-kv #(assoc %1 %2 (int %3)) {} sort-by))
        iterator (doto ^FindPublisher
                  (if session
                    (.find ^MongoCollection collection session bson-query)
                    (.find ^MongoCollection collection bson-query))
                   (.projection (->projections only exclude))
                   (.sort (conversion/to-bson-document sort))
                   (.limit (if one? 1 limit))
                   (.skip skip)
                   (.batchSize ^int batch-size))]
    iterator))


(defn ^:public-api count-docs
  "Count documents in a collection.
   Optionally take a query."
  ([^MongoDatabase db ^String coll query]
   (count-docs db nil coll query))
  ([^MongoDatabase db ^ClientSession session ^String coll query]
   (let [collection ^MongoCollection (get-collection db coll)
         bson-query (conversion/to-bson-document query)]
     (if session
       (.countDocuments collection session bson-query)
       (.countDocuments collection bson-query)))))


(defn ^:public-api delete
  "Delete a document from the collection that matches `query`.
  wc is the write-concern which should be a key from write-concern-map and is optional.
  Else the default write-concern is used."
  [^MongoDatabase db ^String coll query & {write-concern :write-concern session :session
                                           :or {write-concern :majority}}]
  {:pre [(or (nil? write-concern)
             (write-concern-map write-concern))]}
  (let [collection (get-collection db coll write-concern)
        bson-query (conversion/to-bson-document query)]
    (if session
      (.deleteMany collection session bson-query)
      (.deleteMany collection bson-query))))


(defn ^:public-api update
  "Update one or more documents with given document depending on query.
   Optionally upsert."
  [^MongoDatabase db ^String coll query doc & {upsert? :upsert? multi? :multi? write-concern :write-concern
                                               session :session
                                               :or {upsert? false multi? false write-concern :majority}}]
  {:pre [(or (nil? write-concern)
             (write-concern-map write-concern))]}
  (let [collection ^MongoCollection (get-collection db coll write-concern)
        bson-query (conversion/to-bson-document query)
        bson-update (conversion/to-bson-document doc)
        update-options (.upsert (UpdateOptions.) upsert?)]
    (if multi?
      (if session
        (.updateMany collection session bson-query bson-update update-options)
        (.updateMany collection bson-query bson-update update-options))
      (if session
        (.updateOne collection session bson-query bson-update update-options)
        (.updateOne collection bson-query bson-update update-options)))))


(defn ^:public-api create-index
  "Ensure that the given index on the collection exists.
   Inexpensive if the index already exists.
   index-spec is map like : {:field 1 :another-field -1}
   1 indicates ascending index, -1 indicated descending index
   To ensure order of the fields in a compound index,
   always use an array map like : (array-map :field 1 :another-field -1)
   Supports option :unique (boolean) => creates a unique index"
  ([^MongoDatabase db ^String coll index-spec]
   (create-index db nil coll index-spec nil))
  ([^MongoDatabase db ^ClientSession session ^String coll index-spec]
   (create-index db session coll index-spec nil))
  ([^MongoDatabase db ^ClientSession session ^String coll index-spec options]
   (let [collection (get-collection db coll)
         allowed-options (select-keys options [:unique])
         indexes (group-by second index-spec)
         ascending (map (comp name first) (get indexes 1))
         descending (map (comp name first) (get indexes -1))
         index ^Bson (Indexes/compoundIndex [(Indexes/ascending ascending)
                                             (Indexes/descending descending)])]
     (if (seq allowed-options)
       (if session
         (.createIndex collection
                       session
                       index
                       (.unique (IndexOptions.)
                                (:unique allowed-options)))
         (.createIndex collection
                       index
                       (.unique (IndexOptions.)
                                (:unique allowed-options))))
       (if session
         (.createIndex collection
                       session
                       index)
         (.createIndex collection
                       index))))))


(defn ^:public-api get-indexes
  "Get indexes for a given collection"
  ([^MongoDatabase db ^String coll]
   (get-indexes db nil coll))
  ([^MongoDatabase db ^ClientSession session ^String coll]
   (let [collection (get-collection db coll)]
     (if session
       (.listIndexes collection session)
       (.listIndexes collection)))))


(defn chan-subcriber
  [publisher]
  (let [buf-size 10
        ch (async/chan buf-size)
        subscriber (reify Subscriber
                     (onSubscribe
                       [this subscription]
                       ;; this needs to happen in order to
                       ;; make stuff happen, but how many items to
                       ;; request is a consideration
                       (.request subscription buf-size))

                     (onNext
                       [this result]
                       (async/>!! ch result))

                     (onError
                       [this t]
                       (async/close! ch))

                     (onComplete
                       [this]
                       (async/close! ch)))]
    (.subscribe publisher subscriber)
    ch))


(defn basic-subcriber
  [publisher onNext onComplete onError]
  (let [subscriber (reify Subscriber
                     (onSubscribe
                       [this subscription]
                       (.request subscription Integer/MAX_VALUE))

                     (onNext
                       [this result]
                       (onNext result))

                     (onError
                       [this t]
                       (onError t))

                     (onComplete
                       [this]
                       (onComplete)))]
    (.subscribe publisher subscriber)))


(deftype ValueSubscriber
  [pr ^:unsynchronized-mutable value]

  Subscriber

  (onSubscribe
    [this subscription]
    (.request subscription Integer/MAX_VALUE)
    this)


  (onNext
    [this result]
    (set! value result))


  (onError
    [this t]
    (deliver pr t))


  (onComplete
    [this]
    (deliver pr value)))


(defn value-subcriber
  [publisher]
  (let [val (promise)
        subscriber (ValueSubscriber. val [])]
    (.subscribe publisher subscriber)
    val))


(comment
  (def client (connect :replica-set [{:host "localhost"
                                      :port 27017
                                      :opts {:read-preference :primary}}
                                     ;; {:host "localhost"
                                     ;;  :port 27018}
                                     ;; {:host "localhost"
                                     ;;  :port 27019}
                                     ]))
  (def test-db (get-db client "test_reactions"))

  (def test-coll (get-collection test-db "a"))
  ;; insert-one

  (def insert-one-publisher (insert test-db "a" {:id :reactive-2}))
  (basic-subcriber insert-one-publisher
                   #(println "client next: " %)
                   #(println "client complete")
                   #(println "client error: " %))

  ;; insert-many valuesubscriber
  (let [b-coll (get-collection test-db "b")
        docs (reduce #(conj %1 {:id %2
                                :name (str "user-" %2)
                                :age (rand-int 20)
                                :dob (java.util.Date.)})
                     []
                     (vec (range 10)))
        p (value-subcriber (insert test-db "b" docs :multi? true))]
    (println "Documents inserted : " @p))


  ;; insert-many chan-subscriber
  (let [b-coll (get-collection test-db "b")
        docs (reduce #(conj %1 {:id %2
                                :name (str "user-" %2)
                                :age (rand-int 20)
                                :dob (java.util.Date.)})
                     []
                     (vec (range 10)))
        mongo-chan (chan-subcriber (insert test-db "b" docs :multi? true))]
    (println "Got results on channel: " (async/<!! mongo-chan)))


  ;; count docs
  (let [c (value-subcriber (count-docs test-db "b" {}))]
    (println "Number of documents is " @c))

  ;; query
  (let [c (chan-subcriber (query test-db "b" {:name "user-3"} :only [:age] :exclude [:name]))
        docs (async/into [] c)]
    (println "Documents are " (async/<!! docs)))

  ;; fetch-one
  (let [c (value-subcriber (fetch-one test-db "b" {:name "user-4"} :exclude [:id]))]
    (println "Document is " @c))


  (let [c (value-subcriber (drop-collection test-db "a"))]
    (println "Collection a dropped " @c))

  (let [c (value-subcriber (get-collection-names test-db))]
    (println "Collections are" @c))

  (let [c (value-subcriber (get-database-names client))]
    (println "Collections are" @c))

  (let [c (value-subcriber (create-index test-db "b" (array-map :a 1 :b -1)))
        indexes (async/into [] (chan-subcriber (get-indexes test-db "b")))]
    (println "Indexes are " (async/<!! indexes)))
  )
