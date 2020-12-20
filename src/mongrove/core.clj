(ns ^{:author "Rhishikesh Joshi <rhishikesh@helpshift.com>", :doc "A clojure wrapper over the official MongoDB java driver"} mongrove.core
  (:refer-clojure :exclude [update])
  (:require
    [clojure.set :as cset]
    [clojure.tools.logging :as ctl]
    [mongrove.conversion :as conversion])
  (:import
    (com.mongodb
      Block
      MongoClientSettings
      MongoClientSettings$Builder
      ReadConcern
      ReadPreference
      ServerAddress
      WriteConcern)
    (com.mongodb.client
      FindIterable
      MongoClient
      MongoClients
      MongoCollection
      MongoCursor
      MongoDatabase)
    (com.mongodb.client.model
      IndexOptions
      Indexes
      Projections
      Sorts
      UpdateOptions)
    (com.mongodb.connection
      ClusterSettings
      ConnectionPoolSettings
      SocketSettings)
    org.bson.Document
    org.bson.conversions.Bson))


(declare ->server-address ->projections)


(def ^{:private true :doc "Default Mongo Client Opts"}
  default-opts
  {:read-preference :primary
   :read-concern :majority
   :write-concern :majority
   :retry-reads false
   :retry-writes false
   :connections-per-host 100
   :socket-timeout 100000 ;ms
   :connect-timeout 60000 ;ms
   :max-connection-wait-time 60000 ;ms
   })


(def ^:private read-preference-map
  "Map of all valid ReadPreference."
  {:primary (ReadPreference/primary)
   :secondary (ReadPreference/secondary)
   :secondary-preferred (ReadPreference/secondaryPreferred)
   :primary-preferred (ReadPreference/primaryPreferred)
   :nearest (ReadPreference/nearest)})


(def ^:private read-concern-map
  "Map of all valid ReadConcerns."
  {:available ReadConcern/AVAILABLE
   :default ReadConcern/DEFAULT
   :linearizable ReadConcern/LINEARIZABLE
   :local ReadConcern/LOCAL
   :majority ReadConcern/MAJORITY
   :snapshot ReadConcern/SNAPSHOT})


(def ^:private write-concern-map
  "Map of all valid WriteConcerns."
  {;; WriteConcern names that are being used in 3.x MongoDB
   :unacknowledged WriteConcern/UNACKNOWLEDGED
   :acknowledged WriteConcern/ACKNOWLEDGED
   :majority WriteConcern/MAJORITY
   :journal-safe WriteConcern/JOURNALED
   :w1 WriteConcern/W1
   :w2 WriteConcern/W2
   :w3 WriteConcern/W3


   ;; WriteConcern names that are valid in 2.6 but deprecated in 3.x MongoDB
   :safe WriteConcern/ACKNOWLEDGED
   :replicas-safe WriteConcern/W2
   :normal WriteConcern/UNACKNOWLEDGED
   :none WriteConcern/UNACKNOWLEDGED
   :fsync-safe WriteConcern/JOURNALED
   :ack WriteConcern/ACKNOWLEDGED})


(definline ^:private clean
  "Remove the :_id key from a DB result document.
   Macro for performance."
  [op]
  `(let [result# ~op]
     (if (seq? result#)
       (map (fn [x#] (dissoc x# :_id)) result#)
       (dissoc result# :_id))))


(defn- m-cursor-iterate
  "Build a lazy-seq using cursor object and applies transform-fn.
  Keywordize-fields is a flag which is set to true using empty-query function"
  [cursor keywordize-fields]
  (if (.hasNext ^java.util.Iterator cursor)
    (lazy-seq (cons (conversion/from-bson-document (.next ^java.util.Iterator cursor)
                                                   keywordize-fields)
                    (m-cursor-iterate cursor keywordize-fields)))
    ;; Note: close is a void function and it returns nil. Because of
    ;; that it still follows seq abstraction. close is used here to
    ;; immediately close the cursor when its result-set is exhausted.
    (.close ^MongoCursor cursor)))


(defn ^SocketSettings socket-settings
  "Initialize a SocketSettings object from given options map.
  Available options : :connect-timeout :socket-timeout"
  [^MongoClientSettings$Builder builder
   {:keys [connect-timeout socket-timeout] :as opts}]
  (let [socket-block (reify Block
                       (apply
                         [this socket-builder]
                         (doto socket-builder
                           (.connectTimeout connect-timeout
                                            java.util.concurrent.TimeUnit/MILLISECONDS)
                           (.readTimeout socket-timeout
                                         java.util.concurrent.TimeUnit/MILLISECONDS))))]
    (.applyToSocketSettings builder socket-block)))


(defn ^ClusterSettings cluster-settings
  "Initialize a ClusterSettings object from given options map.
  Available options : :hosts"
  [^MongoClientSettings$Builder builder
   {:keys [hosts] :as opts}]
  (let [cluster-block (reify Block
                        (apply
                          [this cluster-builder]
                          (.hosts cluster-builder (map ->server-address hosts))))]
    (.applyToClusterSettings builder cluster-block)))


(defn ^ConnectionPoolSettings connection-pool-settings
  "Initialize a ConnectionPoolSettings object from given options map.
  Available options : :connections-per-host :max-connection-wait-time"
  [^MongoClientSettings$Builder builder
   {:keys [connections-per-host max-connection-wait-time] :as opts}]
  (let [pool-block (reify Block
                     (apply
                       [this pool-builder]
                       (doto pool-builder
                         (.maxSize connections-per-host)
                         (.maxWaitTime max-connection-wait-time
                                       java.util.concurrent.TimeUnit/MILLISECONDS))))]
    (.applyToConnectionPoolSettings builder pool-block)))


(defn ^MongoClientSettings client-settings
  "Initialize a ConnectionPoolSettings object from given options map.
  Available options : :read-preference :read-concern :write-concern
  :retry-reads :retry-writes"
  [{:keys [read-preference read-concern write-concern
           retry-reads retry-writes] :as opts}]
  {:pre [(or (nil? read-preference)
             (read-preference-map read-preference))
         (or (nil? read-concern)
             (read-concern-map read-concern))
         (or (nil? write-concern)
             (write-concern-map write-concern))]}
  (let [opts (merge default-opts opts)
        {:keys [read-preference read-concern write-concern
                retry-reads retry-writes]} opts
        builder (doto (MongoClientSettings/builder)
                  (socket-settings opts)
                  (cluster-settings opts)
                  (connection-pool-settings opts)
                  (.readConcern (get read-concern-map read-concern))
                  (.writeConcern (get write-concern-map write-concern))
                  (.readPreference (get read-preference-map read-preference))
                  (.retryWrites retry-writes)
                  ;; @TODO : Documentation states that this method
                  ;; exists ! yet we get method not found exception
                  #_(.retryReads retry-reads))]
    (.build builder)))


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
  [^MongoClient client]
  (conversion/from-bson-document (seq (.listDatabases client)) true))


(defn ^:public-api get-database-names
  "Get all database names available in the given mongo server."
  [^MongoClient client]
  (seq (.listDatabaseNames client)))


(defn ^:public-api ^MongoCollection get-collection
  "Get the collection object from given db"
  ([^MongoDatabase db ^String coll write-concern]
   {:pre [(write-concern-map write-concern)]}
   (.withWriteConcern (.getCollection db coll) ^WriteConcern (get write-concern-map write-concern)))
  ([^MongoDatabase db ^String coll]
   (get-collection db coll :majority)))


(defn ^:public-api get-collection-names
  "Returns names of all the collections for the given db"
  [^MongoDatabase db]
  (seq (.listCollectionNames db)))


(defn ^:public-api drop-collection
  "Drop the given collection."
  [^MongoDatabase db ^String coll]
  (let [collection (get-collection db coll)]
    (.drop collection)))


(defn ^:public-api drop-database
  "Drop the given database."
  [^MongoDatabase db]
  (.drop db))


(defn ^:public-api insert
  "Insert a document into the database. If inserting in bulk, provide a vector of
   maps and set multi? as true."
  [^MongoDatabase db ^String coll docs & {multi? :multi? write-concern :write-concern
                                          :or {multi? false write-concern :majority}}]
  {:pre [(or (nil? write-concern)
             (write-concern-map write-concern))]}
  (let [collection (get-collection db coll write-concern)
        bson-docs (conversion/to-bson-document docs)]
    (if multi?
      (.insertMany collection bson-docs)
      (.insertOne collection bson-docs))))


(defn ^:public-api fetch-one
  "Fetch a single document depending on query.
   Optionally return (or exclude) only a subset of the fields."
  [^MongoDatabase db ^String coll query & {:keys [only exclude]
                         :or {only [] exclude []}}]
  (let [collection (get-collection db coll)
        bson-query (conversion/to-bson-document query)
        iterator (doto ^FindIterable (.find ^MongoCollection collection bson-query)
                   (.projection (->projections only exclude)))]
    (clean (conversion/from-bson-document (.first ^FindIterable iterator) true))))


(defn ^:public-api query
  "Perform an arbitrary query on a collection.
   Optionally sort, limit, paginate, fetch a subset of fields.
  Note: Beware of the queries where limit > batch-size which can lead to a
  situation where 1 batch is received and processed and if for another batch
  if mongod server goes down at the same time, then cursor is lost and query
  will fail with the Cursor exception"
  [^MongoDatabase db ^String coll query & {:keys [sort-by limit only exclude skip one?
                                batch-size]
                         :or {skip 0 limit 10 one? false only []
                              exclude [] batch-size 10000}}]
  (let [collection (get-collection db coll)
        bson-query (conversion/to-bson-document query)
        sort (when sort-by
                      (reduce-kv #(assoc %1 %2 (int %3)) {} sort-by))
        iterator (doto ^FindIterable (.find ^MongoCollection collection bson-query)
                   (.projection (->projections only exclude))
                   (.sort (conversion/to-bson-document sort))
                   (.limit (if one? 1 limit))
                   (.skip skip)
                   (.batchSize ^int batch-size))
        cursor (.cursor iterator)]
    (clean (m-cursor-iterate cursor true))))


(defn ^:public-api count-docs
  "Count documents in a collection.
   Optionally take a query."
  [^MongoDatabase db ^String coll query]
  (let [collection ^MongoCollection (get-collection db coll)
        bson-query (conversion/to-bson-document query)]
    (.countDocuments collection bson-query)))


(defn ^:public-api delete
  "Delete a document from the collection that matches `query`.
  wc is the write-concern which should be a key from write-concern-map and is optional.
  Else the default write-concern is used."
  [^MongoDatabase db ^String coll query & {write-concern :write-concern
                                           :or {write-concern :majority}}]
  {:pre [(or (nil? write-concern)
             (write-concern-map write-concern))]}
  (let [collection (get-collection db coll write-concern)
        bson-query (conversion/to-bson-document query)]
    (.deleteMany collection bson-query)))


(defn ^:public-api update
  "Update one or more documents with given document depending on query.
   Optionally upsert."
  [^MongoDatabase db ^String coll query doc & {upsert? :upsert? multi? :multi? write-concern :write-concern
                                               :or {upsert? false multi? false write-concern :majority}}]
  {:pre [(or (nil? write-concern)
             (write-concern-map write-concern))]}
  (let [collection ^MongoCollection (get-collection db coll write-concern)
        bson-query (conversion/to-bson-document query)
        bson-update (conversion/to-bson-document doc)
        update-options (.upsert (UpdateOptions.) upsert?)]
    (if multi?
      (.updateMany collection bson-query bson-update update-options)
      (.updateOne collection bson-query bson-update update-options))))


(defn ^:public-api create-index
  "Ensure that the given index on the collection exists.
   Inexpensive if the index already exists.
   index-spec is map like : {:field 1 :another-field -1}
   1 indicates ascending index, -1 indicated descending index
   To ensure order of the fields in a compound index,
   always use an array map like : (array-map :field 1 :another-field -1)
   Supports option :unique (boolean) => creates a unique index"
  ([^MongoDatabase db ^String coll index-spec]
   (create-index db coll index-spec nil))
  ([^MongoDatabase db ^String coll index-spec options]
   (let [collection (get-collection db coll)
         allowed-options (select-keys options [:unique])
         indexes (group-by second index-spec)
         ascending (map (comp name first) (get indexes 1))
         descending (map (comp name first) (get indexes -1))
         index ^Bson (Indexes/compoundIndex [(Indexes/ascending ascending)
                                             (Indexes/descending descending)])]
     (if (seq allowed-options)
       (.createIndex collection
                     index
                     (.unique (IndexOptions.)
                              (:unique allowed-options)))
       (.createIndex collection
                     index)))))


(defn ^:public-api get-indexes
  "Get indexes for a given collection"
  [^MongoDatabase db ^String coll]
  (let [collection (get-collection db coll)
        iterator (.listIndexes collection)
        cursor (.cursor iterator)]
    (clean (m-cursor-iterate cursor true))))

;;;
;;; Util
;;;


(defn- ->server-address
  "Construct a ServerAddress object from the given spec."
  [{host :host port :port}]
  {:pre [(string? host) (integer? port)]}
  (ServerAddress. ^String host ^int port))


(defn- ->projections
  "Take the includes and (optionally) excludes vector and convert
   them into org.bson.conversions.Bson"
  [includes & [excludes]]
  {:pre [(sequential? includes) (sequential? excludes)]
   :post [(instance? Bson %)]}
  (let [include-names (map name includes)
        exclude-names (map name excludes)]
    ;; We cannot mix and match including and excluding fields, mongo does not
    ;; allow this. If only is specified, it will take precedence over exclude
    (if (seq include-names)
      (Projections/fields [(Projections/include include-names)])
      (Projections/fields [(Projections/exclude exclude-names)]))))


;; API usage

(comment
  (def client (connect :replica-set [{:host "shiva.local"
                                      :port 27017
                                      :opts {:read-preference :primary}}]))
  (def test-db (get-db client "test_driver"))

  (def mongo-coll "mongo")

  (ctl/info nil (query test-db mongo-coll {} :sort-by {:age 1}))

  (count-docs test-db mongo-coll {:age {:$lt 10}})

  (count-docs test-db mongo-coll {})

  (doseq [i (range 10)]
    (insert test-db mongo-coll {:id i
                                :name (str "rhishikesh-user-" i)
                                :age (rand-int 20)
                                :dob (java.util.Date.)} :multi? false))

  (fetch-one test-db mongo-coll {:id 3} :only [:name])

  (delete test-db mongo-coll {:age {:$gt 10}})

  (update test-db mongo-coll {:age {:$lt 10}} {:$inc {:age 1}})

  (create-index test-db mongo-coll (array-map :a 1 :b -1) nil)

  (get-indexes test-db mongo-coll)

  )
