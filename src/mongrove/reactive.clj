(ns ^{:author "Rhishikesh Joshi <rhishikeshj@gmail.com>", :doc "A clojure wrapper over the official MongoDB java reactive driver"} mongrove.reactive
  (:refer-clojure :exclude [update])
  (:require
    [clojure.core.async :as async]
    [clojure.set :as cset]
    [clojure.tools.logging :as ctl]
    [mongrove.conversion :as conversion]
    [mongrove.core :as core]
    [mongrove.utils :as utils :refer [client-settings write-concern-map ->projections]])
  (:import
    (com.mongodb
      WriteConcern)
    (com.mongodb.client.model
      CreateCollectionOptions)
    (com.mongodb.reactivestreams.client
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


(defmacro apply-if
  "Evaluates function fn with first and args, if (some? (first args)) returns true,
  else returns the value of first arg"
  [first fn & args]
  `(if (some? (first (list ~@args)))
     (apply ~fn ~first (list ~@args))
     ~first))


(comment
  ;; this does not work yet!
  (let [default-opts {:capped? false}]
   (defn ^CreateCollectionOptions create-collection-options
     [{:keys [capped?
              size
              max-docs
              expire-after
              collation-opts
              storage-engine-opts
              time-series-opts
              validation-opts] :as opts}]
     {:pre []}
     (let [opts (merge default-opts opts)
           {:keys [capped?
                   size
                   max-docs
                   expire-after
                   collation-opts
                   storage-engine-opts
                   time-series-opts
                   validation-opts]} opts]
       (-> (CreateCollectionOptions.)
           ;; But if we make this a macro, it does not work!
           (apply-if .capped capped?)
           (apply-if .collation collation-opts)
           (apply-if .expireAfter expire-after java.util.concurrent.TimeUnit/MILLISECONDS)
           (apply-if .maxDocuments max-docs)
           (apply-if .sizeInBytes size)
           (apply-if .storageEngineOptions storage-engine-opts)
           (apply-if .timeSeriesOptions time-series-opts)
           (apply-if .validationOptions validation-opts))))))


(defn create-collection
  [^MongoDatabase db ^String coll ^CreateCollectionOptions opts]
  (.subscribe (.createCollection db coll opts)))


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


(defn ^:public-api ^MongoCollection get-collection
  "Get the collection object from given db"
  ([^MongoDatabase db ^String coll write-concern]
   {:pre [(write-concern-map write-concern)]}
   (.withWriteConcern (.getCollection db coll)
                      ^WriteConcern (get write-concern-map write-concern)))
  ([^MongoDatabase db ^String coll]
   (get-collection db coll :majority)))


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
                       (.request subscription buf-size)
                       (println "onSubscribe: " subscription))

                     (onNext
                       [this result]
                       (println "onNext: " result)
                       (async/>!! ch result))

                     (onError
                       [this t]
                       (println "onError: " t)
                       (async/close! ch))

                     (onComplete
                       [this]
                       (println "onComplete")
                       (async/close! ch)))]
    (.subscribe publisher subscriber)
    ch))


(defn basic-subcriber
  [publisher onNext onComplete onError]
  (let [subscriber (reify Subscriber
                     (onSubscribe
                       [this subscription]
                       (.request subscription Integer/MAX_VALUE)
                       (println "onSubscribe: " subscription))

                     (onNext
                       [this result]
                       (println "onNext: " result)
                       (onNext result))

                     (onError
                       [this t]
                       (println "onError: " t)
                       (onError t))

                     (onComplete
                       [this]
                       (println "onComplete")
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
  (def insert-one-publisher (.insertOne test-coll (conversion/to-bson-document {:id :reactive-2})))
  (basic-subcriber insert-one-publisher
                   #(println "client next: " %)
                   #(println "client complete")
                   #(println "client error: " %))

  ;; insert-many valuesubscriber
  (let [b-coll (get-collection test-db "b")
        docs (mapv conversion/to-bson-document [{:name :reactive-arr-42}
                                                {:name :reactive-arr-53}
                                                {:name :reactive-arr-64}])
        p (value-subcriber (.insertMany b-coll docs))]
    (println "Documents inserted : " @p))


  ;; insert-many chan-subscriber
  (let [b-coll (get-collection test-db "b")
        docs (mapv conversion/to-bson-document [{:name :reactive-arr-42}
                                                {:name :reactive-arr-53}
                                                {:name :reactive-arr-64}])
        mongo-chan (chan-subcriber (.insertMany b-coll docs))]
    (println "Got results on channel: " (async/<!! mongo-chan)))


  ;; count docs
  (let [b-coll (get-collection test-db "b")
        c (value-subcriber (.countDocuments b-coll))]
    (println "Number of documents is " @c))

  ;; query
  (let [collection (get-collection test-db "b")
        only [:name]
        exclude []
        sort-by {:name 1}
        skip 0
        batch-size 10
        limit 2
        sort (when sort-by
               (reduce-kv #(assoc %1 %2 (int %3)) {} sort-by))
        bson-query (conversion/to-bson-document {})
        iterator (doto ^FindPublisher
                     (.find ^MongoCollection collection bson-query)
                   (.projection (->projections only exclude))
                   (.sort (conversion/to-bson-document sort))
                   (.limit limit)
                   (.skip skip)
                   (.batchSize batch-size))
        c (chan-subcriber iterator)
        docs (async/into [] c)]
    (println "Documents are " (async/<!! docs)))

  ;; fetch-one
  (let [collection (get-collection test-db "b")
        only [:name]
        exclude []
        bson-query (conversion/to-bson-document {:name :reactive-arr-53})
        iterator (doto ^FindPublisher
                     (.find ^MongoCollection collection bson-query)
                   (.projection (->projections only exclude))
                   (.first))
        c (value-subcriber iterator)]
    (println "Document is " (conversion/from-bson-document @c true)))
  )
