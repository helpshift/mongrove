(ns ^{:author "Rhishikesh Joshi <rhishikeshj@gmail.com>", :doc "A clojure wrapper over the official MongoDB java reactive driver"} mongrove.reactive
  (:refer-clojure :exclude [update])
  (:require
    [clojure.core.async :as async]
    [clojure.set :as cset]
    [clojure.tools.logging :as ctl]
    [mongrove.conversion :as conversion]
    [mongrove.core :as core])
  (:import
    (com.mongodb
      WriteConcern)
    (com.mongodb.client.model
      CreateCollectionOptions)
    (com.mongodb.reactivestreams.client
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
        settings (core/client-settings opts)
        conn (MongoClients/create settings)]
    conn))


(defmethod connect :direct
  [_ server-spec]
  (let [hosts [(select-keys server-spec [:host :port])]
        opts (assoc (:opts server-spec {})
                    :hosts hosts)
        settings (core/client-settings opts)
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
   {:pre [(core/write-concern-map write-concern)]}
   (.withWriteConcern (.getCollection db coll)
                      ^WriteConcern (get core/write-concern-map write-concern)))
  ([^MongoDatabase db ^String coll]
   (get-collection db coll :majority)))


(defn subcriber
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
  (def insert-one-publisher (.insertOne test-coll (conversion/to-bson-document {:id :reactive-2})))

  (let [b-coll (get-collection test-db "b")
        docs (mapv conversion/to-bson-document [{:id :reactive-arr-4}
                                               {:id :reactive-arr-5}
                                               {:id :reactive-arr-6}])]
    (def insert-many-publisher (.insertMany b-coll docs)))


  (def mongo-chan (subcriber insert-many-publisher))
  (async/<!! mongo-chan)


  )
