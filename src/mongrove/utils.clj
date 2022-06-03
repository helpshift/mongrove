(ns ^{:author "Rhishikesh Joshi <rhishikesh@helpshift.com>", :doc "Mongo utils required by the API namespaces"} mongrove.utils
  (:require
    [clojure.set :as cset]
    [clojure.tools.logging :as ctl]
    [mongrove.conversion :as conversion])
  (:import
    (com.mongodb
      Block
      ClientSessionOptions
      ClientSessionOptions$Builder
      MongoClientSettings
      MongoClientSettings$Builder
      ReadConcern
      ReadPreference
      ServerAddress
      TransactionOptions
      TransactionOptions$Builder
      WriteConcern)
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


(defn ->server-address
  "Construct a ServerAddress object from the given spec."
  [{host :host port :port}]
  {:pre [(string? host) (integer? port)]}
  (ServerAddress. ^String host ^int port))


(defn ->projections
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


(def ^{:doc "Default Mongo Client Opts"}
  default-opts
  {:read-preference :primary
   :read-concern :majority
   :write-concern :majority
   :retry-reads false
   :retry-writes false
   :connections-per-host 100
   :socket-timeout 100000 ; ms
   :connect-timeout 60000 ; ms
   :max-connection-wait-time 60000 ; ms
   })


(def read-preference-map
  "Map of all valid ReadPreference."
  {:primary (ReadPreference/primary)
   :secondary (ReadPreference/secondary)
   :secondary-preferred (ReadPreference/secondaryPreferred)
   :primary-preferred (ReadPreference/primaryPreferred)
   :nearest (ReadPreference/nearest)})


(def read-concern-map
  "Map of all valid ReadConcerns."
  {:available ReadConcern/AVAILABLE
   :default ReadConcern/DEFAULT
   :linearizable ReadConcern/LINEARIZABLE
   :local ReadConcern/LOCAL
   :majority ReadConcern/MAJORITY
   :snapshot ReadConcern/SNAPSHOT})


(def write-concern-map
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


(definline clean
  "Remove the :_id key from a DB result document.
   Macro for performance."
  [op]
  `(let [result# ~op]
     (if (seq? result#)
       (map (fn [x#] (dissoc x# :_id)) result#)
       (dissoc result# :_id))))


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
