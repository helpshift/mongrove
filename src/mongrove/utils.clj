(ns mongrove.utils
  (:require
    [cheshire.core :as json]
    [clojure.string :as string]
    [mongrove.conversion :as conversion]
    [mongrove.core :as mc])
  (:import
    com.mongodb.CursorType
    (com.mongodb.client
      FindIterable
      MongoClient
      MongoCollection
      MongoCursor)
    org.bson.types.BSONTimestamp))


(defn tail-oplog
  "Loop through mongodb oplog collection. Ref: https://docs.mongodb.com/manual/core/replica-set-oplog/

  Params:
  client : MongoClient connection
  handler-fn - fn to process returned rows
  Mongo Query Params:
  :ns - (optional) Sequence of namespaces
  :ts - (optional) Timestamp in seconds since epoch
  :op - (optional) Sequence of operations. Possible operations:
         c(command), d(delete), i(insert), u(update)

   Ex.
   > (tail-oplog client
                 (fn [obj]
                   (println obj))
                 :ns [\"admin.$cmd\" \"db-name.coll-name\"]
                 :op [\"c\" \"u\"]
                 :ts (int (/ (System/currentTimeMillis) 1000))"
  [^MongoClient client handler-fn & {ns :ns ts :ts op :op
                                     :or {ts (int (/ (System/currentTimeMillis)
                                                     1000))}}]
  (let [db (mc/get-db client "local")
        collection (mc/get-collection db "oplog.rs")
        query (merge {}
                     (when (seq ns)
                       {:ns {:$in ns}})
                     (when (seq op)
                       {:op {:$in op}})
                     (when ts
                       {:ts {:$gte (BSONTimestamp. ts 1)}}))
        bson-query (conversion/to-bson-document query)
        iterator (doto ^FindIterable
                  (.find ^MongoCollection collection bson-query)
                   (.noCursorTimeout true)
                   (.cursorType (CursorType/TailableAwait)))
        cursor ^MongoCursor (.cursor iterator)]
    (loop []
      (if (.hasNext cursor)
        (do
          (let [oplog (conversion/from-bson-document (.next cursor)
                                                     true)]
            (when (not= "n" (:op oplog))
              (handler-fn oplog))
            (recur)))
        (println "Empty cursor !!")))))


(defn- oplog-to-command
  "Info taken from https://www.compose.com/articles/the-mongodb-oplog-and-node-js/
  and https://docs.mongodb.com/manual/reference/command/"
  [oplog]
  (case (:op oplog)
    "i"
    (let [doc (:o oplog)
          [db-name coll-name] (string/split (:ns oplog) #"\.")]
      [db-name {:insert coll-name
                :documents [doc]}])
    "u"
    (let [doc (:o oplog)
          [db-name coll-name] (string/split (:ns oplog) #"\.")
          query (:o2 oplog)]
      [db-name {:update coll-name
                :updates [{:q query
                           :u doc}]}])

    "d"
    (let [doc (:o oplog)
          [db-name coll-name] (string/split (:ns oplog) #"\.")]
      [db-name {:delete coll-name
                :deletes [{:q doc}]}])

    nil))


(defn- parse-json
  "Parse the received input as json"
  [input]
  (try
    (json/parse-string input true)
    (catch Exception e
      (println e)
      nil)))


(defn- parse-mongoshake-oplog
  [input]
  (let [shake-map (parse-json input)
        doc (reduce (fn [a m]
                      (assoc a (:Name m)
                             (:Value m)))
                    {} (:o shake-map))]
    (assoc shake-map :o doc)))


(comment
  ;; oplog related work
  (require '[clojure.java.io :as io])
  (defn- read-file
    "Read a file into a vector of strings.
     This is used for local testing in repl"
    [f]
    (with-open [rdr (io/reader (io/input-stream f))]
      (reduce conj [] (line-seq rdr))))

  (def client (mc/connect :replica-set [{:host "localhost"
                                         :port 27017
                                         :opts {:read-preference :primary}}
                                        {:host "localhost"
                                         :port 27018}
                                        {:host "localhost"
                                         :port 27019}]))

  (def target-client (mc/connect :replica-set [{:host "localhost"
                                         :port 28017
                                         :opts {:read-preference :primary}}
                                        {:host "localhost"
                                         :port 28018}
                                        {:host "localhost"
                                         :port 28019}]))

  (def command* (oplog-to-command (parse-mongoshake-oplog (first (read-file "shake-input.json")))))
  (mc/run-command (mc/get-db client (first command*)) (second command*))


  (def command* (oplog-to-command (parse-json (first (read-file "input.json")))))
  (mc/run-command (mc/get-db client (first command*)) (second command*))

  (tail-oplog client
              (fn [oplog]
                (println "Source oplog " oplog)
                (let [[db-name command] (oplog-to-command oplog)]
                  (when (and db-name command)
                    (println "Running command " command " on db " db-name)
                    (mc/run-command (mc/get-db target-client db-name) command)))))

  )
