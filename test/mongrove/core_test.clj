(ns mongrove.core-test
  (:require
    [clojure.string :as cs]
    [clojure.test :refer :all]
    [mongrove.core :as mc])
  (:import
    (com.mongodb.client
      MongoClient
      MongoCollection
      MongoDatabase)))


(def shared-connection (atom nil))


(defn- init-connection-fixture
  "This will run once before tests are executed
  and will initialise a connection into the shared-connection atom"
  [tests]
  (let [c (mc/connect :replica-set [{:host "localhost"
                                     :port 27017}])]
    (reset! shared-connection c)
    (tests)))


(defn- before-each-test
  []
  nil)


(defn- after-each-test
  "Clean up any test data that we might create.
  Note : All test data should be created under databases starting with `test-db`"
  []
  (let [dbs (mc/get-database-names @shared-connection)
        test-dbs (filter #(cs/starts-with? % "test-db") dbs)]
    (doseq [db test-dbs]
      (mc/drop-database (mc/get-db @shared-connection db)))))


(defn each-fixture
  [tests]
  (before-each-test)
  (tests)
  (after-each-test))


(use-fixtures :once (join-fixtures [init-connection-fixture each-fixture]))


(deftest connect-test
  (testing "Connect to a replica set with default opts"
    (let [client (mc/connect :replica-set [{:host "localhost"
                                            :port 27017}])]
      (is (not (nil? client)))
      (is (instance? MongoClient client))))
  (testing "Connect to a replica set with custom opts"
    (let [client (mc/connect :replica-set [{:host "localhost"
                                            :port 27017
                                            :opts {:read-preference :primary
                                                   :read-concern :default
                                                   :write-concern :majority
                                                   :retry-reads true
                                                   :retry-writes false
                                                   :connect-timeout 5000
                                                   :socket-timeout 5000
                                                   :connections-per-host 2
                                                   :max-connection-wait-time 5000}}])]
      (is (not (nil? client)))
      (is (instance? MongoClient client))))
  (testing "Connect to a direct node"
    (let [client (mc/connect :direct {:host "localhost"
                                      :port 27017
                                      :opts {:read-preference :primary}})]
      (is (not (nil? client)))
      (is (instance? MongoClient client)))))


(deftest get-databases-test
  (testing "Get databases list from connection"
    (let [client @shared-connection
          dbs (mc/get-databases client)]
      (is (not (nil? dbs)))
      (doseq [db dbs]
        (is (map? db))
        (is (= #{:name :empty :sizeOnDisk} (set (keys db))))))))


(deftest get-databases-names-test
  (testing "Get databases names from connection"
    (let [client @shared-connection
          dbs (mc/get-database-names client)]
      (is (not (nil? dbs)))
      (doseq [db dbs]
        (is (string? db))))))


(deftest get-db-test
  (testing "Get DB object from connection"
    (let [client @shared-connection
          db (mc/get-db client "test-db")]
      (is (not (nil? db)))
      (is (instance? MongoDatabase db)))))


(deftest get-collection-test
  (testing "Get collection object from db"
    (let [client @shared-connection
          db (mc/get-db client "test-db")
          coll (mc/get-collection db "test-coll")]
      (is (not (nil? coll)))
      (is (instance? MongoCollection coll))))
  (testing "Get collection object from db with write-concern"
    (let [client @shared-connection
          db (mc/get-db client "test-db")
          coll (mc/get-collection db "test-coll" :majority)]
      (is (not (nil? coll)))
      (is (instance? MongoCollection coll)))))


(deftest get-collections-test
  (testing "Get collection objects from db"
    (let [client @shared-connection
          db (mc/get-db client (str "test-db-" (.toString (java.util.UUID/randomUUID))))
          created-colls (for [i (range 10)]
                          (str "coll-" (.toString (java.util.UUID/randomUUID))))
          _ (doseq [c created-colls]
              (mc/insert db c {:a (rand-int 42)}))
          colls (mc/get-collection-names db)]
      (is (not (nil? colls)))
      (doseq [c colls]
        (is (string? c)))
      (is (= (set created-colls) (set colls))))))


(deftest drop-collections-test
  (testing "Drop collection objects from db"
    (let [client @shared-connection
          db (mc/get-db client (str "test-db-" (.toString (java.util.UUID/randomUUID))))
          _ (doseq [i (range 10)]
              (let [coll-name (str "coll-" (.toString (java.util.UUID/randomUUID)))]
                (mc/insert db coll-name {:a i})
                coll-name))
          colls (mc/get-collection-names db)
          drop-colls (take 5 colls)]
      (doseq [dc drop-colls]
        (mc/drop-collection db dc))
      (let [remaining-colls (mc/get-collection-names db)]
        (is (= (set remaining-colls) (set (drop 5 colls))))))))


(deftest indexes-test
  (testing "Create indexes in a collection"
    (let [client @shared-connection
          db (mc/get-db client (str "test-db-" (.toString (java.util.UUID/randomUUID))))
          coll (str "test-coll-" (.toString (java.util.UUID/randomUUID)))]
      (mc/create-index db coll (array-map :a 1 :b -1))
      (let [indexes (mc/get-indexes db coll)
            our-index (second indexes)]
        (is (= 2 (count indexes)))
        (is (= {:a 1 :b -1} (:key our-index)))
        (is (= "a_1_b_-1" (:name our-index)))))))
