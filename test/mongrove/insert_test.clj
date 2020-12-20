(ns mongrove.insert-test
  (:require
    [clojure.string :as cs]
    [clojure.test :refer :all]
    [mongrove.core :as mc]))


(def shared-connection (atom nil))


(defn- init-connection-fixture
  "This will run once before tests are executed
  and will initialise a connection into the shared-connection atom"
  [tests]
  (let [c (mc/connect :replica-set [{:host "shiva.local"
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


(deftest insert-test
  (testing "Insert single document with default options"
    (let [client @shared-connection
          doc {:name (.toString (java.util.UUID/randomUUID))
               :age (rand-int 60)
               :city (.toString (java.util.UUID/randomUUID))
               :country (.toString (java.util.UUID/randomUUID))}
          db (mc/get-db client (str "test-db-" (.toString (java.util.UUID/randomUUID))))
          coll (str "test-coll-" (.toString (java.util.UUID/randomUUID)))]
      (mc/insert db coll doc)
      (let [db-doc (mc/query db coll {})]
        (is (= 1 (count db-doc)))
        (is (= doc (first db-doc))))))
  (testing "Insert single document with options"
    (let [client @shared-connection
          doc {:name (.toString (java.util.UUID/randomUUID))
               :age (rand-int 60)
               :city (.toString (java.util.UUID/randomUUID))
               :country (.toString (java.util.UUID/randomUUID))}
          db (mc/get-db client (str "test-db-" (.toString (java.util.UUID/randomUUID))))
          coll (str "test-coll-" (.toString (java.util.UUID/randomUUID)))]
      (mc/insert db coll doc :multi? false :write-concern :w1)
      (let [db-doc (mc/query db coll {})]
        (is (= 1 (count db-doc)))
        (is (= doc (first db-doc))))))
  (testing "Insert multiple documents"
    (let [client @shared-connection
          docs (for [_ (range 10)]
                 {:name (.toString (java.util.UUID/randomUUID))
                  :age (rand-int 60)
                  :city (.toString (java.util.UUID/randomUUID))
                  :country (.toString (java.util.UUID/randomUUID))})
          db (mc/get-db client (str "test-db-" (.toString (java.util.UUID/randomUUID))))
          coll (str "test-coll-" (.toString (java.util.UUID/randomUUID)))]
      (mc/insert db coll docs :multi? true :write-concern :w1)
      (let [db-docs (mc/query db coll {})]
        (is (= 10 (count db-docs)))
        (is (= docs db-docs))))))


(deftest delete-test
  (testing "Delete documents"
    (let [client @shared-connection
          docs (for [_ (range 10)]
                 {:name (.toString (java.util.UUID/randomUUID))
                  :age (rand-int 60)
                  :city (.toString (java.util.UUID/randomUUID))
                  :country (.toString (java.util.UUID/randomUUID))})
          db (mc/get-db client (str "test-db-" (.toString (java.util.UUID/randomUUID))))
          coll (str "test-coll-" (.toString (java.util.UUID/randomUUID)))]
      (mc/insert db coll docs :multi? true :write-concern :w1)
      (mc/delete db coll {})
      (let [count-db-docs (mc/count-docs db coll {})]
        (is (= 0 count-db-docs)))))
  (testing "Delete only some documents"
    (let [client @shared-connection
          docs (for [_ (range 10)]
                 {:name (.toString (java.util.UUID/randomUUID))
                  :age (rand-int 300)
                  :city (.toString (java.util.UUID/randomUUID))
                  :country (.toString (java.util.UUID/randomUUID))})
          filtered-docs (filter #(>= 100 (:age %)) docs)
          db (mc/get-db client (str "test-db-" (.toString (java.util.UUID/randomUUID))))
          coll (str "test-coll-" (.toString (java.util.UUID/randomUUID)))]
      (mc/insert db coll docs :multi? true :write-concern :w1)
      (mc/delete db coll {:age {:$gt 100}})
      (let [queried-docs (mc/query db coll {:age {:$lte 100}})]
        (is (= filtered-docs queried-docs))))))
