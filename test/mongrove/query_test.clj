(ns mongrove.query-test
  (:require
    [clojure.string :as cs]
    [clojure.test :refer :all]
    [mongrove.core :as mc]))


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


(deftest fetch-one-test
  (testing "Get any document"
    (let [client @shared-connection
          docs (for [_ (range 10)]
                 {:name (.toString (java.util.UUID/randomUUID))
                  :age (rand-int 60)
                  :city (.toString (java.util.UUID/randomUUID))
                  :country (.toString (java.util.UUID/randomUUID))})
          db (mc/get-db client (str "test-db-" (.toString (java.util.UUID/randomUUID))))
          coll (str "test-coll-" (.toString (java.util.UUID/randomUUID)))]
      (mc/insert db coll docs :multi? true :write-concern :w1)
      (let [db-doc (mc/fetch-one db coll {})]
        (is ((set docs) db-doc)))))
  (testing "Get a particular document"
    (let [client @shared-connection
          docs (for [_ (range 10)]
                 {:name (.toString (java.util.UUID/randomUUID))
                  :age (rand-int 60)
                  :city (.toString (java.util.UUID/randomUUID))
                  :country (.toString (java.util.UUID/randomUUID))})
          db (mc/get-db client (str "test-db-" (.toString (java.util.UUID/randomUUID))))
          coll (str "test-coll-" (.toString (java.util.UUID/randomUUID)))]
      (mc/insert db coll docs :multi? true :write-concern :w1)
      (mc/insert db coll {:name "harry"
                          :age 100
                          :city "knowhere"
                          :country "rohan"})
      (let [db-doc (mc/fetch-one db coll {:age {:$gt 60}})]
        (is (= {:name "harry"
                :age 100
                :city "knowhere"
                :country "rohan"}
               db-doc)))))
  (testing "Get a particular document with some fields"
    (let [client @shared-connection
          docs (for [_ (range 10)]
                 {:name (.toString (java.util.UUID/randomUUID))
                  :age (rand-int 60)
                  :city (.toString (java.util.UUID/randomUUID))
                  :country (.toString (java.util.UUID/randomUUID))})
          db (mc/get-db client (str "test-db-" (.toString (java.util.UUID/randomUUID))))
          coll (str "test-coll-" (.toString (java.util.UUID/randomUUID)))]
      (mc/insert db coll docs :multi? true :write-concern :w1)
      (mc/insert db coll {:name "harry"
                          :age 100
                          :city "knowhere"
                          :country "rohan"})
      (let [db-doc (mc/fetch-one db coll {:age {:$gt 60}} :only [:name])
            another-doc (mc/fetch-one db coll {:age {:$lt 60}} :exclude [:name])
            yet-another-doc (mc/fetch-one db coll {:age {:$lt 60}}
                                          :only [:age :city]
                                          :exclude [:age])] ;; :only takes precedence
        (is (= {:name "harry"} db-doc))
        (is (= (set (keys another-doc)) #{:age :city :country}))
        (is (= (set (keys yet-another-doc)) #{:age :city}))))))


(deftest query-test
  (testing "Query all documents"
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
        (is (= docs db-docs)))))
  (testing "Get documents"
    (let [client @shared-connection
          docs (for [_ (range 10)]
                 {:name (.toString (java.util.UUID/randomUUID))
                  :age (rand-int 60)
                  :city (.toString (java.util.UUID/randomUUID))
                  :country (.toString (java.util.UUID/randomUUID))})
          query-docs (for [_ (range 10)]
                       {:name (.toString (java.util.UUID/randomUUID))
                        :age (+ 100 (rand-int 40))
                        :city (.toString (java.util.UUID/randomUUID))
                        :country (.toString (java.util.UUID/randomUUID))})
          db (mc/get-db client (str "test-db-" (.toString (java.util.UUID/randomUUID))))
          coll (str "test-coll-" (.toString (java.util.UUID/randomUUID)))]
      (mc/insert db coll docs :multi? true :write-concern :w1)
      (mc/insert db coll query-docs :multi? true :write-concern :w1)
      (mc/insert db coll {:name "harry"
                          :age 100
                          :city "knowhere"
                          :country "rohan"})
      (let [db-doc (mc/query db coll {:name "harry"})
            queried-docs (mc/query db coll {:age {:$gte 100}})]
        (is (= {:name "harry"
                :age 100
                :city "knowhere"
                :country "rohan"}
               (first db-doc)))
        (is (= query-docs queried-docs)))))
  (testing "Get documents with some fields"
    (let [client @shared-connection
          docs (for [_ (range 10)]
                 {:name (.toString (java.util.UUID/randomUUID))
                  :age (rand-int 60)
                  :city (.toString (java.util.UUID/randomUUID))
                  :country (.toString (java.util.UUID/randomUUID))})
          db (mc/get-db client (str "test-db-" (.toString (java.util.UUID/randomUUID))))
          coll (str "test-coll-" (.toString (java.util.UUID/randomUUID)))]
      (mc/insert db coll docs :multi? true :write-concern :w1)
      (mc/insert db coll {:name "harry"
                          :age 100
                          :city "knowhere"
                          :country "rohan"})
      (let [db-doc (mc/query db coll {:age {:$gt 60}} :only [:name])
            other-docs (mc/query db coll {:age {:$lt 60}} :exclude [:name])
            yet-another-doc (mc/query db coll {:age {:$lt 60}}
                                      :only [:age :city]
                                      :exclude [:age])] ;; :only takes precedence
        (is (= 1 (count db-doc)))
        (is (= 10 (count other-docs)))
        (is (= {:name "harry"} (first db-doc)))
        (doseq [d other-docs]
          (is (= (set (keys d)) #{:age :city :country})))
        (is (= (set (keys (first yet-another-doc))) #{:age :city}))))))


(deftest query-sort-test
  (testing "Query documents with single field sort"
    (let [client @shared-connection
          docs (for [_ (range 10)]
                 {:name (.toString (java.util.UUID/randomUUID))
                  ;; @WARN Increasing the range here because
                  ;; is the age value repeats, the sort of 2 entries
                  ;; with equal values is not predictable in MongoDB
                  :age (rand-int 300)
                  :city (.toString (java.util.UUID/randomUUID))
                  :country (.toString (java.util.UUID/randomUUID))})
          sorted-docs (sort #(compare (:age %1) (:age %2)) docs)
          r-sorted-docs (reverse sorted-docs)
          db (mc/get-db client (str "test-db-" (.toString (java.util.UUID/randomUUID))))
          coll (str "test-coll-" (.toString (java.util.UUID/randomUUID)))]
      (mc/insert db coll docs :multi? true :write-concern :w1)
      (let [db-docs (mc/query db coll {} :sort-by {:age 1})
            r-db-docs (mc/query db coll {} :sort-by {:age -1})]
        (is (= db-docs sorted-docs))
        (is (= r-db-docs r-sorted-docs))))))


(deftest query-limit-test
  (testing "Get only some documents"
    (let [client @shared-connection
          docs (for [_ (range 10)]
                 {:name (.toString (java.util.UUID/randomUUID))
                  :age (rand-int 300)
                  :city (.toString (java.util.UUID/randomUUID))
                  :country (.toString (java.util.UUID/randomUUID))})
          sorted-docs (sort #(compare (:age %1) (:age %2))
                            (filter #(< 100 (:age %)) docs))
          db (mc/get-db client (str "test-db-" (.toString (java.util.UUID/randomUUID))))
          coll (str "test-coll-" (.toString (java.util.UUID/randomUUID)))]
      (mc/insert db coll docs :multi? true :write-concern :w1)
      (let [queried-docs (mc/query db coll {:age {:$gt 100}} :sort-by {:age 1} :limit 2)
            one-doc (mc/query db coll {:age {:$gt 100}} :sort-by {:age 1} :one? true :limit 2)
            skip-docs (mc/query db coll {:age {:$gt 100}} :sort-by {:age 1} :skip 2)]
        (is (= (take 2 sorted-docs) queried-docs))
        (is (= 1 (count one-doc)))
        (is (= (first sorted-docs) (first one-doc)))
        (is (= (drop 2 sorted-docs) skip-docs))))))


(deftest count-docs-test
  (testing "Count documents"
    (let [client @shared-connection
          docs (for [_ (range 10)]
                 {:name (.toString (java.util.UUID/randomUUID))
                  :age (rand-int 60)
                  :city (.toString (java.util.UUID/randomUUID))
                  :country (.toString (java.util.UUID/randomUUID))})
          db (mc/get-db client (str "test-db-" (.toString (java.util.UUID/randomUUID))))
          coll (str "test-coll-" (.toString (java.util.UUID/randomUUID)))]
      (mc/insert db coll docs :multi? true :write-concern :w1)
      (let [count-db-docs (mc/count-docs db coll {})]
        (is (= 10 count-db-docs)))))
  (testing "Count only some documents"
    (let [client @shared-connection
          docs (for [_ (range 10)]
                 {:name (.toString (java.util.UUID/randomUUID))
                  :age (rand-int 300)
                  :city (.toString (java.util.UUID/randomUUID))
                  :country (.toString (java.util.UUID/randomUUID))})
          filtered-docs (filter #(< 100 (:age %)) docs)
          db (mc/get-db client (str "test-db-" (.toString (java.util.UUID/randomUUID))))
          coll (str "test-coll-" (.toString (java.util.UUID/randomUUID)))]
      (mc/insert db coll docs :multi? true :write-concern :w1)
      (let [queried-docs-count (mc/count-docs db coll {:age {:$gt 100}})]
        (is (= (count filtered-docs) queried-docs-count))))))
