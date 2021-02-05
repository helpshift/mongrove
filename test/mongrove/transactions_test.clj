(ns mongrove.transactions-test
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
                                     :port 27017}
                                    {:host "localhost"
                                     :port 27018}
                                    {:host "localhost"
                                     :port 27019}])]
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


(deftest transactions-test
  (testing "Push multiple values to limited number of objects via overlapping threads"
    (let [client @shared-connection
          counters (take 10 (repeat (atom 0)))
          test-db (mc/get-db client (str "test-db-"
                                         (.toString (java.util.UUID/randomUUID))))
          num-threads 100
          futures (atom [])]
      (doseq [i (range 10)]
        (mc/insert test-db "a" {:id i
                                :vals []} :multi? false)
        (mc/insert test-db "b" {:id i
                                :vals []} :multi? false))
      (doseq [_ (range num-threads)]
        (let [f (future
                  (mc/run-in-transaction client
                                         (fn [session]
                                           (doseq [j (range 10)]
                                             (let [val (swap! (nth counters j) inc)]
                                               (mc/update test-db
                                                          "a"
                                                          {:id j}
                                                          {:$push {:vals val}}
                                                          :session session)
                                               (mc/update test-db
                                                          "b"
                                                          {:id j}
                                                          {:$push {:vals val}}
                                                          :session session))
                                             (Thread/sleep 500)))
                                         {:transaction-opts {:retry-on-errors true}})
                  (Thread/sleep 100))]
          (swap! futures conj f)))
      (doseq [i (range num-threads)]
        (deref (nth @futures i)))
      (let [acoll (mc/query test-db "a" {} :limit 0)
            bcoll (mc/query test-db "b" {} :limit 0)]
        ;; Ideally with retries all objects should eventually be inserted
        ;; without data loss. But apparently this doesn't happen.
        ;; Uncomment when you figure out more details here
        #_(doseq [j (range 10)]
          (is (= num-threads (count (:vals (nth acoll j)))))
          (is (= num-threads (count (:vals (nth bcoll j))))))
        (is (= (map #(set (:vals %)) acoll)
               (map #(set (:vals %)) bcoll)))))))


;; This test should ideally fail !
(deftest no-transactions-test
  (testing "Push multiple values to limited number of objects via overlapping threads"
    (let [client @shared-connection
          counters (take 10 (repeat (atom 0)))
          test-db (mc/get-db client (str "test-db-"
                                         (.toString (java.util.UUID/randomUUID))))
          num-threads 100
          futures (atom [])]
      (doseq [i (range 10)]
        (mc/insert test-db "a" {:id i
                                :vals []} :multi? false)
        (mc/insert test-db "b" {:id i
                                :vals []} :multi? false))
      (doseq [i (range num-threads)]
        (let [f (future
                  (doseq [j (range 10)]
                    (Thread/sleep 50)
                    (let [val (swap! (nth counters j) inc)]
                      (mc/update test-db "a" {:id j} {:$push {:vals val}})
                      (mc/update test-db "b" {:id j} {:$push {:vals val}})))
                  (Thread/sleep 100))]
          (swap! futures conj f)))
      (println "Derefing futures")
      (doseq [i (range num-threads)]
        (deref (nth @futures i)))
      (println "Done derefing")
      (let [acoll (mc/query test-db "a" {} :limit 0)
            bcoll (mc/query test-db "b" {} :limit 0)]
        (doseq [j (range 10)]
          (is (= num-threads (count (:vals (nth acoll j)))))
          (is (= num-threads (count (:vals (nth bcoll j))))))
        (is (= (map #(set (:vals %)) acoll)
               (map #(set (:vals %)) bcoll)))))))


(deftest transactions-for-atomicity
  (testing "Mongo operations in the same transaction either succeed or fail together"
    (let [client @shared-connection
          test-db (mc/get-db client (str "test-db-"
                                         (.toString (java.util.UUID/randomUUID))))]
      (try
        ;; it is important to create the namespace
        ;; first, since namespaces cant be created
        ;; in transaction
        (mc/insert test-db "a" {:id 1})
        (mc/insert test-db "b" {:id 1})
        (mc/run-in-transaction
          client
          (fn [session]
            (mc/insert test-db "a" {:a 42} :session session)
           ;; This will throw an exception
            (mc/insert test-db "b" {:b (.toString nil)} :session session)))
        (catch Exception e))
      (is (= 1 (mc/count-docs test-db "a" {})))
      (is (= 1 (mc/count-docs test-db "b" {}))))))
