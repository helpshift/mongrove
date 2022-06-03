# mongrove

<img src="helpshift-logo.png" alt="drawing" width="200" height="200"/>

A Clojure library designed to interact with MongoDB.

## Status

[![Helpshift](https://circleci.com/gh/helpshift/mongrove.svg?style=shield)](https://circleci.com/gh/helpshift/mongrove)

## APIs
Currently there are 2 ways in which the mongrove API can be used to interact with MongoDB

The first is the wrapper over the traditional synchronous [Java API](https://www.mongodb.com/docs/drivers/java/sync/current/)
And there is also the wrapper over the [Reactive Java API](https://www.mongodb.com/docs/drivers/reactive-streams/)



### Sync Java API

```clojure
(def client (connect :replica-set [{:host "localhost"
                                    :port 27017
                                    :opts {:read-preference :primary}}]))
(def test-db (get-db client "test_driver"))

(def mongo-coll "mongo")

(query test-db mongo-coll {} :sort-by {:age 1})

(count-docs test-db mongo-coll {:age {:$lt 10}})

(count-docs test-db mongo-coll {})

(doseq [i (range 10)]
  (insert test-db mongo-coll {:id i
                              :name (str "user-" i)
                              :age (rand-int 20)
                              :dob (java.util.Date.)} :multi? false))

(fetch-one test-db mongo-coll {:id 3} :only [:name])

(delete test-db mongo-coll {:age {:$gt 10}})

(update test-db mongo-coll {:age {:$lt 10}} {:$inc {:age 1}})

;; For using multi-document transactions,

(try
    (delete test-db "a" {})
    (delete test-db "b" {})
    ;; Creating new collections is not supported
    ;; in Mongo 4.0 so ensure that there exist collections
    ;; a and b
    (insert test-db "a" {:id 1})
    (insert test-db "b" {:id 1})

    (run-in-transaction client
                        (fn [session]
                          ;; DO NOT ADD try-catch here. If you do this, exceptions
                          ;; will not percolate to the transaction and it will get committed
                          ;; successfully
                          (insert test-db "a" {:a 42} :session session)
                          ;; This will throw an exception
                          (insert test-db "b" {:b (.toString nil)} :session session))
                        {:transaction-opts {:retry-on-errors true}})
    (catch Exception e
      (println "Data in collection a " (query test-db "a" {}))))
```

### Reactive Java API


```clojure

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
  (basic-subscriber insert-one-publisher
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
        p (value-subscriber (insert test-db "b" docs :multi? true))]
    (println "Documents inserted : " @p))


  ;; insert-many chan-subscriber
  (let [b-coll (get-collection test-db "b")
        docs (reduce #(conj %1 {:id %2
                                :name (str "user-" %2)
                                :age (rand-int 20)
                                :dob (java.util.Date.)})
                     []
                     (vec (range 10)))
        mongo-chan (chan-subscriber (insert test-db "b" docs :multi? true))]
    (println "Got results on channel: " (async/<!! mongo-chan)))


  ;; count docs
  (let [c (value-subscriber (count-docs test-db "b" {:age {:$gt 15}}))]
    (println "Number of documents is " @c))

  ;; query, results on a channel
  (let [ch (chan-subscriber (query test-db "b" {:age {:$gt 15}} :exclude [:name]))]
    (loop []
      (let [val (async/<!! ch)]
        (println val)
        (when (some? val)
          (println "Doc: " val)
          (recur)))))

  ;; query, results in a promise
  (let [pr (value-subscriber (query test-db "b" {:name "user-3"} :only [:age] :exclude [:name]))]
    (println "Documents are " @pr))

  ;; fetch-one
  (let [c (value-subscriber (fetch-one test-db "b" {:name "user-4"} :exclude [:id]))]
    (println "Document is " @c))

  (let [c (value-subscriber (drop-collection test-db "a"))]
    (println "Collection a dropped " @c))

  (let [c (value-subscriber (get-collection-names test-db))]
    (println "Collections are" @c))

  (let [c (value-subscriber (get-database-names client))]
    (println "Collections are" @c))

  (let [c (value-subscriber (create-index test-db "b" (array-map :a 1 :b -1)))
        indexes (value-subscriber (get-indexes test-db "b"))]
    (println "Indexes are " @indexes))

```


### API docs

To view full API you can generate documentation using [codox](https://github.com/weavejester/codox)

Run

```shell
lein codox
```
from the project folder and the API documentation will be generated in the `target/doc` folder.

## Tests

The `test/mongrove` folder contains unit tests for mongrove APIs. For running these tests, please make sure you have a running mongo cluster on `localhost:27017`
If you want to test against a different mongo cluster, please change the options in the test namespaces' `init-connection-fixture` function.

The `test/mongrove/generative` folder contains generative tests for Mongo query operators. These tests are primarily aimed comparing query results across 2 different Mongo clusters, something you might need to do when testing for an upgrade !

## Next steps

1. Add support for ACID transactions : ✅
2. Add benchmarking results comparing against [Monger](https://github.com/michaelklishin/monger)
3. Add support for sharded Mongo clusters
4. Add support for Reactive Java API : ✅

## License

Copyright © Helpshift Inc. 2020

EPL (See [LICENSE](https://github.com/helpshift/mongrove/blob/master/LICENSE))
