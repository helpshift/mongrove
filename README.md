# mongrove

<img src="helpshift-logo.png" alt="drawing" width="200" height="200"/>

A Clojure library designed to interact with MongoDB.

## Status

[![Helpshift](https://circleci.com/gh/helpshift/mongrove.svg?style=shield)](https://circleci.com/gh/helpshift/mongrove)

## Usage

```clojure
(def client (connect :replica-set [{:host "localhost"
                                      :port 27017
                                      :opts {:read-preference :primary}}]))
  (def test-db (get-db client "test_driver"))

  (def mongo-coll "mongo")

  (ctl/info nil (query test-db mongo-coll {} :sort-by {:age 1}))

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
```

### API

To view full API you can generate documentation using [codox](https://github.com/weavejester/codox)

Run

```shell
lein codox
```
from the project folder and the API documentation will be generated in the `target/doc` folder.

## Next steps

1. Add support for ACID transactions
2. Add benchmarking results comparing against [Monger](https://github.com/michaelklishin/monger)
3. Add support for sharded Mongo clusters

## License

Copyright © Helpshift Inc. 2020

EPL (See [LICENSE](https://github.com/helpshift/mongrove/blob/master/LICENSE))
