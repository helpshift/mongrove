(ns mongrove.generative.data
  (:require
    [clojure.spec.alpha :as s]
    [clojure.test.check.generators :as gen]
    [mongrove.core :as mcore]))


(defonce test-db "gen-test-db")


(defn gen-data-from-spec
  "Generate data from clojure.spec"
  [spec count]
  (gen/sample (s/gen spec) count))

;; Insert data

(defn insert-data-from-spec
  ([conn dbname coll spec count]
   (let [db (mcore/get-db conn dbname)]
     (mcore/insert db coll (gen-data-from-spec spec count) :multi? true)))
  ([coll spec count]
   (let [db (mcore/get-db test-db)]
     (mcore/insert db coll (gen-data-from-spec spec count) :multi? true))))
