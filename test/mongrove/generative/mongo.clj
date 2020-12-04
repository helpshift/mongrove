(ns mongrove.generative.mongo
  (:require
    [mongrove.core :as mcore]))


(defonce __db__ "gen-test-db")

(defn connect
  [server-addrs]
  (mcore/connect :replica-set server-addrs))

;;;;;;;;;;;;;; Generate insert queries for populating db

(defn insert-data
  ([conn db-name coll data]
   (let [db (mcore/get-db conn db-name)]
     (mcore/insert db coll data)))
  ([conn coll data]
   (insert-data conn __db__ coll data)))


(defn bulk-insert-data
  ([conn db-name coll data]
   (let [db (mcore/get-db conn db-name)]
     (mcore/insert db coll data :multi? true)))
  ([conn coll data]
   (insert-data conn __db__ coll data)))


(defn get-data
  ([conn db coll id]
   (let [db (mcore/get-db conn db)]
     (mcore/fetch-one db coll {:id id})))
  ([conn coll id]
   (get-data conn __db__ coll id)))


(defn get-all-data
  ([conn db coll]
   (let [db (mcore/get-db conn db)]
     (mcore/query db coll {} :limit 0)))
  ([conn coll]
   (get-all-data conn __db__ coll)))


(defn delete-all-data
  ([conn db coll]
   (let [db (mcore/get-db conn db)]
     (mcore/delete db coll {})))
  ([conn coll]
   (delete-all-data conn __db__ coll)))


(defn run-query
  ([conn db coll query]
   (let [db (mcore/get-db conn db)]
     (mcore/query db coll query)))
  ([conn coll query]
   (run-query conn __db__ coll query)))


(defn run-update
  ([conn db coll query update-doc options]
   (let [db (mcore/get-db conn db)]
     (apply (partial mcore/update db coll query update-doc)
            (mapcat identity options))))
  ([conn coll query update-doc options]
   (run-update conn __db__ coll query update-doc options)))
