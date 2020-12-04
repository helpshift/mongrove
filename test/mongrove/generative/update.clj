(ns mongrove.generative.update
  (:require
    [clojure.spec.alpha :as s]
    [clojure.test.check.generators :as gen]
    [mongrove.generative.query :as qgen]
    [mongrove.generative.scalar-generators :as sgen]
    [mongrove.generative.vector-generators :as vgen]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; Update operators
;; These use the mongo API
;; (gen-mongo/run-update coll query doc {:upsert? :multi?}) and
;; (gen-mongo/run-find-and-modify coll query doc {:sort :only :remove? :return-new? :upsert?})

(defn gen-update-options
  []
  (gen/generate (gen/map (gen/elements [:multi? :upsert?])
                         gen/boolean)))


(defn gen-find-and-modify-options
  [only]
  (merge (if only
           {:only only}
           {})
         (gen/generate (gen/map (gen/elements [:remove? :return-new? :upsert?])
                                gen/boolean))))

;; First we write the gen-update function
;; This returns a doc map which accepts an update operator, field name and value

(defn gen-update-doc
  [op type field & {:keys [each? position condition] :as extras}]
  (when (sgen/scalar-types type)
    (cond (= op :$pullAll)
          {op {field (gen/generate (vgen/gen-array type (rand-int 5)))}}

          ;; To use the $position modifier, it must appear with the $each modifier.
          (and (#{:$push :$addToSet} op)
               each?)
          (let [array-update (gen/hash-map :$each (vgen/gen-array type (rand-int 5)))]
            (gen/generate (gen/fmap #(assoc-in {}
                                               [op field]
                                               (merge (when position
                                                        {:position position})
                                                      %))
                                    array-update)))

          (and (= op :$pull)
               condition)
          {op (qgen/gen-query (:op condition)
                              (:type condition)
                              (:field condition))}

          :else
          {op {field (gen/generate (sgen/scalar-type->generator type))}})))


(defn gen-update-doc-from-spec
  [op spec & {:keys [each? position condition] :as extras}]
  (if (map? (gen/generate (s/gen spec)))
    (gen/fmap #(reduce (fn [acc [k v]]
                         (if (coll? v)
                           (conj acc {op
                                      {(str (name spec) "." (name k))
                                       v}})
                           acc))
                       []
                       %)
              (s/gen spec))
    (gen/map (gen/return op)
             (gen/fmap #(assoc {} (-> spec name keyword) %)
                       (cond (and (= op :$pullAll)
                                  (not (vector? (gen/generate (s/gen spec)))))
                             (gen/vector (s/gen spec)
                                         (rand-int 5))

                             ;; To use the $position modifier, it must appear with the $each modifier.
                             (and (#{:$push :$addToSet} op)
                                  each?)
                             (let [data (if (#(or (seq? %)
                                                  (vector? %))
                                             (gen/generate (s/gen spec)))
                                          (s/gen spec)
                                          (gen/vector (s/gen spec)
                                                      (rand-int 5)))
                                   array-update (gen/hash-map :$each data)]
                               (gen/fmap #(merge (when position
                                                   {:$position position})
                                                 %)
                                         array-update))

                             (and (= op :$pull)
                                  condition)
                             (qgen/gen-operator-expression-from-spec (:op condition)
                                                                     (:spec condition))

                             (= op :$unset)
                             (gen/return "")

                             :else
                             (s/gen spec)))
             {:num-elements 1})))


(defn gen-update-docs-from-spec
  [op specs]
  (gen/fmap
    #(into {} (map (fn [update-data]
                     (let [{:keys [spec
                                   each?
                                   position
                                   condition]}
                           update-data]
                       (get (gen/generate (gen-update-doc-from-spec op
                                                                    spec
                                                                    :each? each?
                                                                    :position position
                                                                    :condition condition))
                            op)))
                   %))
    (gen/return specs)))
