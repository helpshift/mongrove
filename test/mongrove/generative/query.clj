(ns mongrove.generative.query
  (:require
    [clojure.spec.alpha :as s]
    [clojure.test.check.generators :as gen]
    [mongrove.generative.scalar-generators :as sgen]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; The below functions will generate queries for

;; $gt/$lt/$gte/$lte operator
;; Syntax: {field: {$gt: value} }
;; Clojure syntax: {:a {:$gt 1}}
;; Supported BSON types
;; numeric types (int, long, decimal), string, array, object, date, timestamp, binData

;; $eq operator
;; Syntax: {field: {$eq: value} }
;; Clojure syntax: {:a {:$eq 1}}
;; Supported BSON types
;; numeric types (int, long, decimal), string, array, object, date, timestamp, binData

;; $ne operator
;; Syntax: {field: {$ne: value} }
;; Clojure syntax: {:a {:$ne 1}}
;; Supported BSON types
;; numeric types (int, long, decimal), string, array, object, date, timestamp, binData

(defn gen-operator-expression
  [op type]
  (gen/fmap #(assoc {} op %)
            (cond (= op :$exists)
                  (sgen/scalar-type->generator :boolean)

                  (= op :$size)
                  (sgen/scalar-type->generator :int)

                  (and (or (= op :$nin)
                           (= op :$in))
                       (sgen/scalar-types type))
                  (gen/vector (sgen/scalar-type->generator type)
                              (rand-int 5))

                  :else
                  (sgen/scalar-type->generator type))))


(defn gen-query
  [op type field]
  (when (sgen/scalar-types type)
    (gen/map (gen/return field)
             (gen-operator-expression op type)
             {:num-elements 1})))


(defn gen-operator-expression-from-spec
  [op spec]
  (gen/fmap #(assoc {} op %)
            (cond (= op :$exists)
                  gen/boolean

                  (= op :$size)
                  (s/gen (s/int-in 0 10))

                  (and (or (= op :$nin)
                           (= op :$in))
                       (not (vector? (gen/generate (s/gen spec)))))
                  (s/gen (s/coll-of spec :kind vector? :max-count 5))

                  :else
                  (s/gen spec))))


(defn gen-query-from-spec
  [op spec]
  (if (map? (gen/generate (s/gen spec)))
    (gen/fmap #(map (fn [[k v]]
                      {(str (name spec) "."
                            (name k))
                       ;; Ideally we would have done this !
                       ;; But alas, specs are too opaque and are not
                       ;; given to composing like this. Once part of another
                       ;; spec via s/keys, nothing is visible
                       ;;(gen-operator-expression-from-spec op (get spec k))
                       {op (cond (= op :$exists)
                                 (gen/generate gen/boolean)

                                 (= op :$size)
                                 (gen/generate (s/gen (s/int-in 0 10)))

                                 (and (or (= op :$nin)
                                          (= op :$in))
                                      (not (vector? (get (gen/generate (s/gen spec)) k))))
                                   ;; create a vector of v type values
                                 (map (fn [_]
                                        (get (gen/generate (s/gen spec)) k))
                                      (range 5))

                                 :else
                                 v)}})
                    %)
              (s/gen spec))
    (gen/map (gen/return (-> spec name keyword))
             (gen-operator-expression-from-spec op spec)
             {:num-elements 1})))


;; $and/$or/$nor operators
;; Syntax : $and/$or/$nor: [ { <expression1> }, { <expression2> } , ... , { <expressionN> } ] }
;; Clojure syntax : {:$and/:$or/:$nor : [{:a {:$gt 1}}]}

;; exprs need to be [{:op :type :field}]

(let [allowed-ops #{:$and :$or :$nor}]
  (defn gen-logical-query
    [op exprs]
    (when (allowed-ops op)
      ;; fmap to create the {op : expression}
      (gen/fmap #(assoc {} op %)
                ;; This fmap gives the generator which
                ;; creates the vector of expressions based on input exprs
                (gen/fmap #(map (fn [{:keys [op type field]}]
                                  ;; nested generators dont seem to work
                                  ;; so we need actual instances of the generated
                                  ;; data here
                                  (gen/generate (gen-query op type field)))
                                %)
                          (gen/return exprs))))))


(let [allowed-ops #{:$and :$or :$nor}]
  (defn gen-logical-query-from-spec
    [op specs]
    (when (allowed-ops op)
      ;; if the operator is $and &
      ;; all specs are same, meaning we
      ;; want to apply operators to the same
      ;; field, we use implicit $and form
      (if (and (= op :$and)
               (reduce = (map :spec specs)))
        ;; inputs will look like
        ;; op :$and specs : [{:op :$gt :spec ::dob} {:op :$lt :spec ::dob}]
        ;; output : {:dob {:$gt some-date :$lt some-date}
        (gen/fmap #(assoc {}
                          (-> specs first :spec name keyword)
                          %)
                  (gen/fmap #(reduce (fn [acc {:keys [op spec]}]
                                       (merge acc
                                              (gen/generate (gen-operator-expression-from-spec op
                                                                                               spec))))
                                     {}
                                     %)
                            (gen/return specs)))
        (gen/fmap #(assoc {} op %)
                  (gen/fmap #(map (fn [{:keys [op spec]}]
                                    (gen/generate (gen-query-from-spec op spec)))
                                  %)
                            (gen/return specs)))))))


(defn gen-not-query
  [op type field]
  (gen/fmap (fn [expr]
              {field {:$not {op expr}}})
            (sgen/scalar-type->generator type)))


(defn gen-not-query-from-spec
  [op spec]
  (gen/fmap (fn [expr]
              {(-> spec name keyword) {:$not expr}})
            (gen-operator-expression-from-spec op spec)))
