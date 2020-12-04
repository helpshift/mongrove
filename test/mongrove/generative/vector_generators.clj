(ns mongrove.generative.vector-generators
  (:require
    [clojure.test.check.generators :as gen]
    [mongrove.generative.scalar-generators :as sgen]))


(def vector-types #{:array :object :set})

;; Generators for vector types

(defn gen-array
  "Generate an array of scalar values"
  ([size]
   (gen/bind (gen/elements sgen/scalar-types)
             (fn [type]
               (gen-array type size))))
  ([type size]
   (gen/vector (sgen/scalar-type->generator type)
               (or size
                   10))))


(defn gen-object
  [n]
  (gen/map gen/keyword
           (gen/elements sgen/scalar-types)
           {:num-elements n}))


(defn gen-set
  ([size]
   (gen/bind (gen/elements sgen/scalar-types)
             (fn [type]
               (gen-set type size))))
  ([type size]
   (gen/fmap (partial apply hash-set) (gen/vector (sgen/scalar-type->generator type)
                                                  (or size
                                                      10))))
  ([type size elements]
   (gen/fmap #(vec (take size %)) (gen/shuffle elements))))
