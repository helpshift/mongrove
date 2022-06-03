(ns ^{:author "Rhishikesh Joshi <rhishikeshj@gmail.com>", :doc "A set of utilities for working with reactivestreams.Publishers"} mongrove.utils.subscribers
  (:refer-clojure :exclude [update])
  (:require
    [clojure.core.async :as async]
    [mongrove.core :as core])
  (:import
    (org.reactivestreams
      Publisher
      Subscriber)))


(defn ^:public-api basic-subscriber
  "Given an publisher and callbacks for a subscriber,
  requests Integer/MAX_VALUE objects from subscription
  and triggers appropriate callbacks"
  [publisher onNext onComplete onError]
  (let [subscriber (reify Subscriber
                     (onSubscribe
                       [this subscription]
                       (.request subscription Integer/MAX_VALUE))

                     (onNext
                       [this result]
                       (onNext result))

                     (onError
                       [this t]
                       (onError t))

                     (onComplete
                       [this]
                       (onComplete)))]
    (.subscribe publisher subscriber)))


;; This is a subscriber which returns
;; a stream of values on a core.async channel
;; FIXME: We need to find a way to remove use of mutables here
;; We have used it because this seems like the way to implement
;; a subscriber and also have state to get a handle on the subscription
;; member to request more later.
;; An alternate hack would be to request for all (Integer/MAX_VALUE) items
;; upfront.
(deftype ChanSubscriber
  [ch ^:unsynchronized-mutable subs]

  Subscriber

  (onSubscribe
    [this subscription]
    (.request subscription 1)
    (set! subs subscription))


  (onNext
    [this result]
    (async/>!! ch result)
    (.request subs 1))


  (onError
    [this t]
    (async/>!! ch t)
    (async/close! ch))


  (onComplete
    [this]
    (async/close! ch)))


(defn ^:public-api chan-subscriber
  "Given a Publisher, this returns a channel
  on which values from the publisher will be returned.
  When the results are done, the channel will be closed"
  [publisher]
  (let [ch (async/chan)
        subscriber (ChanSubscriber. ch nil)]
    (.subscribe publisher subscriber)
    ch))


;; This is a subscriber which returns
;; values in a promise
(deftype ValueSubscriber
  [pr ^:unsynchronized-mutable value]

  Subscriber

  (onSubscribe
    [this subscription]
    (.request subscription Integer/MAX_VALUE))


  (onNext
    [this result]
    (set! value (conj value result)))


  (onError
    [this t]
    (deliver pr t))


  (onComplete
    [this]
    (deliver pr value)))


(defn ^:public-api value-subscriber
  "Given a Publisher, this returns a promise
  which will be delivered on when the publisher completes,
  either with success or will failure
  on success, promise will be a collection of values returned
  on error, promise will be the error"
  [publisher]
  (let [val (promise)
        subscriber (ValueSubscriber. val [])]
    (.subscribe publisher subscriber)
    val))
