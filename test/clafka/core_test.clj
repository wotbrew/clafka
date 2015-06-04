(ns clafka.core-test
  (:require [clojure.test :refer :all]
            [clafka.core :refer :all]))

(def head-block
  (fn []
    {:messages []
     :total-bytes 0
     :valid-bytes 0}))

(def example-payload
  (.getBytes "hello-world"))

(defn n-messages
  ([last-n n]
   (n-messages last-n n example-payload))
  ([last-n n payload]
   (fn [] {:messages
          (for [n (range last-n n)]
            {:message payload
             :offset n
             :next-offset (inc n)})
          :total-bytes 10})))

(defn- slow-pop*
  [ref]
  (let [h (first @ref)]
    (ref-set ref (into [] (rest @ref)))
    h))

(defrecord SimulatedConsumer [state]
  IConsumer
  (-fetch [this topic partition offset size]
    (if-let [next-thunk (dosync (slow-pop* state))]
      (next-thunk)
      (head-block))))

(defn sim-consumer
  [steps]
  (map->SimulatedConsumer {:state (ref (into [] steps))}))

(defn error-fetcher
  [error-code]
  (reify IConsumer
    (-fetch [this topic partition offset size]
      {:error :can-be-anything
       :error-code error-code})))

(def dummy-fetcher
  "Always fetches 0 messages"
  (reify IConsumer
    (-fetch [this topic partition offset size]
      {:messages []})))

(deftest test-fetch
  (testing "Errors are thrown if an :error & :error-code is passed back in the fetch response"
    (->>
     "Error code 1 should cause fetch to throw an OffsetOutOfRangeException"
     (is
      (thrown?
       kafka.common.OffsetOutOfRangeException
       (fetch (error-fetcher 1) "can-be-anything" 0 0)))))

  (testing "It should not matter if my partition is a string or already a numeric value"
    (->>
     "String partitions are passed through to the underlying fetcher as numbers"
     (is
      (= :a-number
         (fetch (reify IConsumer
                  (-fetch [this topic partition offset size]
                    (if (number? partition)
                      :a-number
                      :not-a-number)))
                "can-be-anything"
                "12"
                0)))))

  (->> "If an exception is thrown by the underlying fetch call it bubbles up"
       (is (thrown? Exception
                    (fetch (sim-consumer [#(throw (Exception.))]))))))

(deftest test-fetch-log

  (testing "If any fetch returns an :error and :error-code the seq will also throw when evaluated"
    (let [c (sim-consumer [(n-messages 0 5)
                           #(throw (Exception.))])
          log (fetch-log c "anything" 0 0)]
      (->> "The first fetch is successful"
           (is (= (:messages ((n-messages 0 5)))
                  (take 5 log))))

      (->> "The second fetch will throw"
           (is (thrown? Exception
                        (first  (drop 5 log)))))))

  (testing "Once we are at the log head the sequence is ends"
    (let [c (sim-consumer [(n-messages 0 5)
                           head-block])]
      (is (= (:messages ((n-messages 0 5)))
             (fetch-log c "anything" 0 0))))))


(deftest test-log-seq

  (testing "If any fetch returns an :error and :error-code the seq will also throw when evaluated"
    (let [c (sim-consumer [(n-messages 0 5)
                           #(throw (Exception.))])
          log (log-seq c "anything" 0 0)]
      (->> "The first fetch is successful"
           (is (= (:messages ((n-messages 0 5)))
                  (take 5 log))))

      (->> "The second fetch will throw"
           (is (thrown? Exception
                        (first  (drop 5 log)))))))

  (testing "Once at the log head, I will wait until new messages are added to the log"
    (let [c (sim-consumer [(n-messages 0 5)
                           head-block])
          read (atom [])
          block-on-me (promise)
          fetching (future (try (doseq [m (log-seq c "anything" 0 0 {:poll-ms 10})]
                                  (deliver block-on-me :something)
                                  (swap! read conj m))
                                (catch Exception e
                                  nil)))]
      ;;wait for the first block to be read
      (is (deref block-on-me 2000 false))
      ;;put a new block on the log
      (dosync (alter (:state c) conj (n-messages 5 10)))
      ;;force an exception to kill our future
      (dosync (alter (:state c) conj #(throw (Exception.))))

      ;;wait for the future to die
      (when (= :kill (deref fetching 1000 :kill))
        (future-cancel fetching))

      (->> "We should have read both the first block, and the second block which was
           added to our log after the log-seq was created"
           (is (= (:messages ((n-messages 0 10)))
                  @read))))))
