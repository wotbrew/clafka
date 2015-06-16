(ns clafka.pool-test
  (:require [clafka.pool :refer :all]
            [clafka.proto :refer :all]
            [clafka.sim :as sim]
            [clafka.core :as clafka]
            [clojure.test :refer :all]))


(deftest test-pool
  (let [add-leaders #(-> %
                         (sim/+leader "test" 0 {:host "localhost" :port 9092})
                         (sim/+leader "test" 1 {:host "localhost" :port 9093})
                         (sim/+leader "test" 2 {:host "localhost" :port 9094}))

        consumer1 (sim/client (-> (sim/broker "localhost" 9092)
                                  add-leaders
                                  (sim/+message "test" 0 (sim/message (.getBytes "msg1")))))
        consumer2 (sim/client (-> (sim/broker "localhost" 9093)
                                  add-leaders
                                  (sim/+message "test" 1 (sim/message (.getBytes "msg2")))))
        consumer3 (sim/client (-> (sim/broker "localhost" 9094)
                                  add-leaders
                                  (sim/+message "test" 2 (sim/message (.getBytes "msg3")))))

        consumers (group-by (comp :broker deref :state) [consumer1 consumer2 consumer3])

        pool (pool (keys consumers) 1
                   {:wait-time 50
                    :back-off-time 50
                    :factory (fn [host port] (first (consumers {:host host :port port})))})]

    (testing "I should be able for data from any partition and the right consumer will be hit"
      (is (= (seq (.getBytes "msg1"))
             (-> (clafka/fetch pool "test" 0 0)
                 :messages
                 first
                 :message
                 seq)))

      (is (= (seq (.getBytes "msg2"))
             (-> (clafka/fetch pool "test" 1 0)
                 :messages
                 first
                 :message
                 seq)))

      (is (= (seq (.getBytes "msg3"))
             (-> (clafka/fetch pool "test" 2 0)
                 :messages
                 first
                 :message
                 seq))))

    (testing "I can ask about the leaders of each partition (can use any consumer)"
      (is (= {:host "localhost" :port 9092}
             (clafka/find-leader pool "test" 0)))

      (is (= {:host "localhost" :port 9093}
             (clafka/find-leader pool "test" 1)))

      (is (= {:host "localhost" :port 9094}
             (clafka/find-leader pool "test" 2))))

    (testing "I can still get errors..."
      (is (thrown? kafka.common.OffsetOutOfRangeException
                   (clafka/fetch pool "test" 0 100)))

      (is (thrown? Exception
                   (clafka/fetch pool "test2" 0 0))))

    (shutdown! pool)))
