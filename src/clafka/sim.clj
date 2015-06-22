(ns clafka.sim
  "Contains a simulated IBrokerClient for use in tests and at the repl"
  (:require [clafka.core :as clafka]
            [clafka.proto :refer :all]))

(def head-block
  {:messages []
   :total-bytes 0
   :valid-bytes 0})

(defn message
  ([]
   {:sim/bytes 1024})
  ([payload]
   {:sim/bytes (count payload)
    :message payload}))

(defn messages
  ([n]
   (repeat n (message))))

(defrecord SimulatedClient [state]
  IBrokerClient
  (-find-topic-metadata [this topics]
    (mapv
     #(get-in @state [:topic-metadata %])
     topics))
  (-find-offsets [this m time]
    (for [[topic partitions] m]
      [topic
       (->> partitions
            (mapv
             #(get-in @state [:offsets topic %])))]))
  (-fetch [this topic partition offset size]
    (or
     (when-not (= (:broker @state) (clafka/find-leader this topic partition))
       {:error :not-leader-for-partition
        :error-code 6})
     (when-let [log (get-in @state [:log topic partition])]
       (let [first-offset (:offset (first log))
             last-offset (:next-offset (peek log))]
         (if (or (< offset first-offset)
                 (< last-offset offset))
           {:error :offset-out-of-range
            :error-code 1}
           (when-let [msgs (seq (drop-while #(not= (:offset %) offset) log))]
             (first
              (reduce (fn [[acc bytes-left] x]
                        (if (<= (:sim/bytes x) bytes-left)
                          (-> (update-in acc [:messages] conj x)
                              (update-in [:valid-bytes] + (:sim/bytes x))
                              (assoc :total-bytes size)
                              (vector (- bytes-left (:sim/bytes x))))
                          (reduced [acc bytes-left])))
                      [{:messages []
                        :total-bytes 0
                        :valid-bytes 0} size]
                      msgs))))))
     head-block))
  ICloseable
  (shutdown! [this]
    (reset! state nil)))

(defn client
  "Create a simulated client suitable for tests and repl usage.
  e.g (client (-> (broker \"localhost\" 9092) (+make-leader \"test\" 0) (+messages \"test\" 0 (messages 20))))
  => creates a client that is the leader of topic: test partition: 0, having 20 1024 byte messages in its log."
  [m]
  (map->SimulatedClient {:state (atom m)}))

(defn broker
  [host port]
  {:broker
   {:host host
    :port port}})

(defn +leader
  [m topic partition broker]
  (update-in m
             [:topic-metadata topic :partitions]
             (fnil conj [])
             {:partition-id partition
              :leader broker}))

(defn +make-leader
  [m topic partition]
  (+leader m topic partition (:broker m)))

(defn add-leader
  "Makes a simulated client recognise the broker as leader for the given partition"
  [client topic partition broker]
  (swap! (:state client) +leader topic partition broker))

(defn make-leader
  "Makes the client a leader for the given partition"
  [client topic partition]
  (swap! (:state client) +make-leader topic partition))

(defn +message
  [m topic partition message]
  (update-in m
             [:log topic partition]
             (fn [coll]
               (conj (or coll [])
                     (merge message
                            {:offset (or (:next-offset (peek coll)) 0)
                             :next-offset (inc (or (:next-offset (peek coll)) 0))})))))

(defn +messages
  [m topic partition messages]
  (reduce #(+message %1 topic partition %2) m messages))

(defn add-message
  "Adds a message to the simulated client's log"
  [client topic partition message]
  (swap! (:state client) +message topic partition message)
  nil)
