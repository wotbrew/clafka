(ns clafka.pool
  "Contains a pooled IBrokerClient which gives up some performance
   for greater concurrency"
  (:require [clafka.proto :refer :all]
            [clafka.core :as clafka])
  (:import [java.util.concurrent LinkedBlockingQueue TimeUnit]))



(defn dequeue-loop
  [broker-client queue {:keys [wait-time back-off-time]}]
  (let [stop? (atom false)
        loop
        (future
          (loop []
            (when-not @stop?
              (when-let [[[f & args] r] (.poll queue wait-time TimeUnit/MILLISECONDS)]
                (do
                  (deliver r (try
                               (apply f broker-client args)
                               (catch Throwable e e)))
                  (when (instance? Throwable @r)
                    (Thread/sleep back-off-time))))
              (recur))))]
    (reify ICloseable
      (shutdown! [this]
        (reset! stop? true)
        @loop))))

(def ^:dynamic *req-timeout* (* 1000 15))

(defn make-req
  [queue f & args]
  (let [r (promise)]
    (.put queue [(cons f args) r])
    (let [x (deref r *req-timeout* ::error)]
      (cond
       (instance? Throwable x) (throw x)
       (= x ::error) (throw (Exception. "Could not receive a result from underlying client"))
       :else x))))

(defn make-req-retry
  [queue n f & args]
  (when (pos? n)
    (trampoline (fn ! [n]
                  (try
                    (apply make-req queue f args)
                    (catch InterruptedException e
                      (throw e))
                    (catch Throwable e
                      (if (= n 0)
                        (throw e)
                        (fn [] (! (dec n)))))))
                n)))

(defn with-leader
  [pool topic partition f]
  (if-let [{:keys [m meta-queue]} pool]
    (f (make-req-retry meta-queue (count (mapcat identity (vals m)))
                            clafka/find-leader topic partition))
    (throw (Exception. (format "No leader could be found for %s %s" topic partition)))))

(defn with-leader-client
  [pool topic partition f & args]
  (with-leader
    pool topic partition
    (fn [leader]
      (if-let [cls (seq (get (:m pool) (select-keys leader [:host :port])))]
        (apply make-req-retry
         (:queue (first cls))
         (count cls)
         f args)
        (throw (Exception. (str "No clients could be found for broker: " leader)))))))

(defrecord PooledClient [meta-queue shutdown? m]
  IBrokerClient
  (-fetch [this topic partition offset size]
    (when @shutdown?
      (throw (Exception. "PooledClient is closed")))
    (with-leader-client
      this topic partition
      -fetch topic partition offset size))
  (-find-topic-metadata [this topics]
    (when @shutdown?
      (throw (Exception. "PooledClient is closed")))
    (make-req-retry
     meta-queue (count (mapcat identity (vals m)))
     -find-topic-metadata topics))
  (-find-offsets [this m time]
    (when @shutdown?
      (throw (Exception. "PooledClient is closed")))
    (->> (for [[topic partitions] m
               partition partitions]
           (with-leader-client
             this topic partition
             -find-offsets {topic [partition]} time))
         (apply merge-with #(reduce conj %1 %2))))
  ICloseable
  (shutdown! [this]
    (reset! shutdown? true)
    (doseq [{:keys [loop client]} (mapcat identity (vals m))]
      (try-shutdown! loop)
      (shutdown! client))))

(def default-config
  "The default configuration used for `pool`"
  {:wait-time 1000
   :back-off-time (* 5 2000)
   :factory clafka/consumer})

(defn pool
  "Creates a pooled client allowing `n` clients per broker.

  Brokers should be collection of maps of the form `{:host host-name, :port port-number}`.

  Re-routes requests requiring leadership to the correct brokers and load balances requests
  over the pool of connections.

  Some naive back-off is applied as connections fail so that they should be less likely
  to handle subsequent requests for a period of time.

  It is expected that the underlying IBrokerClient's are able to re-establish their own connections, this
  is true of the `SimpleConsumer`.

  If a leader changes while still making a request you can still get a `NotLeaderForPartitionException`.

  A fn `:factory` taking host, port as args can be specified in order to create custom IBrokerClients.
  Such clients should also be `ICloseable`. (`SimpleConsumer` instances are already `ICloseable`)

  Load balancing is achieved through allocating worker threads for each client.
  The configuration supports a `wait-time` which is the poll time on the work queue and a `back-off-time`
  which is the time a worker should wait after an error, in hope that it recovers the next time
  it picks up some work.

  Shutdown the pool with `clafka.proto/shutdown!`"
  ([brokers n]
   (pool brokers n {}))
  ([brokers n config]
   (let [config (merge default-config
                       config)
         factory (:factory config)
         meta-queue (LinkedBlockingQueue. 1)
         loops (for [broker brokers
                     :let [queue (LinkedBlockingQueue. 1)
                           client-factory #(factory (:host broker) (:port broker))]
                     client (repeatedly n client-factory)]
                 {:broker broker
                  :queue queue
                  :client client
                  :loop (dequeue-loop client queue config)
                  :meta-loop (dequeue-loop client meta-queue config)})]
     (map->PooledClient
      {:meta-queue meta-queue
       :shutdown? (atom false)
       :m (group-by #(select-keys (:broker %) [:host :port]) loops)}))))
