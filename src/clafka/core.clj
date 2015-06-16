(ns clafka.core
  "Contains a clojure interface for the Producer and SimpleConsumer api's"
  (:import [kafka.api FetchRequestBuilder OffsetRequest PartitionOffsetRequestInfo]
           [kafka.javaapi TopicMetadataRequest]
           [kafka.javaapi.consumer SimpleConsumer]
           [kafka.common ErrorMapping TopicAndPartition]
           [org.apache.kafka.common.serialization Serializer]
           [org.apache.kafka.clients.producer KafkaProducer Producer
            ProducerRecord RecordMetadata
            Callback])
  (:require [clafka.proto :refer :all]))

(defn ^Serializer kafka-serializer
  [serializer]
  (if (ifn? serializer)
    (reify Serializer
      (serialize [this topic v] (serializer topic v))
      (close [this]))
    serializer))

(defmulti serialize-default (fn [topic v] topic))

(defmethod serialize-default :default
  [topic v]
  v)

(defn producer
  "Pass in a config map for the producer [docs](http://kafka.apache.org/documentation.html#newproducerconfigs)
  and a serializer that takes the topic name and a value, and is expected to return a byte array.

  n.b `serialize-default` can be overridden per topic to specify global serialization behaviour"
  ([config]
   (producer config serialize-default))
  ([config serializer]
   (producer config serializer serializer))
  ([config key-serializer value-serializer]
    (KafkaProducer.
     ^java.util.Map config
     (kafka-serializer key-serializer)
     (kafka-serializer value-serializer))))

(defn producer-record
  "Creates a keyed message for the given topic"
  ([topic k v]
   (ProducerRecord. topic k v)))

(defn record-metadata->map
  [^RecordMetadata rm]
  {:topic (.topic rm)
   :offset (.offset rm)
   :partition (.partition rm)})

(defn publish!
  "Sends a message asynchronously via 'producer', returns a delay that will contain metadata
  about what has been sent. The key and value ought to be compatible with the
  serializer used by the producer."
  ([producer producer-record]
   (let [fut (.send ^Producer producer producer-record)]
     (delay (record-metadata->map @fut))))
  ([producer topic k v]
   (publish! producer (producer-record topic k v))))

(defn ^Callback fn->callback
  [f]
  (reify Callback
    (onCompletion [this rm exc]
      (f (when rm (record-metadata->map rm))
         exc))))

(defn publish-ack!
  "Sends a message asynchronously via 'producer', returns a delay that will contain metadata
  about what has been sent. Accepts a function `f` that will be called when the broker has acknowledged
  receipt of the message.
  The precise acknowledgment semantics will depend on your producer's `acks` setting."
  ([producer producer-record f]
   (let [fut (.send ^Producer producer producer-record (fn->callback f))]
     (delay (record-metadata->map @fut))))
  ([producer topic k v f]
   (publish-ack! producer (producer-record topic k v) f)))

(def ^:dynamic *default-socket-timeout* (* 30 1000))
(def ^:dynamic *default-buffer-size* (* 512 1024))

(defn consumer
  "Creates a SimpleConsumer instance in order to
  1. Query topic metadata with `topic-metadata-request`
  2. Query leadership status with `find-leader` and `find-leaders`
  3. Fetch data with `fetch`, `fetch-log` and `log-seq`
  4. Make offset metadata requests with `offsets` or `offset-at`

  The consumer maintains a tcp connection internally and can be closed with
  `.close` - be warned, there appears to be a bug in kafka 0.8.2 whereby this
  connection is re-opened if you use the consumer again, subsequent .close calls should close it again.

  The connection is automatically reconnected if the socket is closed/timed out
  however, while it is unavailable some operations will return ClosedChannelExceptions"
  ([host port]
   (consumer host port (str "consumer-" (java.util.UUID/randomUUID))))
  ([host port client-id]
   (consumer host port client-id *default-socket-timeout* *default-buffer-size*))
  ([host port client-id socket-timeout buffer-size]
    (SimpleConsumer. host port socket-timeout buffer-size client-id)))

(defn broker->map
  [^kafka.cluster.Broker broker]
  (when broker
    {:host (.host broker)
     :port (.port broker)
     :id (.id broker)}))

(defn partition-metadata->map
  [^kafka.javaapi.PartitionMetadata partition-metadata]
  (when partition-metadata
    {:partition-id (.partitionId partition-metadata)
     :isr (keep broker->map (.isr partition-metadata))
     :leader (broker->map (.leader partition-metadata))
     :replicas (keep broker->map (.replicas partition-metadata))}))

(defn topic-metadata->map
  [^kafka.javaapi.TopicMetadata topic-metadata]
  (when topic-metadata
    {:topic (.topic topic-metadata)
     :partitions (keep partition-metadata->map (.partitionsMetadata topic-metadata))}))

(defn topic-metadata-request
  [consumer topics]
  (->> (.send consumer (TopicMetadataRequest. ^java.util.List topics))
       .topicsMetadata
       (keep topic-metadata->map)))

(def earliest-time
  "A constant that can be used as a `time` value
  for offset requests via `offsets` representing the
  beginning of the log"
  (OffsetRequest/EarliestTime))

(def latest-time
  "A constant that can be used as a `time` value
  for offset requests via `offsets` representing the
  head of the log"
  (OffsetRequest/LatestTime))

(def error-code->kw
  "Maps Kafka error codes to keywords"
  {(ErrorMapping/BrokerNotAvailableCode) :broker-unavailable
   (ErrorMapping/ConsumerCoordinatorNotAvailableCode) :consumer-coordinator-unavailable
   (ErrorMapping/InvalidFetchSizeCode) :invalid-fetch-size
   (ErrorMapping/InvalidMessageCode) :invalid-message
   (ErrorMapping/InvalidTopicCode) :invalid-topic
   (ErrorMapping/LeaderNotAvailableCode) :leader-unavailable
   (ErrorMapping/MessageSetSizeTooLargeCode) :message-set-too-large
   (ErrorMapping/MessageSizeTooLargeCode) :message-too-large
   (ErrorMapping/NoError) nil
   (ErrorMapping/NotCoordinatorForConsumerCode) :not-coordinator-for-consumer
   (ErrorMapping/NotEnoughReplicasAfterAppendCode) :not-enough-replicas-after-append
   (ErrorMapping/NotEnoughReplicasCode) :not-enough-replicas
   (ErrorMapping/NotLeaderForPartitionCode) :not-leader-for-partition
   (ErrorMapping/OffsetMetadataTooLargeCode) :offset-metadata-too-large
   (ErrorMapping/OffsetOutOfRangeCode) :offset-out-of-range
   (ErrorMapping/OffsetsLoadInProgressCode) :offsets-load-in-progress
   (ErrorMapping/ReplicaNotAvailableCode) :replica-unavailable
   (ErrorMapping/RequestTimedOutCode) :request-timed-out
   (ErrorMapping/StaleControllerEpochCode) :stale-controller-epoch
   (ErrorMapping/StaleLeaderEpochCode) :stale-leader-epoch
   (ErrorMapping/UnknownCode) :unknown
   (ErrorMapping/UnknownTopicOrPartitionCode) :unknown-topic-or-partition})


(defn offset-request
  [m time client-id]
  (let [r (into {} (for [[k v] m
                         v v]
                     [(TopicAndPartition. k v)
                      (PartitionOffsetRequestInfo. time 1)]))]
    (kafka.javaapi.OffsetRequest. r (OffsetRequest/CurrentVersion) client-id)))


(defn block
  "Returns a map describing a block of data in a kafka log"
  [topic partition offset size]
  {:topic topic
   :partition partition
   :offset offset
   :size size})

(defn message-and-offset->map
  [^kafka.message.MessageAndOffset mao]
  (when mao
    {:message (when-let [m (.message mao)]
                (let [payload (.payload m)
                      bytes (byte-array (.limit payload))]
                  (.get payload bytes)
                  bytes))
     :next-offset (.nextOffset mao)
     :offset (.offset mao)}))


(defn fetch-response->map
  [^kafka.javaapi.FetchResponse fetch-response blocks]
  (when fetch-response
    {:has-error? (.hasError fetch-response)
     :data (into {} (for [{:keys [topic partition]} blocks]
                      [{:topic topic
                        :partition partition}
                       (let [ec (.errorCode fetch-response topic partition)
                             ^kafka.javaapi.message.ByteBufferMessageSet
                             ms (.messageSet fetch-response topic partition)]
                         {:error (error-code->kw ec ec)
                          :error-code ec
                          :total-bytes (when ms (.sizeInBytes ms))
                          :valid-bytes (when ms (.validBytes ms))
                          :messages
                          (keep message-and-offset->map ms)})]))}))

(defn- add-fetch
  [^FetchRequestBuilder fb {:keys [topic partition offset size]}]
  (.addFetch fb topic partition offset size))

(defn fetch-request
  "Requests the consumer to fetch
  blocks of data from kafka the blocks are described by maps
  in the `blocks` collection, each block is simply a map of :topic, :partition, :offset and :size"
  [^SimpleConsumer consumer blocks]
  (let [fb (FetchRequestBuilder.)
        fb (reduce add-fetch fb blocks)
        fr (.build fb)]
    (-> (.fetch consumer fr)
        (fetch-response->map blocks))))

(extend-type SimpleConsumer

  ICloseable
  (shutdown! [this]
    (.close this))

  IBrokerClient
  (-fetch [this topic partition offset size]
    (let [r (fetch-request this [(block topic partition offset size)])
          data (-> r :data (get {:topic topic :partition partition}))]
      data))
  (-find-topic-metadata [this topics]
    (topic-metadata-request this topics))
  (-find-offsets [this m time]
    (let [r (offset-request m time (.clientId this))
          result (.getOffsetsBefore this r)]
    (into {}
          (for [[k v] m]
            [k (mapv (fn [v] {:offset (first (.offsets result k v))
                             :partition v
                             :error-code (.errorCode result k v)
                             :error (error-code->kw (.errorCode result k v))})
                     v)])))))

(defn find-leaders
  "Finds partition leaders for the given topics"
  [client topics]
  (map #(update-in % [:partitions] (partial map (juxt :partition-id :leader)))
       (-find-topic-metadata client topics)))

(defn find-leader
  "Finds the leader for the given topic partition"
  [client topic partition]
  (first
   (for [{:keys [topic partitions]} (find-leaders client [topic])
         [id leader] partitions
         :when (= (str id) (str partition))]
     leader)))

(defn offsets
  "Make an offset request to determine the offset at `time` in the log
  time is a long value or one of the 2 special constants `earliest-time` or
  `latest-time`. m is a map of {topic [partition]} you will receive back a map
   of {topic [{:offset, :partition}]}"
  [client m time]
  (let [r (-find-offsets client m time)]
    (doseq [[k v] r]
      (when (:error v)
        (throw (ErrorMapping/exceptionFor (:error-code v)))))
    (into {}
          (for [[k v] r]
            [k (mapv #(dissoc % :error :error-code) v)]))))

(defn offset-at
  "Makes a request to find the offset for time `time` in the log
  given by the `topic` and `partition` - you can use the 2 special constants
  `earliest-time` and `latest-time` to ask for the earliest or latest offset"
  [client topic partition time]
  (let [r (offsets client {topic [partition]} time)]
    (-> r (get topic) first :offset)))

(defn earliest-offset
  "Finds the earliest offset in the log"
  [client topic partition]
  (offset-at client topic partition earliest-time))

(defn latest-offset
  "Finds the latest offset in the log"
  [client topic partition]
  (offset-at client topic partition latest-time))


(def ^:dynamic *default-fetch-size* (* 1024 512))

(defn fetch
  "Fetches a single block of data from kafka, throws exceptions for broker errors"
  ([client topic partition offset]
   (fetch client topic partition offset *default-fetch-size*))
  ([client topic partition offset size]
    (let [partition (Long/valueOf (str partition))
          data (-fetch client topic partition offset size)]
      (if (:error data)
        (throw (ErrorMapping/exceptionFor (:error-code data)))
        data))))

(defn log-head?
  "Are we at the log head?"
  [block]
  (and (not (:error block))
       (empty? (:messages block))
       (= 0
          (:total-bytes block)
          (:valid-bytes block))))

(defn next-block-offset
  "Find the starting offset from the block if possible (nil if not)"
  [block]
  (:next-offset (last (:messages block))))

(defn fetch-log
  "Fetch the entire log from the given offset,
  lazily fetches in batches of `size` bytes (default 512KB).
  Returns a seq of messages.
  Messages greater in size than `size` in total will be omitted"
  ([client topic partition offset]
   (fetch-log client topic partition offset *default-fetch-size*))
  ([client topic partition offset size]
   (lazy-seq
    (let [data (fetch client topic partition offset size)]
      (concat (:messages data)
              (if (log-head? data)
                nil
                (fetch-log client topic partition
                           (or (next-block-offset data)
                               (inc offset))
                           size)))))))

(defn poll-from-offset
  "Fetches a block of data from the kafka log, unless
  the offset is at the head of the log, in which case it will wait and poll until a block
  can be received every `poll-ms`"
  [client topic partition offset size poll-ms]
  (let [data (fetch client topic partition offset size)]
    (if (log-head? data)
      (do (Thread/sleep poll-ms)
          (recur client topic partition offset size poll-ms))
      data)))

(def ^:dynamic *default-poll-ms* 1000)

(defn log-seq
  "Returns an infinite seq of messages from offset, requests blocks of data
  from kafka in blocks of `:size` (512KB by default)
  Messages greater in size than `:size` in total will be omitted
  once the log has been exhausted will enter a polling mode whereby it tries to retrieve new
  blocks every `:poll-ms` millis, (1000 by default)"
  ([client topic partition offset]
   (log-seq client topic partition offset {}))
  ([client topic partition offset {:keys [size poll-ms]
                                     :or {size *default-fetch-size*
                                          poll-ms *default-poll-ms*}
                                     :as opts}]
   (lazy-seq
    (let [data (poll-from-offset client topic partition offset size poll-ms)
          messages (:messages data)]
      (concat messages
              (log-seq client topic partition
                       (or (next-block-offset data)
                           (inc offset))
                       opts))))))
