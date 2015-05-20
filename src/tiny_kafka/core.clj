(ns tiny-kafka.core
  (:import [kafka.api FetchRequestBuilder]
           [kafka.javaapi TopicMetadataRequest]
           [kafka.javaapi.consumer SimpleConsumer]
           [kafka.common ErrorMapping]
           [org.apache.kafka.common.serialization Serializer]
           [org.apache.kafka.clients.producer KafkaProducer Producer
            ProducerRecord RecordMetadata
            Callback]))

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
  "Pass in a config map for the producer (docs)[http://kafka.apache.org/documentation.html#newproducerconfigs]
  and a serializer that takes the topic name and a value, and is expected to return a byte array.

  n.b `serialize-default` can be override per topic to specify global serialization behaviour"
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
   about what has been sent."
  ([producer producer-record]
   (let [fut (.send ^Producer producer producer-record)]
     (delay (record-metadata->map @fut))))
  ([producer topic k v]
   (publish! producer (producer-record topic k v))))

(defn ^Callback fn->callback
  "Transforms a clojure fn taking a record map and an exception
   as arguments (either/or) into a kafka Callback instance"
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

(defn consumer
  "Creates a SimpleConsumer instance in order to
  1. Query topic metadata with `topic-metadata-request`
  2. Query leadership status with `find-leader` and `find-leaders`
  3. Fetch data with `fetch`, `fetch-log` and `log-seq`

  The consumer maintains a tcp connection internally and can be closed with
  `.close`

  The connection is automatically reconnected if the socket is closed/timedout
  however, while it is unavailable some operations will return ClosedChannelExceptions"
  [host port socket-timeout buffer-size client-id]
  (SimpleConsumer. host port socket-timeout buffer-size client-id))

(defn broker->map
  [broker]
  (when broker
    {:host (.host broker)
     :port (.port broker)
     :id (.id broker)}))

(defn partition-metadata->map
  [partition-metadata]
  (when partition-metadata
    {:partition-id (.partitionId partition-metadata)
     :isr (keep broker->map (.isr partition-metadata))
     :leader (broker->map (.leader partition-metadata))
     :replicas (keep broker->map (.replicas partition-metadata))}))

(defn topic-metadata->map
  [topic-metadata]
  (when topic-metadata
    {:topic (.topic topic-metadata)
     :partitions (keep partition-metadata->map (.partitionsMetadata topic-metadata))}))

(defn topic-metadata-request
  [consumer topics]
  (->> (.send consumer (TopicMetadataRequest. ^java.util.List topics))
       .topicsMetadata
       (keep topic-metadata->map)))

(defn block
  "Returns a map describing a block of data in a kafka log"
  [topic partition offset size]
  {:topic topic
   :partition partition
   :offset offset
   :size size})

(defn message-and-offset->map
  [mao]
  (when mao
    {:message (when-let [m (.message mao)]
                (let [payload (.payload m)
                      bytes (byte-array (.limit payload))]
                  (.get payload bytes)
                  bytes))
     :next-offset (.nextOffset mao)
     :offset (.offset mao)}))

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

(defn fetch-response->map
  [fetch-response blocks]
  (when fetch-response
    {:has-error? (.hasError fetch-response)
     :data (into {} (for [{:keys [topic partition]} blocks]
                      [{:topic topic
                        :partition partition}
                       (let [ec (.errorCode fetch-response topic partition)
                             ms (.messageSet fetch-response topic partition)]
                         {:error (error-code->kw ec ec)
                          :error-code ec
                          :total-bytes (when ms (.sizeInBytes ms))
                          :valid-bytes (when ms (.validBytes ms))
                          :messages
                          (keep message-and-offset->map ms)})]))}))

(defn ^:private add-fetch
  [fb {:keys [topic partition offset size]}]
  (.addFetch fb topic partition offset size))

(defn fetch-request
  "Requests the consumer to fetch
  blocks of data from kafka the blocks are described by maps
  in the `blocks` collection, each block is simply a map of :topic, :partition, :offset and :size"
  [consumer blocks]
  (let [fb (FetchRequestBuilder.)
        fb (reduce add-fetch fb blocks)
        fr (.build fb)]
    (-> (.fetch consumer fr)
        (fetch-response->map blocks))))

(defn find-leaders
  "Finds partition leaders for the given topics"
  [consumer topics]
  (map #(update-in % [:partitions] (partial map (juxt :partition-id :leader)))
       (topic-metadata-request consumer topics)))

(defn find-leader
  "Finds the leader for the given topic partition"
  [consumer topic partition]
  (first
   (for [{:keys [topic partitions]} (find-leaders consumer [topic])
         [id leader] partitions
         :when (= id partition)]
     leader)))

(defn fetch*
  "Fetches a single block of data from kafka, will only throw an exception
  if no response is received from the broker, for broker error codes, they will be returned
  as :error-code and as a keyword :error which may be more useful at a glance."
  [consumer topic partition offset size]
  (let [r (fetch-request consumer [(block topic partition offset size)])
        data (-> r :data (get {:topic topic :partition partition}))]
    data))

(defn fetch
  "Fetches a single block of data from kafka, throws exceptions for broker errors"
  [consumer topic partition offset size]
  (let [data (fetch* consumer topic partition offset size)]
    (if (:error data)
      (throw (ErrorMapping/exceptionFor (:error-code data)))
      data)))

(defn fetch-log
  "Fetch the entire log from the given offset,
  lazily fetches in batches of `size` bytes (default 512KB) returns a seq of messages.
  Messages greater in size than `size` in total will be omitted"
  ([consumer topic partition offset]
   (fetch-log consumer topic partition offset (* 1024 512)))
  ([consumer topic partition offset size]
   (lazy-seq
    (let [data (fetch* consumer topic partition offset size)]
      (case (:error data)
        nil (concat (:messages data)
              (fetch-log consumer topic partition
                         (or (last (map :next-offset (:messages data)))
                             (inc offset))
                         size))
        :offset-out-of-range nil
        (throw (ErrorMapping/exceptionFor (:error-code data))))))))

(defn log-seq
  "Returns an infinite seq of messages from offset, requests blocks of data
  from kafka in blocks of `:size` (512KB by default)
  Messages greater in size than `:size` in total will be omitted
  once the log has been exhausted will enter a polling mode where it tries to retrieve new
  blocks every `:poll-ms` millis, (1000 by default)"
  ([consumer topic partition offset]
   (fetch-seq consumer topic partition offset {}))
  ([consumer topic partition offset {:keys [size poll-ms]
                                     :or {size (* 1024 512)
                                          poll-ms 1000}
                                     :as opts}]
   (lazy-seq
    (let [data (fetch* consumer topic partition offset size)]
      (case (:error data)
        nil (concat (:messages data)
                    (log-seq consumer topic partition
                             (last (map :next-offset (:messages data)))
                             size))
        :offset-out-of-range (do (Thread/sleep poll-ms)
                                 (log-seq consumer topic partition offset size))
        (throw (ErrorMapping/exceptionFor (:error-code data))))))))
