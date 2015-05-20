(ns tiny-kafka.core
  (:import [org.apache.kafka.common.serialization Serializer]
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
