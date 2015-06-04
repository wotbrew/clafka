# tiny-kafka

###Concept

Provide the simplest possible consumer and producer interfaces for kafka by exposing the new java producer api and the simple consumer.

It is designed to be used as the basis for more sophisticated consumers whose needs are not met 
by the default zookeeper consumer in kafka.
    
## Usage

```clojure 
[mixrad.io/tiny-kafka "0.1.0"]
```
```clojure
(require '[tiny-kafka.core :refer :all])
```

### Consumer
I have only provided a wrapper for the simple consumer, a good zookeeper consumer api can be found in [clj-kafka](http://github.com/pingles/clj-kafka)

First create a `SimpleConsumer` instance with `consumer`

```clojure
(def c (consumer "localhost" 9092))
;;close a consumer with
(.close c)
```

The `SimpleConsumer` talks to a single broker, and can only receive data for partitions on which that
broker is the leader.

However, you can ask any broker which broker is the leader for a partition in your cluster via
`find-leader` or `find-leaders`

#### Finding a leader
```clojure 
(find-leader c "my-topic" 0)
;;=>
{:host "localhost" :port 9092 :id 0}
```

Lets assume the existing consumer is talking to the broker that owns partition *0*. If it wasn't 
then you would have to use `consumer` to give you a `SimpleConsumer` for that broker.

#### Fetching
You can fetch some data from a log with `fetch`

```clojure
;;fetches 1024 bytes of data from my-topic partition 0, offset 0.
(fetch c "my-topic" 0 0 1024)
;; =>
```
`fetch` will throw exceptions for the various 
kafka error codes given by the `ErrorMapping` class in kafka, if you do not want this and you want to manually check error-codes etc you can do so with the underlying `-fetch` fn.

So for example, if the leader for a partition changes, you should expect `fetch` to throw a 
kafka.common.LeaderNotA

#### Fetching a seq

You can fetch a lazy seq of the entire log until the current head with `fetch-log`
```clojure
;;fetches a seq of messages lazily from the log in blocks of *default-fetch-size*
;;from my-topic partition 0 offset 0
(fetch-log c "my-topic" 0 0) 
;;=>
;;you can also manually specify the fetch size to use... (here we say 1024 bytes)
(fetch-log c "my-topic" 0 0 1024)
;;=>
```

You can also return an infinite seq of messages with `log-seq`, this sequence does not 
terminate when the log is exhausted, rather it enters a polling mode allowing you to block on new messages being added to the log over time.

```clojure
;;will use the default size and poll time parameters (512KB and 1 second)
(log-seq c "my-topic" 0 0)
;;=>

;;you can manually specify the size and poll-ms through a configuration map
(log-seq c "my-topic 0 0 {:size 1024, :poll-ms 2000})
``` 
 
`fetch-log` and `fetch-seq` will skip messages that are too large to be fetched with a single fetch,
so tune the fetch-size carefully. The default fetch size is 512KB which should be plenty 
for most use cases.

### Producer 

You can produce messages using the `KafkaProducer` api.

Create a producer using configuration as specified: [docs](http://kafka.apache.org/documentation.html#newproducerconfigs)
```clojure
(def p (producer {"bootstrap.servers" "localhost:9092,localhost:9093"}))
```
** NB ** - The config options are specified in the properties style, so always use strings!

Then publish a message using  `publish!`

```clojure
(publish! p "topic-a" (.getBytes "some-key") (.getBytes "hello world!"))
;;close a producer with 
(.close p)
```

By default `publish!` will take byte arrays for the key and value. If you want you can use the `KafkaProducer` serialization mechanism by specifying a pair of either functions or `Serializer` instances when you create the producer.

```clojure
;;using a pair of functions, one for the key and the latter for the value
(def p2 (producer {"bootstrap.servers" "localhost:9092,localhost:9093"} 
                  (fn [topic v] (.getBytes v))
                  (fn [topic v] (.getBytes v))))
                  
;;using just a single function for both the key the value
(def p3 (producer {"bootstrap.servers" "localhost:9092,localhost:9093"}
                  (fn [topic v] (.getBytes v))))
```

### Contributing

Low hanging fruit:
- There are no type hints!
- More tests would be good
- Keep the library simple, its not designed as competition for java api's or clj-kafka, its just a wrapper!

## License
Copyright © 2015 MixRadio

[mr-edda is released under the 3-clause license ("New BSD License" or "Modified BSD License").](http://github.com/mixradio/tiny-kafka/blob/master/LICENSE)

