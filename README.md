# clafka

A clojure kafka client focusing on the simple consumer.

![latest clafka version](https://clojars.org/mixradio/clafka/latest-version.svg)

### Concept

To provide the simplest possible consumer and producer interfaces for [kafka](http://kafka.apache.org/documentation.html) by exposing the new java producer api and the simple consumer.

It is designed to be used as the basis for more sophisticated consumers whose needs are not met 
by the default zookeeper consumer in kafka.

### Docs

API docs can be found [here](http://danstone.github.io/clafka)
    
## Usage

Include the following to your lein `project.clj` dependencies

```clojure 
[mixradio/clafka "0.2.3"]
```

Most functions are found in `clafka.core`

```clojure
(require '[clafka.core :refer :all])
```
### Clients

The `IBrokerClient` protocol is the core abstraction of clafka.

You will see functions that accept an `IBrokerClient` use the name `client`, as opposed to low-level fns
that may require a `consumer`. Most functions require a `client`.

The kafka `SimpleConsumer` is one implementation of `IBrokerClient`. A pooled implementation is also provided.

#### SimpleConsumer Client
I have provided a wrapper for the simple consumer, a good zookeeper consumer api can be found in [clj-kafka](http://github.com/pingles/clj-kafka)

First create a `SimpleConsumer` instance with `consumer`

```clojure
(def c (consumer "localhost" 9092))
;;close a consumer with
(.close c)
```

The `SimpleConsumer` talks to a single broker, and can only receive data for partitions on which that
broker is the leader.


#### Pooled Client

You can use a pooled client in order to:
- Load balance requests over many client/consumer instances
- Support fetching data without worrying about which broker leads a partition.

Find the pooled client in `clafka.pool`.

```clojure
(require '[clafka.pool :refer [pool])
(require '[clafka.proto :refer [shutdown!]])

;; p will balance requests over 2 clients for each listed broker for a total of 4 clients.
(def p (pool [{:host "localhost" :port 9092} {:host "localhost", :port "9093"}] 2))
;; you can use all the regular clafka functions with the pooled client.
(log-seq p "my-topic" 0 0)
;; =>
({:message #<byte[] [B@755df882>
  :offset 0 
  :next-offset 1}, ...)
  
;;close the pooled client with shutdown!, this will attempt to close all underlying clients.
(shutdown! p)
```

By default the pool will create `SimpleConsumer` instances for each listed broker, but you can override
this behaviour by providing a :factory as part of the `pool` config map. See the docstring for more details.

Be warned, you can still get `NotLeaderForPartitionException`'s if leaders change as you are making a request, this 
should be rare however. You should expect subsequent requests to reflect the new leader, so simply retry in these cases.

#### Finding a leader

You can ask any broker which broker is the leader for a partition in your cluster via
`find-leader` or `find-leaders`

```clojure 
(find-leader c "my-topic" 0)
;;=>
{:host "localhost" :port 9092 :id 0}
```

#### Fetching
You can fetch some data from a log with `fetch`

```clojure
;;fetches 1024 bytes of data from topic "my-topic" partition 0, offset 0.
(fetch c "my-topic" 0 0 1024)
;; =>
{:messages [{:message #<byte[] [B@755df882>
             :offset 0 
             :next-offset 1}]
 :total-bytes 1024 
 :valid-bytes 124
 :error nil 
 :error-code 0}
```
`fetch` will throw exceptions for the various 
kafka error codes given by the `ErrorMapping` class in kafka, if you do not want this and you want to manually check error-codes etc you can do so with the underlying `-fetch` fn.

So for example, if the leader for a partition changes, you should expect `fetch` to throw a 
`kafka.common.NotLeaderForPartitionException`.

At which point, if you wanted to continue fetching from that partition, you would have to construct a 
new `consumer`.

#### Fetching a seq

You can fetch a lazy seq of the entire log until the current head with `fetch-log`
```clojure
;;fetches a seq of messages lazily from the log in blocks of *default-fetch-size*
;;from topic "my-topic" partition 0 offset 0
(fetch-log c "my-topic" 0 0) 
;;=>
({:message #<byte[] [B@755df882>
  :offset 0 
  :next-offset 1}, ...)

;;you can also manually specify the fetch size to use... (here we say 1024 bytes)
(fetch-log c "my-topic" 0 0 1024)
```

You can also return an infinite seq of messages with `log-seq`, this sequence does not 
terminate when the log is exhausted, rather it enters a polling mode allowing you to block on new messages being added to the log over time.

```clojure
;;will use the default size and poll-ms parameters (512KB and 1 second)
(log-seq c "my-topic" 0 0)
;;=>
({:message #<byte[] [B@755df882>
  :offset 0 
  :next-offset 1}, ...)

;;you can manually specify the size and poll-ms through a configuration map
(log-seq c "my-topic 0 0 {:size 1024, :poll-ms 2000})
``` 
 
`fetch-log` and `log-seq` will skip messages that are too large to be fetched with a single fetch,
so tune the fetch-size carefully. The default fetch size is 512KB which should be plenty 
for most use cases.

### Producer 

You can produce messages using the `KafkaProducer` api.

Create a producer using configuration as specified: [docs](http://kafka.apache.org/documentation.html#newproducerconfigs)
```clojure
(def p (producer {"bootstrap.servers" "localhost:9092,localhost:9093"}))

;;close a producer with 
(.close p)
```
** NB ** - The config options are specified in the properties style, so always use strings!

Then publish a message using  `publish!`

```clojure
(publish! p "my-topic" (.getBytes "some-key") (.getBytes "hello world!"))

;;publish returns a delay that can returns some metadata about the publish 
@*1 
;;=> 
{:topic "my-topic", :offset 0, :partition 0}

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

### Other features

- You can utilise broker acknowledgment on publish using `publish-ack!`
- You can make requests for offsets at given times using `offsets`, `offset-at`, `earliest-offset` and `latest-offset`

### Contributing

PR's welcome!

Low hanging fruit:
- There are few type hints!
- Anything that I have missed feature wise relating to the SimpleConsumer and KafkaProducer.
- More tests would be good

## License

[clafka is released under the 3-clause license ("New BSD License" or "Modified BSD License").](http://github.com/danstone/clafka/blob/master/LICENSE)

