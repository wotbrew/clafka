(defproject mixradio/tiny-kafka "0.1.0-SNAPSHOT"
  :description "The simplest possible way to read and produce messages for kafka"
  :url "http://github.com/mixradio/tiny-kafka"
  :license "https://github.com/mixradio/tiny-kafka/blob/master/LICENSE"
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.apache.kafka/kafka_2.9.2 "0.8.2.1"
                  :exclusions [[com.sun.jmx/jmxri]
                               [com.sun.jdmk/jmxtools]]]])
