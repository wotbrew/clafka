(defproject mixradio/clafka "0.2.3-SNAPSHOT"
  :description "The simplest possible way to read and produce messages for kafka"
  :url "http://github.com/mixradio/clafka"
  :license "https://github.com/mixradio/clafka/blob/master/LICENSE"
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.apache.kafka/kafka_2.9.2 "0.8.2.1"
                  :exclusions [[com.sun.jmx/jmxri]
                               [com.sun.jdmk/jmxtools]]]]

  :profiles {:dev {:plugins [[codox "0.8.11"]]
                   :codox {:src-dir-uri "http://github.com/mixradio/clafka/blob/0.2.2/"
                           :src-linenum-anchor-prefix "L"
                           :defaults {:doc/format :markdown}}}})
