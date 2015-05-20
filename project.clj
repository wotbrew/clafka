(defproject tiny-kafka "0.1.0-SNAPSHOT"
  :description "The simplest possible way to read messages and produce them for kafka"
  :url "http://github.com/danstone/tiny-kafka"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.apache.kafka/kafka_2.9.2 "0.8.2.1"
                  :exclusions [[com.sun.jmx/jmxri]
                               [com.sun.jdmk/jmxtools]]]])
