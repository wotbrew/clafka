(ns clafka.proto)

(defprotocol IBrokerClient
  (-fetch [this topic partition offset size])
  (-find-topic-metadata [this topics])
  (-find-offsets [this m time]))

(defprotocol ICloseable
  (shutdown! [this] "closes the resource"))

(extend-protocol ICloseable
  Object
  (shutdown! [this]
    (try
      (.close this)
      (catch IllegalArgumentException e
        (throw (Exception. "No .close method, please implement ICloseable manually"))))))

(defn try-shutdown!
  [x]
  (try
    (shutdown! x)
    (catch Throwable e
      nil)))
