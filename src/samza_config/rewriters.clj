(ns samza-config.rewriters
  (:require
   [clojure.edn :as edn]
   [environ.core :refer [env]])
  (:import
   [org.apache.samza.config MapConfig ConfigRewriter]))

;; The ClojureRewriter has Clojure `read-string` each config value. This
;; makes it possible to set some parameters (e.g. kafka/zookeeper hosts)
;; at runtime using environment variables or other dynamic methods.
;;
;; e.g.
;;
;; systems.kafka.consumer.zookeeper.connect=#=(env :zk-host)
;; systems.kafka.producer.bootstrap.servers=#=(env :kafka-host)

(defrecord ClojureReaderRewriter []
  ConfigRewriter
  (rewrite [this name config]
    (let [read-val (fn [k]
                     ;; This is only OK if config can be trusted not to
                     ;; be malicious
                     [k (str (read-string (.get config k)))])]
      (->> (mapcat read-val (.getSet config))
           (apply hash-map)
           (MapConfig.)))))
