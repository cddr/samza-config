(ns samza-config.rewriters
  (:require
   [clojure.edn :as edn]
   [environ.core :refer [env]]
   [samza-config.job :refer [job-config]])
  (:import
   [org.apache.samza.config MapConfig ConfigRewriter]))

(defn flatten-map
  "Flattens a nested map"
  ([form]
     (into {} (flatten-map form nil)))
  ([form pre]
     (mapcat (fn [[k v]]
               (let [prefix (if pre
                              (conj pre k)
                              [k])]
                 (if (map? v)
                   (flatten-map v prefix)
                   [[prefix v]])))
             form)))

(def job-rewriter
  "Returns a samza config derived from a `defjob` spec"
  (reify
    ConfigRewriter
    (rewrite [this name config]
      (let [job (-> (.get config "job.var")
                    symbol
                    find-var)
            config (job-config job)]
        (->> (flatten-map config)
             (apply hash-map)
             (MapConfig.))))))

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
