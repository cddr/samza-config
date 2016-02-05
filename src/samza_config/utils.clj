(ns samza-config.utils
  "Encapsulates a few of the samza settings we use regularly. You'll
   probably want to :refer :all this stuff in your config"
  (:require
   [clojure.string :as str]
   [schema.core :as s :refer [Str Int Bool Keyword]]
   [environ.core :refer [env]]
   [samza-config.serde])
  (:import
   [org.apache.samza.task StreamTask InitableTask]
   [org.apache.samza.job.local ThreadJobFactory ProcessJobFactory]
   [org.apache.samza.system.kafka KafkaSystemFactory]
   [org.apache.samza.storage.kv RocksDbKeyValueStorageEngineFactory]
   [samza_config.serde MapSerdeFactory UUIDSerdeFactory]))

;; TODO: Consider giving these helpers their own ns
(def thread-job-factory   {:class (.getName ThreadJobFactory)})
(def process-job-factory  {:class (.getName ProcessJobFactory)})
(def rocks-db-factory     {:class (.getName RocksDbKeyValueStorageEngineFactory)})
(def map-serde-factory    {:class (.getName MapSerdeFactory)})
(def uuid-serde-factory   {:class (.getName UUIDSerdeFactory)})
(def kafka-system-factory {:class (.getName KafkaSystemFactory)})

(defn class-name [class]
  (.getName class))

(defn kafka-system [env]
  {:samza {:factory kafka-system-factory}
   :key {:serde :uuid}
   :msg {:serde :map}
   :consumer {:zookeeper {:connect (str/join ":" [(env :zk-host)
                                                  (env :zk-port)])}}
   :producer {:bootstrap {:servers (str/join ":" [(env :kafka-host)
                                                  (env :kafka-port)])}}})
