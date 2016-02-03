(ns samza-config.utils
  "Encapsulates a few of the samza settings we use regularly. You'll
   probably want to :refer :all this stuff in your config"
  (:require
   [clojure.string :as str]
   [samza-config.serde]
   [samza-config.rewriters]
   [schema.core :as s :refer [Str Int Bool Keyword]])
  (:import
   [samza_config.rewriters ClojureReaderRewriter]
   [samza_config.serde MapSerdeFactory UUIDSerdeFactory]
   [org.apache.samza.system.kafka KafkaSystemFactory]
   [org.apache.samza.storage.kv RocksDbKeyValueStorageEngineFactory]
   [org.apache.samza.job.local ThreadJobFactory ProcessJobFactory]))

;; Job Factories
(def thread-job-factory {:class (.getName ThreadJobFactory)})
(def process-job-factory {:class (.getName ProcessJobFactory)})

;; Storage Factories
(def rocks-db-factory {:class (.getName RocksDbKeyValueStorageEngineFactory)})

;; Serde Factories
(def map-serde-factory {:class (.getName MapSerdeFactory)})
(def uuid-serde-factory {:class (.getName UUIDSerdeFactory)})

;; System Factories
(def kafka-system-factory {:class (.getName KafkaSystemFactory)})



(defn class-name [class]
  (.getName class))

;; Rewriters
(def clojure-config (class-name ClojureReaderRewriter))


(defn kafka-system [env]
  {:samza {:factory kafka-system-factory}
   :key {:serde :uuid}
   :msg {:serde :map}
   :consumer {:zookeeper {:connect "#=(str/join \":\" [(env :zk-host) (env :zk-port)]"}}
   :producer {:bootstrap {:servers (str/join ":" [(env :kafka-host)
                                                  (env :kafka-port)])}}})
