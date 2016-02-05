(ns samza-config.utils
  "Encapsulates a few of the samza settings we use regularly. You'll
   probably want to :refer :all this stuff in your config"
  (:require
   [clojure.string :as str]
   [samza-config.serde]
   [samza-config.rewriters]
   [schema.core :as s :refer [Str Int Bool Keyword]]
   [environ.core :refer [env]])
  (:import
   [samza_config.rewriters ClojureReaderRewriter]
   [samza_config.serde MapSerdeFactory UUIDSerdeFactory]
   [org.apache.samza.system.kafka KafkaSystemFactory]
   [org.apache.samza.storage.kv RocksDbKeyValueStorageEngineFactory]
   [org.apache.samza.job.local ThreadJobFactory ProcessJobFactory]))

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
