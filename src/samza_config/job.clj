(ns samza-config.job
  "This namespace includes the `defjob` macro together with a number of helpers
   and shortcuts for samza factories. The intention is to make job specs a little
   more readable than the standard Samza configuration"
  (:require
   [environ.core :refer [env]]
   [clojure.java.io :as io :refer [file]]
   [clojure.string :as str]
   [samza-config.serde])
  (:import
   [samza_config.serde AvroSerdeFactory UUIDSerdeFactory EDNSerdeFactory]
   [io.confluent.kafka.schemaregistry.client CachedSchemaRegistryClient]
   [org.apache.samza.config MapConfig]
   [org.apache.samza.system.kafka KafkaSystemFactory]
   [org.apache.samza.job.local ThreadJobFactory ProcessJobFactory]
   [org.apache.samza.storage.kv RocksDbKeyValueStorageEngineFactory]
   [org.apache.samza.system.kafka KafkaSystemFactory]
   [org.apache.samza.task StreamTask InitableTask]
   [org.apache.samza.config ConfigFactory]
   [org.apache.samza.job JobRunner]))

(def thread-job-factory   {:class (.getName ThreadJobFactory)})
(def process-job-factory  {:class (.getName ProcessJobFactory)})
(def rocks-db-factory     {:class (.getName RocksDbKeyValueStorageEngineFactory)})

(def avro-serde-factory   {:class (.getName AvroSerdeFactory)})
(def uuid-serde-factory   {:class (.getName UUIDSerdeFactory)})
(def edn-serde-factory    {:class (.getName EDNSerdeFactory)})

(def schema-registry-client {:class (.getName CachedSchemaRegistryClient)})

(defn full-name [sym]
  (str sym))

(defn job-name [sym]
  (str (ns-name *ns*)
       "/"
       (str sym)))

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

(defn input-streams [& streams]
  (apply hash-map (mapcat identity streams)))

(defn input-topic [topic key-serde msg-serde]
  [(keyword topic)
   {:key {:serde (name key-serde)}
    :msg {:serde (name msg-serde)}}])

(def ^:dynamic *system*)

(defn task-inputs [& inputs]
  (str/join "," (map #(str *system* "." %) inputs)))

(defn zookeeper-config [env]
  {:zookeeper {:connect (:zookeeper-address env)}})

(defn kafka-config [env]
  {:bootstrap {:servers (:kafka-address env)}})

(defn job-coordinator [system replication-factor]
  {:system "kafka"
   :replication {:factor "1"}})

(defn local-stores [& stores]
  (apply hash-map (mapcat identity stores)))

(defn key-value-store [store key-serde msg-serde]
  [(keyword store)
   {:key {:serde (name key-serde)}
    :msg {:serde (name msg-serde)}
    :factory "org.apache.samza.storage.kv.RocksDbKeyValueStorageEngineFactory"
    :change-log (str store "-changelog")}])

(defn kafka-system
  [env & [key-serde msg-serde]]
  (let [samza (cond-> {:factory (.getName KafkaSystemFactory)}
                key-serde (assoc :key {:serde (name key-serde)})
                msg-serde (assoc :msg {:serde (name msg-serde)}))]
    {:samza samza
     :consumer {:zookeeper {:connect (env :zookeeper-connect)}}
     :producer {:bootstrap {:servers (env :kafka-brokers)}}}))

(defn schema-registry
  [env]
  {:schema
   {:resolver (env :schema-resolver)
    :registry (merge {:url (env :schema-registry-url)
                      :factory (env :schema-registry-factory)})}})

(defn samza-config [job]
  (let [propertize-keys (fn [[path value]]
                          [(str/join "." (map name path)) value])]
    (->> (flatten-map job)
         (mapcat propertize-keys)
         (apply hash-map)
         (MapConfig.))))

(defmacro defjob
  "Define a samza job. This is a convenience wrapper around samza-config. It just
   registers the job by name so that it can be found by `find-job`

   Supported parameters:-

     :task
     :job-factory
     :inputs
     :outputs
     :serializers
     "
  [job-sym version & body]
  `(binding [*system* ~(str job-sym)]
     (def ~job-sym (merge-with merge
                                {:job {:name ~(job-name job-sym)}}
                                (hash-map ~@body)))))
