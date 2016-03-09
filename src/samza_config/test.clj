(ns samza-config.test
  (:require
   [clojure.string :as str]
   [samza-config.core :refer [*samza-system-name* *samza-stream-name*]]
   [samza-config.job :refer [samza-config]])
  (:import
   [io.confluent.kafka.schemaregistry.client LocalSchemaRegistryClient]
   [org.apache.samza.config MapConfig]
   [org.apache.samza.job JobRunner]
   [org.apache.samza.storage.kv KeyValueStore KeyValueIterator Entry]
   [org.apache.samza.task MessageCollector TaskContext TaskCoordinator]
   [org.apache.samza Partition]
   [org.apache.samza.system SystemStream SystemStreamPartition IncomingMessageEnvelope]))

(defprotocol TestSystem
  (input [this system topic message])
  (output [this topic]))

(defn mock-collector [output]
  (reify MessageCollector
    (send [this envelope]
      (swap! output update-in
             [(-> envelope .getSystemStream .getStream)]
             conj
             (.getMessage envelope)))))

(defn mock-kv-store []
  (let [store (atom {})]
    (reify KeyValueStore
      (get [this k]
        (let [result (get @store k)]
          result))

      (put [this k v]
        (swap! store assoc k v))

      (delete [this k]
          (swap! store dissoc k))

      (getAll [this ks]
          (map #(get @store %) ks))

      (putAll [this entries]
          (doseq [e entries]
            (swap! store assoc (.getKey e) (.getValue e))))

      (deleteAll [this ks]
          (doseq [k ks]
            (.delete this k)))

      (all [this]
        (let [iterator (.iterator (map (fn [[k v]] (Entry. k v)) @store))]
          (reify
            KeyValueIterator
            (hasNext [this] (.hasNext iterator))
            (next [this] (.next iterator)))))

      (range [this from to]
          (filter (fn [[k v]]
                    (and (<= k to)
                         (>= k from)))
                  @store)))))

(defn mock-coordinator []
  (reify TaskCoordinator
    (commit [this scope])
    (shutdown [this scope])))

(defn mock-task-context [job-config]
  (let [stores (->> (:stores job-config)
                    (map (fn [[store-name store-serdes]]
                           ;; TODO:
                           ;;
                           ;; It wouldn't be hard to put objects through whatever serializer
                           ;; is defined for the store.
                           [(name store-name) (mock-kv-store)]))
                    (mapcat identity)
                    (apply hash-map))]
    (reify
      TaskContext
      (getStore [this store]
        (get stores store)))))

(defn build-task [job-config]
  (let [task-factory (-> (get-in job-config [:job :task :factory])
                         read-string
                         eval)]
    (let [config (samza-config job-config)
          context (mock-task-context job-config)]

      (println "build-task: ")
      (doseq [[k v] (into (sorted-map) config)]
        (println (format "  %s = %s" k v)))

      (task-factory config context))))

(defn key-serde [job-config system stream]
  (or (get-in job-config [:systems system :streams stream :samza :key :serde])
      (get-in job-config [:systems system :samza :key :serde])))

(defn msg-serde [job-config system stream]
  (or (get-in job-config [:systems system :streams stream :samza :msg :serde])
      (get-in job-config [:systems system :samza :msg :serde])))

(defn mock-serde [job-config serde-name]
  (-> (clojure.lang.Reflector/invokeConstructor
       (resolve (-> (get-in job-config [:serializers :registry serde-name :class])
                    (symbol)))
       (to-array []))
      (.getSerde serde-name (samza-config job-config))))

(defn roundtrip [msg serde]
  (let [as-bytes (.toBytes serde msg)]
    (.fromBytes serde as-bytes)))

(defn test-system [job-configs]
  (let [offsets        (atom {})
        output         (atom {})
        collector      (mock-collector output)
        coordinator    (mock-coordinator)
        job-tasks      (mapv (juxt identity build-task) job-configs)

        ;; TODO: This seems like it shouldn't be here but it is convenient
        ;;       for now
        registry       (LocalSchemaRegistryClient.)]

    (reify TestSystem
      (input [this system topic message]
        (swap! offsets update-in [topic] (fnil inc 0))

        (let [envelope (fn [msg key-serde msg-serde]
                         (IncomingMessageEnvelope.
                          (SystemStreamPartition. (str system) topic (Partition. 1))
                          (str (get @offsets topic))
                          (roundtrip (:id msg) key-serde)
                          (roundtrip msg msg-serde)))]

          (doseq [[job task] job-tasks]
            (let [key-serde (mock-serde job (key-serde job system topic))
                  msg-serde (mock-serde job (msg-serde job system topic))]
              (binding [*samza-system-name* system
                        *samza-stream-name* topic]

                (.process task (envelope message
                                         key-serde
                                         msg-serde)
                          collector
                          coordinator))))))


      (output [this topic]
        (get @output topic)))))
