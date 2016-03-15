(ns samza-config.test
  (:require
   [clojure.string :as str]
   [samza-config.core :refer [*samza-topic*]]
   [samza-config.serde :refer [local-registry-client]]
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
  (input [this topic message])
  (output [this])
  (trigger-window [this]))

(defn roundtrip [msg serde]
  (try
    (let [as-bytes (.toBytes serde msg)]
      (.fromBytes serde as-bytes))
    (catch Exception e
      (throw (ex-info "Tried but failed to roundtrip message"
                      {:cause e
                       :message msg
                       :serde serde})))))

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

(defn mock-config [config]
  (assoc-in config [:confluent :schema :registry :factory]
            (str #'local-registry-client)))


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
    (let [config (samza-config (mock-config job-config))
          context (mock-task-context job-config)]

      (task-factory config context))))

(defn key-serde [job-config system stream]
  (or (get-in job-config [:systems (keyword system) :streams (keyword stream) :samza :key :serde])
      (get-in job-config [:systems (keyword system) :samza :key :serde])))

(defn msg-serde [job-config system stream]
  (or (get-in job-config [:systems (keyword system) :streams (keyword stream) :samza :msg :serde])
      (get-in job-config [:systems (keyword system) :samza :msg :serde])))

(defn mock-serde [job-config serde-name]
  (-> (clojure.lang.Reflector/invokeConstructor
       (resolve (-> (get-in job-config [:serializers :registry serde-name :class])
                    (symbol)))
       (to-array []))
      (.getSerde serde-name (samza-config job-config))))

(defn mock-collector
  "This feels a little bit wrong to be sharing a collector between several
   jobs but given that we are trying to test a bunch of these things together
   maybe it's not that crazy"
  [config output]
  (reify MessageCollector
    (send [this envelope]
      (let [topic (-> envelope .getSystemStream .getStream)
            msg-serde (mock-serde config (msg-serde config "kafka" topic))]
        (binding [*samza-topic* topic]
          (swap! output update-in
                 [topic]
                 (fnil conj [])
                 (roundtrip (.getMessage envelope) msg-serde)))))))

(defn test-system [job-configs]
  (let [offsets        (atom {})
        output         (atom {})
        coordinator    (mock-coordinator)
        job-tasks      (mapv (juxt mock-config build-task) job-configs)]

    (reify TestSystem
      (input [this topic message]
        (swap! offsets update-in [topic] (fnil inc 0))

        (binding [*samza-topic* topic]
          (let [envelope (fn [msg key-serde msg-serde]
                           (IncomingMessageEnvelope.
                            (SystemStreamPartition. "kafka" (name topic) (Partition. 1))
                            (str (get @offsets topic))
                            (roundtrip (:id msg) key-serde)
                            (roundtrip msg msg-serde)))]

            (doseq [[job task] job-tasks]
              (let [key-serde (some->> (key-serde job "kafka" topic)
                                       (mock-serde job))
                    msg-serde (some->> (msg-serde job "kafka" topic)
                                       (mock-serde job))]
                (if (and key-serde msg-serde)
                  ;; There's a risk that the reason we can't find a key-serde or msg-serde
                  ;; is that the configuration is not set up correctly which might lead to
                  ;; confusing behaviour (i.e. we would not send that job the input it would
                  ;; expect). But we also need a way to not send input to jobs that don't
                  ;; expect it so...
                  (.process task (envelope message
                                           key-serde
                                           msg-serde)
                            (mock-collector job output)
                            coordinator)
                  (println {:msg "Not sending"
                            :topic topic
                            :message message
                            :job job})))))))


      (trigger-window [this]
        (doseq [[job task] job-tasks]
          (.window task (mock-collector job output) coordinator)))

      (output [this]
        @output))))
