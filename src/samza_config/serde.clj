(ns samza-config.serde
  (:require
   [samza-config.core :refer [*samza-stream-name*]]
   [abracad.avro :as a]
   [clojure.string :as str]
   [clojure.edn :as edn]
   [inflections.core :refer [hyphenate]])
  (:import
   [org.apache.samza.serializers Serde SerdeFactory]
   [org.apache.samza.system SystemStream OutgoingMessageEnvelope]
   [java.io ByteArrayOutputStream DataOutputStream]
   [kafka.serializer Encoder Decoder]
   [io.confluent.kafka.schemaregistry.client CachedSchemaRegistryClient LocalSchemaRegistryClient]
   [org.apache.avro.io DatumWriter]))

(def magic                 (byte 0x0))
(def id-size               4)
(def registry-cache-size   100)
(def schema-registry-url   (System/getenv "SCHEMA_REGISTRY_URL"))

(defn registry-client [config]
  (let [registry-url (-> (.get config "confluent.schema.registry.url"))
        registry-capacity (or (-> (.get config "confluent.schema.registry.capacity")
                                  edn/read-string)
                              100)]
    (CachedSchemaRegistryClient. registry-url registry-capacity)))

(defn local-registry-client [_config]
  (LocalSchemaRegistryClient.))

(defn avro-encoder [registry config]
  (let [schema (let [find-schema (-> (get config "confluent.schema.resolver")
                                     (read-string)
                                     (eval))]
                 (find-schema *samza-stream-name*))

        topic (fn [schema]
                (str (hyphenate (.getName schema))
                     "-value"))]

    (-> (reify Encoder
          (toBytes [this object]
            (when object
              (let [version (.register registry (topic schema) schema)
                    out (ByteArrayOutputStream.)]
                (doto (DataOutputStream. out)
                  (.writeByte magic)
                  (.writeInt version)
                  (.flush))
                (println "encoding: " object)
                (a/encode schema out object)
                (.toByteArray out))))))))

(defn avro-decoder [registry config]
  (reify Decoder
    (fromBytes [this bs]
      (when-let [buffer (and bs (java.nio.ByteBuffer/wrap bs))]
        (if-not (= (.get buffer) magic)
          (throw (ex-info "Unknown magic byte" {:bytes bs}))
          (let [schema (.getByID registry (.getInt buffer))
                len (- (.limit buffer) 1 id-size)
                start (+ (.position buffer)
                         (.arrayOffset buffer))
                decoded (a/decode schema
                                  (a/binary-decoder [(.array buffer) start len]))]
            decoded))))))

(defn avro-producer [config system topic schema-locator]
  (let [stream (SystemStream. system topic)
        envelope (fn [stream k v]
                     (OutgoingMessageEnvelope. stream k v))]

    (fn [collector msg key-fn]
      (let [k (key-fn msg)
            v msg]
        (binding [*samza-stream-name* topic]
          (.send collector (envelope stream k v)))))))

(defrecord AvroSerdeFactory []
  SerdeFactory
  (getSerde [this serde-name config]
    (println config)
    (let [registry (let [build-registry (-> (get config "confluent.schema.registry.factory")
                                            (read-string)
                                            (eval))]
                     (build-registry config))]
      (reify Serde
        (fromBytes [this bytes]
          (.fromBytes (avro-decoder registry config) bytes))

        (toBytes [this msg]
          (.toBytes (avro-encoder registry config) msg))))))

(defrecord UUIDSerdeFactory []
  SerdeFactory
  (getSerde [this serde-name config]
    (reify Serde
      (fromBytes [this bytes]
        (when bytes
          (java.util.UUID/fromString (String. bytes "utf-8"))))

      (toBytes [this uuid]
        (when uuid
          (.getBytes (str uuid) "utf-8"))))))

(defrecord EDNSerdeFactory []
  SerdeFactory
  (getSerde [this serde-name config]
    (reify Serde
      (fromBytes [this bytes]
        (when bytes
          (edn/read-string (String. bytes))))
      (toBytes [this form]
        (.getBytes (pr-str form))))))
