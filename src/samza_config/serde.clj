(ns samza-config.serde
  (:require
   [abracad.avro :as a]
   [clojure.string :as str]
   [clojure.edn :as edn]
   [inflections.core :refer [hyphenate]])
  (:import
   [io.confluent.kafka.schemaregistry.client CachedSchemaRegistryClient]
   [org.apache.samza.serializers Serde SerdeFactory]
   [org.apache.samza.system SystemStream OutgoingMessageEnvelope]
   [java.io ByteArrayOutputStream DataOutputStream]
   [kafka.serializer Encoder Decoder]))

(def magic                 (byte 0x0))
(def id-size               4)
(def registry-cache-size   100)
(def schema-registry-url   (System/getenv "SCHEMA_REGISTRY_URL"))

(defn topic [schema]
  (str (hyphenate (.getName schema))
       "-value"))

(defn registry []
  (CachedSchemaRegistryClient. (str schema-registry-url) 100))

(defn avro-encoder [schema]
  (reify Encoder
    (toBytes [this object]
      (when object
        (let [version (.register (registry) (topic schema))
              out (ByteArrayOutputStream.)]
          (doto (DataOutputStream. out)
            (.writeByte magic)
            (.writeInt version)
            (.flush))
          (a/encode schema out object))))))

(def ^:dynamic *avro-encoder*)

(defn envelope [stream k v]
  (OutgoingMessageEnvelope. stream k v))

(defn avro-producer [system topic schema-locator]
  (let [stream (SystemStream. system topic)]
    (fn [collector msg key-fn]
      (let [k (key-fn msg)
            v msg]
        (binding [*avro-encoder* (avro-encoder (schema-locator topic))]
          (.send collector (envelope stream k v)))))))

;; The reason for the asymmetry here is because on the decode side, the schema we need
;; to decode the message is embedded in the message itself whereas on the encoding side
;; we must provide the schema we intend to write with.
(defn avro-decoder []
  (reify Decoder
    (fromBytes [this bs]
      (when-let [buffer (and bs (java.nio.ByteBuffer/wrap bs))]
        (if-not (= (.get buffer) magic)
          (throw (ex-info "Unknown magic byte" {:bytes bs}))
          (let [schema (.getByID (registry) (.getInt buffer))
                len (- (.limit buffer) 1 id-size)
                start (+ (.position buffer)
                         (.arrayOffset buffer))
                decoded (a/decode schema
                                  (a/binary-decoder [(.array buffer) start len]))]
            (println "decoded value: " decoded)
            decoded))))))

(defrecord AvroSerdeFactory []
  SerdeFactory
  (getSerde [this serde-name config]
    (reify Serde
      (fromBytes [this bytes]
        (.fromBytes (avro-decoder) bytes))

      (toBytes [this msg]
        (.toBytes *avro-encoder* msg)))))

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
        (edn/read-string (String. bytes)))
      (toBytes [this form]
        (.getBytes (pr-str form))))))
