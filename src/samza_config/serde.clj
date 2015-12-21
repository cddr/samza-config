(ns samza-config.serde
  (:require
   [abracad.avro :as a]
   [clojure.string :as str])
  (:import [io.confluent.kafka.schemaregistry.client CachedSchemaRegistryClient]
           [org.apache.samza.serializers Serde SerdeFactory]
           [java.io ByteArrayOutputStream DataOutputStream]))

(def magic 0x0)
(def id-size 4)
(def utf-8 "UTF-8")

(defn get-option [config path]
  (let [path (str/join "." (map name path))]
    (.get config path)))

(defn schema-registry-url [config]
  (get-option [:confluent :schema :registry :url]))

(defn registry-client [config]
  (-> (schema-registry-url config)
      (CachedSchemaRegistryClient. 100)))

(defn deserialize [subject-meta bytes]
  (let [buffer (java.nio.ByteBuffer/wrap bytes)]
    (if-not (= (.get buffer) magic)
      (throw (Exception. "Unknown magic byte"))
      (let [schema (.getSchema subject-meta)
            len (- (.limit buffer) 1 id-size)
            start (+ (.position buffer)
                     (.arrayOffset buffer))]
        (a/decode schema
                  (a/binary-decoder [(.array buffer) start len]))))))

(defn serialize [subject-meta m]
  (let [out (ByteArrayOutputStream.)]
    (doto (DataOutputStream. out)
      (.writeByte (byte magic))
      (.writeInt (.getId subject-meta))
      (.flush))
    (apply a/encode (.getSchema subject-meta) out [m])
    (.toByteArray out)))

(defn find-serde [serde-name config]
  (let [subject (get-option [:serializers :registry serde-name :subject])
        registry-client (registry-client config)
        subject-meta (.getLatestSchemaMetadata registry-client subject)]

    (reify Serde
      (fromBytes [this bytes]
        (deserialize subject-meta bytes))

      (toBytes [this m]
        (serialize subject-meta m)))))


(defrecord MapSerdeFactory []
  SerdeFactory
  (getSerde [this serde-name config]
    (find-serde serde-name config)))

(defrecord UUIDSerdeFactory []
  SerdeFactory
  (getSerde [this serde-name config]
    (reify Serde
      (fromBytes [this bytes]
        (when bytes
          (java.util.UUID/fromString (String. bytes utf-8))))

      (toBytes [this uuid]
        (when uuid
          (.getBytes (str uuid) utf-8))))))
