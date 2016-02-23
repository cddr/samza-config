(ns samza-config.confluent
  (:import
   [io.confluent.kafka.serializers KafkaAvroDecoder KafkaAvroEncoder]
   [kafka.utils VerifiableProperties]
   [org.apache.samza.serializers Serde SerdeFactory]
   [java.util Properties]))

;; Use this if you don't mind dealing with Generic avro objects. If
;; you're writing your job in Clojure, you might prefer
;; `samza_config.serde.AvroSerdeFactory`

(defrecord AvroSerdeFactory []
  SerdeFactory
  (getSerde [this s config]
    (let [registry-url (get config "confluent.schema.registry.url")
          props (doto (Properties.)
                  (.setProperty "schema.registry.url" registry-url))

          encoder (KafkaAvroEncoder. (VerifiableProperties. props))
          decoder (KafkaAvroDecoder. (VerifiableProperties. props))]

      (reify Serde
        (fromBytes [this bytes]
          (.fromBytes decoder bytes))
        (toBytes [this msg]
          (.toBytes encoder msg))))))
