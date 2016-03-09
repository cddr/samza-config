(ns samza-config.schema-test
  (:require
   [clojure.test :refer :all]
   [samza-config.schema :refer [valid? explain-path errors]]
   [samza-config.serde]
   [clojure.walk :refer [postwalk]])

  (:import
   [org.apache.samza.job StreamJobFactory]
   [org.apache.samza.task StreamTask]
   [samza_config.serde UUIDSerdeFactory AvroSerdeFactory]))

(defn has-error? [job path]
  (let [err (errors job)]
    (explain-path err path)))

(defn not-error? [job path]
  (not (has-error? job path)))

(defrecord MockStreamTask []
  StreamTask
  (process [this envelope collector coordinator]))

(def string-serde-factory
  {:class "org.apache.samza.serializers.StringSerdeFactory"})

(deftest factory-resolvers-test
  (testing "SerdeFactory"
    (let [job (fn [serde-class]
                {:serializers
                 {:registry
                  {:yolo serde-class}}})

          valid-serde (job string-serde-factory)
          invalid-serde (job "java.lang.Object")]

      (is (has-error? invalid-serde [:serializers :registry :yolo]))
      (is (not-error? valid-serde [:serializers :registry :yolo])))))
