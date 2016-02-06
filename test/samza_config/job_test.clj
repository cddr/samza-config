(ns samza-config.job-test
  (:require
   [clojure.test :refer :all]
   [samza-config.job :refer :all]
   [samza-config.utils :refer [uuid-serde-factory map-serde-factory
                               input-topic]])
  (:import
   [org.apache.samza.task StreamTask]
   [org.apache.samza.config Config]))

(def test-task
  (reify
    StreamTask
    (process [this env collector coordinator])))

(defjob test-job
  {:job-factory :thread
   :inputs [(input-topic "Foo")
            (input-topic "Bar")]
   :task test-task
   :serializers {"uuid"   uuid-serde-factory
                 "map"    map-serde-factory}})

(deftest test-job-metadata
  (let [job-meta (job-metadata 'test-job)]
    (is (= {:class "org.apache.samza.job.local.ThreadJobFactory"}
           (job-factory 'test-job)))
    (is (= (.getName (class test-task))
           (job-task 'test-job)))
    (is (= {:registry
            {"uuid" uuid-serde-factory
             "map" map-serde-factory}}
           (job-serializers 'test-job)))))

(deftest test-job-config
  (let [test-config (job-config 'test-job)]
    (is (instance? Config test-config))

    (are [k v] (= v (get test-config k))
      "job.name"          "samza-config.job-test.test-job"
      "job.factory.class" "org.apache.samza.job.local.ThreadJobFactory"

      "task.class"  (.getName (class test-task))
      "task.inputs" "kafka.Foo, kafka.Bar"

      "systems.kafka.samza.factory.class" "org.apache.samza.system.kafka.KafkaSystemFactory"
      "systems.kafka.msg.serde"           "map"
      "systems.kafka.key.serde"           "uuid"

      "serializers.registry.uuid.class" "samza_config.serde.UUIDSerdeFactory"
      "serializers.registry.map.class"  "samza_config.serde.MapSerdeFactory")))
