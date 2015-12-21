(ns example.null-job
  (:require
   [samza-config.core :refer [defsamza]]
   [samza-config.utils :refer [thread-job-factory
                               uuid-serde-factory
                               map-serde-factory
                               task-name
                               kafka-system]])
  (:import
   [org.apache.samza.job StreamJobFactory]
   [org.apache.samza.task StreamTask]))

(defrecord NullStreamTask []
  StreamTask
  (process [this envelope collector coordinator]))

(defsamza null-job
  {:job {:factory thread-job-factory}

   :task {:class (task-name NullStreamTask)
          :inputs {"example-system" "example-stream"}}

   :serializers {:registry
                 {:uuid uuid-serde-factory
                  :map map-serde-factory}}

   :systems {:kafka (kafka-system {:zk-host "zk.example.com"
                                   :zk-port "8081"
                                   :kafka-host "kafka.example.com"
                                   :kafka-port "9092"})}})
