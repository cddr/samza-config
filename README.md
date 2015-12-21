# samza-config

Programmable samza configuration

## Rationale

The samza configuration language is very powerful. But often, computations
are built of several jobs. The nature of samza is that these jobs are
completely independent of one another. But it seems to me that having a way
to dynamcally generate samza configurations seems to be a pre-requisite for
building higher level operators that can "compose" (everybody drink) samza jobs.

While we wait for these higher level  operators, we can have a nice way of
validating and exporting configuration from our code, together with a reusable
library containing other generally useful stuff like custom serdes and a kafka
system builder.

## Example Usage

Given the following file in src/examples/null-job.clj

```
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

(defsamza mock-job
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
```

At the command line, run

```
$ lein run -m schema-config.core compile -s src/examples -o /tmp
```

which result in the following samza configuration being emitted to /tmp/null-job.properties

```
job.factory.class=org.apache.samza.job.local.ThreadJobFactory
serializers.registry.map.class=samza_config.serde.MapSerdeFactory
serializers.registry.uuid.class=samza_config.serde.UUIDSerdeFactory
systems.kafka.consumer.zookeeper.connect=zk.example.com:8081
systems.kafka.key.serde=uuid
systems.kafka.msg.serde=map
systems.kafka.producer.bootstrap.servers=kafka.example.com:9092
systems.kafka.samza.factory.class=org.apache.samza.system.kafka.KafkaSystemFactory
task.class=example.null_job.NullStreamTask
task.inputs.example-system=example-stream
```

## Usage



FIXME

## License

Copyright © 2015 FIXME

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
