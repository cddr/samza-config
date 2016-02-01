(defproject samza-config "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories [["confluent" "http://packages.confluent.io/maven/"]]
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.slf4j/slf4j-log4j12 "1.6.2"]
                 [org.apache.samza/samza-log4j "0.10.0"]
                 [org.apache.samza/samza-api "0.10.0"]
                 [org.apache.samza/samza-core_2.10 "0.10.0"]
                 [org.apache.samza/samza-kafka_2.10 "0.10.0"]
                 [org.apache.samza/samza-kv_2.10 "0.10.0"]
                 [org.apache.samza/samza-kv-inmemory_2.10 "0.10.0"]
                 [org.apache.samza/samza-kv-rocksdb_2.10 "0.10.0"]
                 [io.confluent/kafka-avro-serializer "1.0"]
                 [prismatic/schema "1.0.4"]
                 [environ "1.0.2"]
                 [com.damballa/abracad "0.4.12"]
                 [org.clojure/tools.cli "0.3.3"]])
