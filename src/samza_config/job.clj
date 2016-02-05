(ns samza-config.job
  (:require
   [clojure.java.io :as io :refer [file]]
   [samza-config.utils :refer [class-name]])
  (:import
   [org.apache.samza.task StreamTask InitableTask]
   [org.apache.samza.job.local ThreadJobFactory ProcessJobFactory]
   [org.apache.samza.system.kafka KafkaSystemFactory]
   [org.apache.samza.storage.kv RocksDbKeyValueStorageEngineFactory]
   [samza_config.serde MapSerdeFactory UUIDSerdeFactory]))

;; TODO: Consider giving these helpers their own ns
(def thread-job-factory   {:class (.getName ThreadJobFactory)})
(def process-job-factory  {:class (.getName ProcessJobFactory)})
(def rocks-db-factory     {:class (.getName RocksDbKeyValueStorageEngineFactory)})
(def map-serde-factory    {:class (.getName MapSerdeFactory)})
(def uuid-serde-factory   {:class (.getName UUIDSerdeFactory)})
(def kafka-system-factory {:class (.getName KafkaSystemFactory)})

(def jobs (atom {}))

(defmacro defjob [job-name & body]
  `(let [job# ~@body]

     ;; update the `jobs` atom with the current job-spec
     (swap! jobs assoc ~job-name job#)

     ;; def the job
     (def ~job-name
       (:task ~@body))))

(defn job-name [job]
  (let [s (resolve job)]
    (str
     (.name (.ns s))
     "."
     (name job))))

(defn job-factory [job]
  (let [factories {:thread  (class-name ThreadJobFactory)
                   :process (class-name ProcessJobFactory)}]
    {:class
     (factories (:job-factory job))}))

(defn job-inputs [job]
  (:inputs job))

(defn job-config [job]
  (let [config {:job          {:factory  (job-factory job)}
                :task         {:class    (class-name (:task job))
                               :inputs   (job-inputs job)}
                :serializers  {:registry
                               {"uuid"   uuid-serde-factory
                                "map"    map-serde-factory}}
                :systems      {:kafka    {:samza {:factory kafka-system-factory}
                                          :key {:serde "uuid"}
                                          :msg {:serde "map"}}}}]
    config))


;; I wrote this before figuring out how to load a config entirely from job
;; specs defined in clojure but it might be useful sometime.
#_(defn write-job-config [job]
  (let [fmt-cfg (fn [[path value]]
                  (format "%s=%s" (str/join "." (map name path)) value))

        out (file "resources/" (str (job-name job) ".properties"))
        props (->> (flatten-map (job-config job)
                                (sort)
                                (map fmt-cfg)
                                (str/join "\n")))]
    (spit out props)
    (println "Wrote" (.getAbsolutePath out))))
