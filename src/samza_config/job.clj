(ns samza-config.job
  "This ns implements a simple DSL for defining and running samza jobs"
  (:require
   [environ.core :refer [env]]
   [clojure.java.io :as io :refer [file]]
   [clojure.string :as str])
  (:import
   [org.apache.samza.config MapConfig]
   [org.apache.samza.system.kafka KafkaSystemFactory]))

(def job-metadata (atom {}))

(defn find-job [job-name]
  (or (get @job-metadata (str job-name))
      (throw (ex-info (format "Cannot find job for %s" job-name)
                      {:job job-name
                       :job-db @job-metadata}))))

(defn flatten-map
  "Flattens a nested map"
  ([form]
     (into {} (flatten-map form nil)))
  ([form pre]
     (mapcat (fn [[k v]]
               (let [prefix (if pre
                              (conj pre k)
                              [k])]
                 (if (map? v)
                   (flatten-map v prefix)
                   [[prefix v]])))
             form)))

(defn input-streams [& streams]
  (apply hash-map (mapcat identity streams)))

(defn input-topic [topic key-serde msg-serde]
  [(keyword topic)
   {:key {:serde (name key-serde)}
    :msg {:serde (name msg-serde)}}])

(def ^:dynamic *system*)

(defn task-inputs [& inputs]
  (str/join "," (map #(str *system* "." %) inputs)))

(defn job-coordinator [system replication-factor]
  {:system "kafka"
   :replication {:factor "1"}})

(defn stores [& stores]
  (apply hash-map (mapcat identity stores)))

(defn kv-store [store key-serde msg-serde]
  [(keyword store)
   {:key {:serde (name key-serde)}
    :msg {:serde (name msg-serde)}
    :factory "org.apache.samza.storage.kv.RocksDbKeyValueStorageEngineFactory"
    :change-log (str store "-changelog")}])

(defn samza-config [job]
  (let [propertize-keys (fn [[path value]]
                          [(str/join "." (map name path)) value])]
    (->> (flatten-map job)
         (sort-by first)
         (mapcat propertize-keys)
         (apply hash-map)
         (MapConfig.))))

(defmacro defjob
  "Define a samza job. This is a convenience wrapper around samza-config. It just
   registers the job by name so that it can be found by `find-job`

   Supported parameters:-

     :task
     :job-factory
     :inputs
     :outputs
     :serializers
     "
  [job-name version & body]
  `(binding [*system* ~job-name]
     (let [m# (hash-map ~@body)
           job# (samza-config (merge-with merge {:job {:name ~(str *ns* "." job-name)}} m#))]
       (swap! job-metadata assoc (get job# "job.name") job#))))

;; (defn -main [& args]
;;   (JobRunner/main (into-array ["--config-factory=" (.getName JobConfigFactory)
;;                                "--config-path=" (first args)])))
