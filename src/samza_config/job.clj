(ns samza-config.job
  "This ns implements a simple DSL for defining samza jobs."
  (:require
   [clojure.java.io :as io :refer [file]]
   [clojure.string :as str]
   [samza-config.utils :refer [class-name map-serde-factory uuid-serde-factory kafka-system-factory]])
  (:import
   [org.apache.samza.task StreamTask InitableTask]
   [org.apache.samza.config MapConfig]
   [org.apache.samza.job.local ThreadJobFactory ProcessJobFactory]
   [org.apache.samza.system.kafka KafkaSystemFactory]
   [org.apache.samza.storage.kv RocksDbKeyValueStorageEngineFactory]))

(def metadata (atom {}))

(defmacro defjob
  "Define a samza job.

   Supported parameters:-

     :task
     :job-factory
     :inputs
     :outputs
     :serializers
     "
  [job-name & body]
  `(let [job# ~@body]
     ;; def the job
     (def ~job-name
       (:task ~@body))

     ;; update the `jobs` atom with the job-spec defined by `body`
     (swap! metadata assoc '~job-name job#)))

(defn job-name [job]
  (let [s (resolve job)]
    (str
     (.name (.ns s))
     "."
     (name job))))

(defn job-metadata [job]
  (get @metadata job))

(defn job-factory [job]
;  {:pre [(:job-factory job)]}
  (let [factories {:thread  (class-name ThreadJobFactory)
                   :process (class-name ProcessJobFactory)}]
    {:class
     (factories (:job-factory (@metadata job)))}))

(defn job-inputs [job]
  (str/join ", " (:inputs (@metadata job))))

(defn job-task [job]
  (let [job-meta (@metadata job)]
    (class-name (class (:task job-meta)))))

(defn job-serializers [job]
  {:registry
   (:serializers (@metadata job))})

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

(defn job-config [job]
  (let [job-meta (@metadata job)
        config {:job          {:name     (job-name job)
                               :factory  (job-factory job)}

                :task         {:class    (job-task job)
                               :inputs   (job-inputs job)}
                :serializers  (job-serializers job)
                :systems      {:kafka    {:samza {:factory kafka-system-factory}
                                          :key {:serde "uuid"}
                                          :msg {:serde "map"}}}}

        propertize-keys (fn [[path value]]
                          [(str/join "." (map name path)) value])]

    (->> (flatten-map config)
         (sort-by first)
         (mapcat propertize-keys)
         (apply hash-map)
         (MapConfig.))))


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
