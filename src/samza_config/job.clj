(ns samza-config.job
  (:require [clojure.java.io :as io :refer [file]])
  (:import
   [org.apache.samza.task StreamTask InitableTask]))

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

(defn write-job-config [job]
  (let [fmt-cfg (fn [[path value]]
                  (format "%s=%s" (str/join "." (map name path)) value))

        out (file "resources/" (str (job-name job) ".properties"))
        props (->> (flatten-map (job-config job)
                                (sort)
                                (map fmt-cfg)
                                (str/join "\n")))]
    (spit out props)
    (println "Wrote" (.getAbsolutePath out))))
