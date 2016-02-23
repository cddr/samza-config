(ns samza-config.core
  (:require
   [clojure.string :as str]
   [clojure.tools.nrepl.server :refer [start-server]]
   [samza-config.schema :as schema]
   [clojure.java.io :as io :refer [file resource]]
   [clojure.string :as str]
   [clojure.edn :as edn])
  (:import
   [org.apache.samza.config ConfigFactory MapConfig]
   [clojure.lang ILookup ITransientMap]
   [org.apache.samza.job JobRunner]))

(defn local-storage [context name]
  (let [read-val edn/read-string
        write-val pr-str
        kv-store (.getStore context name)]
    (reify
      ILookup
      (valAt [this k]
        (when-let [result (.get kv-store k)]
          (read-val result)))
      (valAt [this k default]
        (or (.valAt this k)
            default))

      ITransientMap
      (assoc [this k v]
        (.put kv-store k (write-val v))
        this) ;; transients always return themselves on mutation.
      (without [this k]
        (.delete kv-store k)
        this))))

(defn run-job [job-config]
  (let [runner (JobRunner. (MapConfig. job-config))]
    (.run runner true)))
