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
   [org.apache.samza.job JobRunner]
   [org.apache.samza.task TaskCoordinator$RequestScope]))

(def ^:dynamic *samza-system-name*)
(def ^:dynamic *samza-stream-name*)

(def commit-scopes
  {:task TaskCoordinator$RequestScope/CURRENT_TASK
   :all TaskCoordinator$RequestScope/ALL_TASKS_IN_CONTAINER})

(defn commit [coordinator scope]
  (.commit coordinator (commit-scopes scope)))

(defn local-storage [context name]
  (.getStore context name))

(defn run-job [job-config]
  (let [runner (JobRunner. (MapConfig. job-config))]
    (.run runner true)))

(defn pprint-config [job-config]
  (doseq [[k v] (into (sorted-map) job-config)]
    (println (format "%s=%s" k v))))
