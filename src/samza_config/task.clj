(ns samza-config.task
  (:require [clojure.string :as str])
  (:gen-class
   :name samza-config.task.Task
   :main false
   :init clojure-init
   :state task
   :implements [org.apache.samza.task.InitableTask
                org.apache.samza.task.WindowableTask
                org.apache.samza.task.ClosableTask
                org.apache.samza.task.StreamTask])
  (:import [org.apache.samza.system OutgoingMessageEnvelope SystemStream]))

(defprotocol Streamable
  (process [this envelope producer]))

(defprotocol Initable
  (init-task [this config context]))

(defprotocol Closable
  (close [this]))

(defprotocol Windowable
  (window [this collector coordinator]))

(defn -clojure-init []
  [[] (atom {})])

(defn -init [this config context]
  (let [parse-factory (fn []
                        (let [fq-sym-str (.get config "job.task.factory")
                              fq-sym-var (find-var (symbol fq-sym-str))
                              ns-sym (-> fq-sym-str
                                         (str/split #"/")
                                         first
                                         symbol)]
                          {:ns-sym ns-sym
                           :factory-fn fq-sym-var}))

        {:keys [ns-sym factory-fn]} (parse-factory)]

    (println "Loading task: " factory-fn " in namespace: " ns-sym)
    (require ns-sym)
    (reset! (.task this) (factory-fn config context))))

(defn- get-task [this]
  (:task @(.task this)))

(defn -process [this envelope collector coordinator]
  (let [task (get-task this)]
    (when (satisfies? Streamable task)
      (.process task
                envelope
                collector
                coordinator))))

(defn -window [this collector coordinator]
  (let [task (get-task this)]
    (when (satisfies? Windowable task)
      (.window task collector coordinator))))

(defn -close [this]
  (let [task (get-task this)]
    (when (satisfies? Closable task)
      (close task))))
