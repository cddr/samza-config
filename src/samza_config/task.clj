(ns samza-config.task
  (:require [clojure.string :as str])
  (:import [org.apache.samza.task WindowableTask ClosableTask StreamTask])
  (:gen-class
   :name samza-config.task.Task
   :main false
   :init clojure-init
   :state state
   :implements [org.apache.samza.task.InitableTask
                org.apache.samza.task.WindowableTask
                org.apache.samza.task.ClosableTask
                org.apache.samza.task.StreamTask]))

(defn handle-exception [e state]
  (println e)
  (println state))

(defn -clojure-init []
  [[] (atom {})])

(defn -init [this config context]
  (println "Initializing task: " this config context)
  (let [task-factory (-> (.get config "job.task.factory")
                         read-string
                         eval)]

    (reset! (.state this) {:task (task-factory config context)
                           :config config
                           :context context})))

(defn- get-task [this]
  (:task @(.state this)))

(defn -process [this envelope collector coordinator]
  (try
    (let [task (get-task this)]
      (when (instance? StreamTask task)
        (.process task envelope collector coordinator)))
    (catch Exception e
      (handle-exception e (.state this)))))

(defn -window [this collector coordinator]
  (try
    (let [task (get-task this)]
      (when (instance? WindowableTask task)
        (.window task collector coordinator)))
    (catch Exception e
      (handle-exception e (.state this)))))

(defn -close [this]
  (try
    (let [task (get-task this)]
      (when (instance? ClosableTask task)
        (.close task)))
    (catch Exception e
      (handle-exception e (.state this)))))
