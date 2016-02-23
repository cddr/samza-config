(ns samza-config.task
  (:require [clojure.string :as str])
  (:gen-class
   :name samza-config.task.Task
   :main false
   :init clojure-init
   :state state
   :implements [org.apache.samza.task.InitableTask
                org.apache.samza.task.WindowableTask
                org.apache.samza.task.ClosableTask
                org.apache.samza.task.StreamTask])
  (:import [org.apache.samza.system OutgoingMessageEnvelope SystemStream]))

(defn -clojure-init []
  [[] (atom {})])

(defn -init [this config context]
  (let [task-factory (-> (.get config "job.task.factory")
                         read-string
                         eval)]

    (reset! (.state this) {:task (task-factory config context)
                           :config config
                           :context context})))

(defn- get-task [this]
  (:task @(.state this)))

(defn -process [this envelope collector coordinator]
  (let [task (get-task this)]
    (.process task
              envelope
              collector
              coordinator)))

(defn -window [this collector coordinator]
  (let [task (get-task this)]
    (.window task collector coordinator)))

(defn -close [this]
  (let [task (get-task this)]
    (.close task)))
