(ns samza-config.core
  (:refer-clojure :exclude [compile])
  (:require
   [clojure.string :as str]
   [clojure.tools.nrepl.server :refer [start-server]]
   [samza-config.schema :as schema]
   [clojure.java.io :as io :refer [file]])
  (:import
   [org.apache.samza.task StreamTask InitableTask]))

(defonce server (start-server :port 7888))

(def ^:dynamic *task-store-name*)
(def ^:dynamic *task-output*)

(defn stateful-task
  "Makes a stateful samza task out of the specified `step` function. Each
   time samza invokes the StreamTask's `process` method, we call the step
   function and pass in the task's local storage, the current input message
   and the task's output constructors.

   Messages include their :message-type which can be useful for dispatching
   messages to their handlers using multi-methods."
  [step]
  (let [state        (atom {})
        store-name   *task-store-name*
        output       *task-output*]
    (reify
      InitableTask
      (init [this config context]
        (swap! state assoc
               :config config
               :context context))

      StreamTask
      (process [this envelope collector coordinator]
        (let [store (.getStore (:context state) store-name)
              msg (bean envelope)]
          (step store msg output))))))
