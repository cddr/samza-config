(ns samza-config.core
  (:refer-clojure :exclude [compile])
  (:require
   [clojure.string :as str]
   [clojure.tools.nrepl.server :refer [start-server stop-server]]
   [samza-config.schema :as schema]
   [clojure.java.io :as io :refer [file]]
   [clojure.tools.cli :refer [parse-opts]])
  (:import
   [org.apache.samza.task StreamTask InitableTask]))

(def ^:dynamic *task-store-name*)
(def ^:dynamic *task-output*)

(defn stateful-task
  "Makes a stateful samza task out of the specified `step` function. Each
   time samza invokes the StreamTask's `process` method, we call the step
   function and pass in the task's local storage, and the task's output
   constructors"
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

;; (defn config-rewriter []
;;   (reify
;;     ConfigRewriter
;;     (rewrite [this rw-name config]
;;       (-> (.get config "samza.job")

;;       (let [job (.get config "samza.job")]
;;         (job-config job)


#_(def cli-options
  ;; An option with a required argument
  [["-s" "--source DIR" "Source directory"
    :default "resources/jobs/"
    :parse-fn #(file %)
    :validate [#(.isDirectory %) "Must be a directory containing samza job definitions"]]

   ["-o" "--output DIR" "Output directory"
    :default "resources/"
    :parse-fn #(file %)
    :validate [#(.isDirectory %) "Must be a directory into which samza jobs will be written"]]

   ["-h" "--help"]])

#_(defn -main [& args]
  (let [{:keys [options arguments summary errors]}
         (parse-opts args cli-options)]
    (case (first arguments)
      "compile" (compile (:source options)
                         (:output options)))))
