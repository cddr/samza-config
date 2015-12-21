(ns samza-config.core
  (:refer-clojure :exclude [compile])
  (:require
   [clojure.string :as str]
   [samza-config.schema :as schema]
   [clojure.java.io :as io :refer [file]]
   [clojure.tools.cli :refer [parse-opts]]))

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

(def samza-jobs (atom {}))

(defmacro defsamza [samza-name m]
  `(let [samza-name# ~(str (name samza-name))]
     (swap! samza-jobs
            assoc samza-name# ~m)))

(defn compile [src dest]
  (doseq [f (->> (file-seq src)
                 (filter #(.isFile %))
                 (map #(.getAbsolutePath %)))]
    (load-file f))

  (doseq [[samza-name samza-job] @samza-jobs]
    (let [out-file (file dest (str samza-name ".properties"))
          props (->> (flatten-map samza-job)
                     (map (fn [[path value]]
                            (format "%s=%s" (str/join "." (map name path)) value)))
                     (sort)
                     (str/join "\n"))]
      (spit out-file props)
      (println "Wrote" (.getAbsolutePath out-file)))))

(def cli-options
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

(defn -main [& args]
  (let [{:keys [options arguments summary errors]}
         (parse-opts args cli-options)]
    (case (first arguments)
      "compile" (compile (:source options)
                         (:output options)))))
