(ns samza-config.schema
  "Defines the schema implicit in a samza configuration"
  (:require
   [clojure.string :as str]
   [schema.core :as s :refer [Str Int Bool Keyword]]
   [schema.utils :refer [validation-error-explain]])
  (:import
   [org.apache.samza.checkpoint CheckpointManagerFactory]
   [org.apache.samza.serializers SerdeFactory]
   [org.apache.samza.system SystemFactory]
   [org.apache.samza.task StreamTask]
   [org.apache.samza.system.chooser MessageChooserFactory]
   [org.apache.samza.job CommandBuilder StreamJobFactory]
   [org.apache.samza.config Config ConfigRewriter]))

(def SystemName Str)
(def StreamName Str)

(defn check-isa? [parent]
  (let [_isa? (fn [class-name]
                (let [cls (Class/forName class-name)]
                  (isa? cls parent)))]
    (s/pred _isa? parent)))

(def TaskConfig
  {:class                (check-isa? StreamTask)
   :inputs               {SystemName StreamName}

   (s/optional-key
    :window.ms)          Int

   (s/optional-key
    :commit.ms)          Int

   (s/optional-key
    :command)            {:class (check-isa? CommandBuilder)}

   (s/optional-key
    :task)               {:opts [Str]}

   (s/optional-key
    :java)               {:home Str}

   (s/optional-key
    :execute)            Str

   (s/optional-key
    :chooser)            {:class (check-isa? MessageChooserFactory)}

   (s/optional-key
    :checkpoint)         {:factory (check-isa? CheckpointManagerFactory)}

   (s/optional-key
    :drop)               {:serialization
                          {:errors Bool}

                          :deserialization
                          {:errors Bool}}})

(def StreamConfig
  {(s/optional-key
    :key)            {:serde Keyword}

   (s/optional-key
    :msg)            {:serde Keyword}

   (s/optional-key
    :offset)         {:default (s/enum :upcoming :oldest)}

   (s/optional-key
    :reset)          {:offset Bool}

   (s/optional-key
    :priority)       Int

   (s/optional-key
    :bootstrap)      Bool})

(def SerializerRegistry
  {:registry {Keyword {:class (check-isa? SerdeFactory)}}})

(def SystemConfig
  (merge {:samza {:factory {:class (check-isa? SystemFactory)}}
          :consumer {:zookeeper
                     {:connect String}}
          :producer {:bootstrap
                     {:servers String}}}

         StreamConfig

         {(s/optional-key
           :streams)        {Keyword StreamConfig}}))

(def JobConfig
  (merge
   {:factory {:class (check-isa? StreamJobFactory)}}

   {:name          Str

    (s/optional-key
     :id)          Int

    (s/optional-key
     :rewriter)   {Keyword (check-isa? ConfigRewriter)}}))

(def SamzaJob
  {:job            JobConfig
   :task           TaskConfig
   :systems        {Keyword SystemConfig}
   (s/optional-key
    :serializers)  SerializerRegistry})

(defn valid? [samza-job]
  (s/validate SamzaJob samza-job))

(defn errors [samza-job]
  (try
    (valid? samza-job)
    (catch clojure.lang.ExceptionInfo ex
      (:error (ex-data ex)))))

(defn explain-path [errors path]
  (when-let [err (get-in errors path)]
    (validation-error-explain err)))
