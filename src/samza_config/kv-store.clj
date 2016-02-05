(ns samza-config.kv-store
  "Extends the ILookup and ITransient interfaces to samza's key value store. Lets you
   just do normal clojure functions on the kv implementation provided by samza.

   e.g. assuming your storage is at a symbol named `store`, you can do....

   (get store)
   (assoc! store :foo \"foo\"
   (dissoc! store :foo)
   (conj! store [:foo :bar] inc)

   See http://spootnik.org/entries/2014/11/06_playing-with-clojure-core-interfaces.html
   for inspiration.
  "
  (:import [org.apache.samza.storage.kv KeyValueStore]
           [clojure.lang ILookup ITransientMap]))

(defn kv->transient [kv-store read-val write-val]
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
      this)))
