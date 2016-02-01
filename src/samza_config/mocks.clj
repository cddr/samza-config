(ns samza-config.mocks
  "Mocks/Stubs for testing Samza Jobs"
  (:import
   [org.apache.samza.storage.kv KeyValueStore]))

(defn key-value-store [name]
  "Return an in-memory key-value-store for testing

   Whilst there is a built-in InMemory kv-store as part of samza, it
   does not let us stuff plain old clojure values in it which is
   unfortunate because provided the serdes are setup correctly, that
   is how it would be used in code.

   Anyway, we just back it with an atom here for testing without
   needing rocksdb. I commented out bits of the interface that I haven't
   found a use for yet. If you need it and it works for you, feel free
   to un-comment."
  (let [store (atom {})]
    (reify KeyValueStore
      (get [this k]
        (get @store k))

      (put [this k v]
        (swap! store assoc k v))

      #_(delete [this k]
          (swap! store dissoc k))

      #_(getAll [this ks]
          (map #(get @store %) ks))

      #_(putAll [this entries]
          (doseq [e entries]
            (swap! store assoc (.getKey e) (.getValue e))))

      #_(deleteAll [this ks]
          (doseq [k ks]
            (.delete this k)))

      #_(range [this from to]
          (filter (fn [[k v]]
                    (and (<= k to)
                         (>= k from)))
                  @store)))))
