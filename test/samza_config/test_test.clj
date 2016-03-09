(ns samza-config.test-test
  (:require [samza-config.test :as sut]
            [clojure.test :as t]))

(t/deftest mock-kv-store-all-test
  (let [kv-store (sut/mock-kv-store)
        iterator (.all kv-store)]
    (t/is (= false (.hasNext iterator)))

    (.put kv-store 1 "one")
    (.put kv-store 2 "two")

    (let [iterator2 (.all kv-store)]
      (t/is (= true (.hasNext iterator2)))
      (.next iterator2)
      (t/is (= true (.hasNext iterator2)))
      (.next iterator2)
      (t/is (= false (.hasNext iterator2))))

    (let [iterator3 (.all kv-store)
          seq3 (iterator-seq iterator3)]
      (t/is (= 2 (count seq3))))

    (.delete kv-store 1)

    (let [iterator4 (.all kv-store)
          seq4 (iterator-seq iterator4)]
      (t/is (= 1 (count seq4))))))
