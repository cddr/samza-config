
 * [ ] manifold system factory
 * [ ] canned jobs for
        - emitting metrics to riemann/graphite
        - feeding topic into elasticache/dynamodb


(deftest test-samza-job
  (let [input (s/stream)
        output (s/stream)
        job (-> (merge-jobs 'word-counter 'test-job))]

    (d/future (run-job job))

    (input "words" "Cats are mean")
    (input "words" "Dogs are nice")

    (let [word-counts (take 6 (s/stream->seq output 1000))]
      (is (= [["Cats" 1]
              ["are" 1]
              ["mean" 1]
              ["Dogs" 1]
              ["are" 2]
              ["nice" 1]]
             word-counts)))))



riemann/samza metrics integration (or should we just go straight to graphite?)
