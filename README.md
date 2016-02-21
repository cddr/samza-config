# samza-config

Samza jobs. In idiomatic Clojure

## Rationale

Samza takes care of a lot of quite difficult problems and while it
would be fun to solve them in Clojure (in-fact [some
people](https://github.com/onyx-platform/onyx) are doing that and
more), it's hard to justify the time it would take when you've got
business features to deliver. We want to allow app developers to focus
on the business logic of their app.

Ideally, you could write a plain old clojure function, and somehow
arrange for Samza to invoke it on each new message it is interested
in. The intention of this project is to provide the "somehow arrange
for" part.

## Implementation

We do this by implementing a few key features

 * A suite of functions that take a function as input, and use `reify` to
   build an implementation of the corresponding samza interface

 * A `defjob` macro that registers the job and allows for it's configuration
   to be generated

 * A config rewriter that pulls a job-id from the environment and generates
   the samza config on the fly

## Usage

I'm still playing around with the syntax of `defjob` but it should look something
like this. Here's the standard LinkedIn scale word-counter.

```
(ns example.word-counter
  (:require
   [samza-config.core :refer [local-storage job-coordinator task-factory]]
   [org.apache.samza.serializers StringSerdeFactory]))

(defn word-counter [config context]
  (let [store (local-storage context "word-counts")]
    (reify
      StreamTask
      (process [this envelope collector coordinator]
        (let [word (.getMessage envelope)]
          (swap! store update-in [word] (fnil inc 0)))))))

(defjob word-counter
  {:job {:factory thread-job-factory
         :coordinator (job-coordinator "word-counter" (env :coordinator-replication-factor))
         :task {:factory (task-factory 'word-counter)}}

   :systems {:word-counter
             {:samza {:factory kafka-system-factory}
              :streams (input-streams
                        (input-topic "words" :string :string))
              :consumer (env :zk-addr)
              :producer (env :kafka-addr)}}

   :serializers {:registry
                 {"string" {:class StringSerdeFactory}}}

   :stores (stores
            (kv-store "word-counts" :edn :edn))

   :task {:class "samza-config.task.Task"
          :inputs (task-inputs "words")}})
```

Samza jobs are typically deliverable as tarballs so we include a helper that
knows how to build one.

```
$ lein run -m samza-job.build tarball
```

Assuming the job above, this will package the job and write it to `target/word-counter.tar.gz`

## License

Copyright © 2015 Andy Chambers

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
