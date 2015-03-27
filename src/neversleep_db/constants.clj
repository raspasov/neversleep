(ns neversleep-db.constants)

;determines how many io-assoc operations to collect before sending blob to durable storage
(def ^:const blob-size 500)

;pipeline config
(def ^:const max-io-retries 3)

;blob part size
(def ^:const blob-part-size 63996)

(def ^:const read-pipeline-size 10)
