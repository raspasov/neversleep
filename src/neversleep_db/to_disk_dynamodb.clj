(ns neversleep-db.to-disk-dynamodb
  (:require [taoensso.faraday :as far]
            [neversleep-db.serializer :as serializer]
            [clojure.core.async :refer [chan timeout go <! <!! >!! >! go-loop put! thread alts!! alts! dropping-buffer pipeline-blocking]]
            [neversleep-db.constants :as constants]
            [neversleep-db.util :as util]
            [neversleep-db.to-disk-utils :as to-disk-utils]))



;(def client-opts
;  {:access-key "AKIAIYWYXT67YSBLYW7A"  ; For DynamoDB Local, just put some random string
;   :secret-key "BC6O8YFed8VQPSMnSPLRjm/AsO+YBpq7Rmh86ta1" ; For production, put your IAM keys here
;   })
;
;(far/list-tables client-opts)
;
;
;;STORAGE ENGINE-SPECIFIC FNs
;(defn get-blob-key [blob-id part entity-id]
;  (str blob-id "_" part "_" entity-id))
;
;(defn get-root-log-key [blob-id part]
;  (str blob-id "_" part))
;
;;blobs
;(defn- save-blob-chunk [blob-id part entity-id data]
;  (far/put-item client-opts :neversleepdb_blobs {:blob_part_entity (get-blob-key blob-id part entity-id)
;                                                 :data data}))
;
;(defn- get-blob-chunk [blob-id part entity-id]
;  (-> (far/get-item client-opts :neversleepdb_blobs {:blob_part_entity (get-blob-key blob-id part entity-id)})
;      (get :data)))
;
;;root log
;(defn save-root-log-chunk [blob-id part data]
;  (far/put-item client-opts :neversleepdb_root_log {:blob_part (get-root-log-key blob-id part)
;                                                    :data data}))
;
;(defn get-root-log-chunk [blob-id part entity-id]
;  (-> (far/get-item client-opts :neversleepdb_root_log {:blob_part (get-root-log-key blob-id part)})
;      (get :data)))
;
;(defn delete-root-log-chunk [blob-id part entity-id]
;  (far/delete-item client-opts :neversleepdb_root_log {:blob_part (get-root-log-key blob-id part)}))
;
;
;(defn- extract-blob-id [a-string]
;  (-> a-string
;      (.split "_")
;      (aget 0)
;      (util/parse-long)))
;
;(defn get-all-root-logs []
;  (sequence (map (fn [x] {:blob-id (extract-blob-id (:blob_part x))}))
;            (far/scan client-opts :neversleepdb_root_log)))
;
;;BLOBS
;(defn save-blob! [byte-array-data blob-id entity-id confirm-ch]
;  (to-disk-utils/save-blob! save-blob-chunk byte-array-data blob-id entity-id confirm-ch))
;
;(defn get-blob! [blob-id entity-id confirm-ch]
;  (to-disk-utils/get-blob! get-blob-chunk blob-id entity-id confirm-ch))
;
;;ROOT LOGS
;(defn save-root-log-remote! [byte-array-data blob-id confirm-ch]
;  (to-disk-utils/save-root-log-remote! save-root-log-chunk byte-array-data blob-id confirm-ch))
;
;(defn get-root-log-remote! [blob-id confirm-ch]
;  (to-disk-utils/get-root-log-remote! get-root-log-chunk blob-id confirm-ch))
;
;;TODO remove
;(defn get-all-root-logs-remote! [confirm-ch]
;  (to-disk-utils/get-all-root-logs-remote! get-all-root-logs confirm-ch))
;
;;TODO
;(defn get-root-logs-remote-lte!
;  "Get all remote root logs less than or equal to blob id"
;  [blob-id confirm-ch]
;  )
;
;;TODO
;(defn delete-root-logs-remote-gt!
;  "Called at server start to clean up logs, deletes all root logs greater than blob-id"
;  [blob-id confirm-ch])
;
;(defn test-write []
;  (doseq [i (range 5)]
;    (println i)
;    (save-root-log-remote! (util/byte-array-n-kb 100) i c-1)
;    (println "DONE WITH " i)))
;
;(defn test-read []
;  (doseq [i (range 100)]
;    (println i)
;
;    (future
;      (let [c (chan 1)]
;        (get-root-log-remote! i c)
;        (println (alength (<!! c)))
;        (println "DONE WITH " i)))))


;
;(far/create-table client-opts :neversleepdb_root_log
;                  [:id :s]  ; Primary key named "id", (:n => number type)
;                  {:throughput {:read 1 :write 1} ; Read & write capacity (units/sec)
;                   :block? true ; Block thread during table creation
;                   })