(ns neversleep-db.to-disk-mysql
  (:require [neversleep-db.mysql-lib :as mysql-lib]
            [neversleep-db.println-m :refer [println-m]]
            [neversleep-db.to-disk-utils :as to-disk-utils]
            [criterium.core :as criterium]
            [clojure.java.io :refer [file output-stream input-stream]]
            [clojure.core.async :refer [chan timeout go <! <!! >!! >! go-loop put! thread alts!! alts! dropping-buffer pipeline-blocking]])
  (:import com.durablenode.Durablenode$BtreeRootLogEntry))


;STORAGE ENGINE-SPECIFIC FNs
(defn save-blob-chunk [blob-id part entity-id data]
  (mysql-lib/execute! ["INSERT INTO `neversleepdb_blobs` (`id`, `blob_id`, `part`, `entity_id`, `data`) VALUES (NULL, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE `data` = ?;"
                       blob-id part entity-id data data]))

(defn save-root-log-chunk [blob-id part data]
  (mysql-lib/execute! ["INSERT INTO `neversleepdb_root_log` (`id`, `blob_id`, `part`, `data`) VALUES (NULL, ?, ?, ?) ON DUPLICATE KEY UPDATE `data` = ?;"
                       blob-id part data data]))

(defn save-root-log-master-chunk [blob-id part data]
  (mysql-lib/execute! ["INSERT INTO `neversleepdb_root_log_master` (`id`, `blob_id`, `part`, `data`) VALUES (NULL, ?, ?, ?) ON DUPLICATE KEY UPDATE `data` = ?;"
                       blob-id part data data]))

(defn- get-blob-chunk [blob-id part-idx entity-id]
  (-> (mysql-lib/query ["SELECT * FROM `neversleepdb_blobs` WHERE `blob_id` = ? and `part` = ? and `entity_id` = ?;" blob-id part-idx entity-id])
      (first)
      (:data nil)))

(defn- get-root-log-chunk [blob-id part-idx entity-id]
  (-> (mysql-lib/query ["SELECT `data` FROM `neversleepdb_root_log` WHERE `blob_id` = ? and `part` = ? LIMIT 1;" blob-id part-idx])
      (first)
      (:data nil)))

(defn- get-root-log-master-chunk [blob-id part-idx entity-id]
  (-> (mysql-lib/query ["SELECT `data` FROM `neversleepdb_root_log_master` WHERE `blob_id` = ? and `part` = ? LIMIT 1;" blob-id part-idx])
      (first)
      (:data nil)))
;==================================
;END

;BLOBS
;commit operations
(defn set-last-saved-blob-id!
  "Commits blob-id"
  [blob-id confirm-ch]
  (try (do (mysql-lib/execute! ["UPDATE `neversleepdb_vars` SET `value` = ? WHERE `id` = '1';" blob-id])
           (let [result (mysql-lib/query ["SELECT * FROM `neversleepdb_vars` WHERE `id` = '1';"])]
             (>!! confirm-ch (= (some-> result (first) :value) blob-id))))
       ;not ok, catch exception, return e on the channel
       (catch Exception e (>!! confirm-ch e))))

(defn get-last-saved-blob-id! [confirm-ch]
  (try (let [result (-> (mysql-lib/query ["SELECT * FROM `neversleepdb_vars` WHERE `id` = 1 LIMIT 1"])
                        (first)
                        (:value nil))]
         (>!! confirm-ch result))
       (catch Exception e (>!! confirm-ch e))))

(defn save-blob! [byte-array-data blob-id entity-id confirm-ch]
  (to-disk-utils/save-blob! save-blob-chunk byte-array-data blob-id entity-id confirm-ch))

(defn get-blob! [blob-id entity-id confirm-ch]
  (to-disk-utils/get-blob! get-blob-chunk blob-id entity-id confirm-ch))

;ROOT LOGS
;commit operations
(defn set-last-saved-root-log! [blob-id confirm-ch]
  (try (do (mysql-lib/execute! ["UPDATE `neversleepdb_vars` SET `value` = ? WHERE `id` = '2';" blob-id])
           (let [result (mysql-lib/query ["SELECT * FROM `neversleepdb_vars` WHERE `id` = '2';"])]
             (>!! confirm-ch (= (some-> result (first) :value) blob-id))))
       ;not ok, catch exception, return e on the channel
       (catch Exception e (>!! confirm-ch e))))

(defn get-last-saved-root-log! [confirm-ch]
  (try (let [result (-> (mysql-lib/query ["SELECT * FROM `neversleepdb_vars` WHERE `id` = 2 LIMIT 1"])
                        (first)
                        (:value nil))]
         (>!! confirm-ch result))
       (catch Exception e (>!! confirm-ch e))))
;================================

;regular
(defn save-root-log-remote! [byte-array-data blob-id confirm-ch]
  (to-disk-utils/save-root-log-remote! save-root-log-chunk byte-array-data blob-id confirm-ch))

(defn get-root-log-remote! [blob-id confirm-ch]
  (to-disk-utils/get-root-log-remote! get-root-log-chunk blob-id confirm-ch))

(defn get-root-logs-remote-between!
  "Get all remote root logs less than or equal to blob id"
  [blob-id-start blob-id-end confirm-ch]
  (to-disk-utils/get-root-logs-remote-between! get-root-log-chunk blob-id-start blob-id-end confirm-ch))

(defn delete-root-log-remote! [blob-id confirm-ch]
  (try
    (do (mysql-lib/execute! ["DELETE FROM `neversleepdb_root_log` WHERE `blob_id` = ?;" blob-id])
        (>!! confirm-ch true))
    (catch Exception e (>!! confirm-ch e))))

;================================

;master
(defn save-root-log-master-remote! [byte-array-data blob-id confirm-ch]
  (to-disk-utils/save-root-log-remote! save-root-log-master-chunk byte-array-data blob-id confirm-ch))

(defn get-root-log-master-remote! [blob-id confirm-ch]
  (to-disk-utils/get-root-log-remote! get-root-log-master-chunk blob-id confirm-ch))

(defn delete-root-log-master-remote! [blob-id confirm-ch]
  (try
    (mysql-lib/execute! ["DELETE FROM `neversleepdb_root_log_master` WHERE `blob_id` = ?;" blob-id])
    (>!! confirm-ch true)
    (catch Exception e (>!! confirm-ch e))))
;=================================







(defn delete-all-data!
  "DELETES ALL DATA, use with care. Used in tests only."
  []
  (mysql-lib/execute! ["TRUNCATE TABLE `neversleepdb_blobs`;"])
  (mysql-lib/execute! ["TRUNCATE TABLE `neversleepdb_root_log`;"])
  ;(mysql-lib/execute! ["TRUNCATE TABLE `neversleepdb_root_log_master`;"])
  ;reset last_saved_root_log
  (mysql-lib/execute! ["UPDATE `neversleepdb_vars` SET `value` = ? WHERE `id` = '2';" 0]))