(ns neversleep-db.to-disk
  (:require [neversleep-db.util :as util]
            [neversleep-db.to-disk-mysql :as to-disk-mysql]
            [neversleep-db.io-utils :as io-utils]
            [neversleep-db.serializer :as serializer]
            [flatland.protobuf.core :refer :all]
            [neversleep-db.println-m :refer [println-m]]
            [clojure.core.async :refer [chan timeout go <! <!! >!! >! go-loop put! thread alts!! alts! dropping-buffer pipeline-blocking]]
            [neversleep-db.env :as env]))

(declare get-blob)

(def to-disk-config
  (if-let [disk-config (-> env/env :config :nvrslp-to-disk-config)]
    disk-config
    ;otherwise, set default
    :mysql))

(defmacro def-vars
  "Initiates vars according to type of disk storage.
   Possible values include: :mysql, :local"
  []
  (let [a-ns (condp = to-disk-config
               :mysql "to-disk-mysql"
               "to-disk-local")]
    `(do
       ;set all the vars according to the config
       ;BLOBS
       ;commit operations
       (def set-last-saved-blob-id! ~(symbol (str a-ns "/set-last-saved-blob-id!")))
       (def get-last-saved-blob-id! ~(symbol (str a-ns "/get-last-saved-blob-id!")))
       ;blob ops
       (def save-blob! ~(symbol (str a-ns "/save-blob!")))
       (def get-blob! ~(symbol (str a-ns "/get-blob!")))
       ;END BLOBS
       ;=================
       ;ROOT LOGS
       ;commit operations
       (def set-last-saved-root-log! ~(symbol (str a-ns "/set-last-saved-root-log!")))
       (def get-last-saved-root-log! ~(symbol (str a-ns "/get-last-saved-root-log!")))
       ;regular
       (def save-root-log-remote! ~(symbol (str a-ns "/save-root-log-remote!")))
       (def get-root-log-remote! ~(symbol (str a-ns "/get-root-log-remote!")))
       (def get-root-logs-remote-between! ~(symbol (str a-ns "/get-root-logs-remote-between!")))
       (def delete-root-log-remote! ~(symbol (str a-ns "/delete-root-log-remote!")))

       ;master
       (def save-root-log-master-remote! ~(symbol (str a-ns "/save-root-log-master-remote!")))
       (def get-root-log-master-remote! ~(symbol (str a-ns "/get-root-log-master-remote!")))
       (def delete-root-log-master-remote! ~(symbol (str a-ns "/delete-root-log-master-remote!")))
       ;END ROOT LOGS
       ;danger zone
       (def delete-all-data! ~(symbol (str a-ns "/delete-all-data!")))
       true)))

;init the vars
(def-vars)

(def allocate-next-blob-id-agent (agent nil))

(defn check-blob-id-value-local
  "Safety check on server startup"
  [last-saved-blob-id confirm-ch]
  (try
    (let [file-path (str (env/data-dir) "blob_id/value")
          value-file (clojure.java.io/as-file file-path)
          current-blob-id (if (.exists value-file)
                            ;file exists
                            (util/parse-long (slurp file-path))
                            ;create the value file
                            (do (spit file-path 1)
                                (util/parse-long (slurp file-path))))]
      (if (< current-blob-id last-saved-blob-id)
        ;abort start
        (>!! confirm-ch false)
        ;else - ok to start
        (>!! confirm-ch true)))
    (catch Exception e (>!! confirm-ch e))))

(defn allocate-next-blob-id
  "Allocates the next blob-id in a local file"
  [confirm-ch]
  (send-off allocate-next-blob-id-agent
            (fn [_]
              (let [file-path (str (env/data-dir) "blob_id/value")
                    current-blob-id (util/parse-long (slurp file-path))
                    next-blob-id (+ 1 current-blob-id)]
                (spit file-path next-blob-id)
                (>!! confirm-ch next-blob-id)
                nil))))

;IFn to be implemented by storage engines
;==============================================================================

;BLOBS
;commit operations
(defn set-last-saved-blob-id [blob-id confirm-ch]
  (set-last-saved-blob-id! blob-id confirm-ch))
(defn get-last-saved-blob-id [confirm-ch]
  (get-last-saved-blob-id! confirm-ch))

;blob ops
(defn save-blob [blob blob-id entity-id confirm-ch]
  (save-blob! blob blob-id entity-id confirm-ch))
(defn get-blob [blob-id entity-id confirm-ch]
  (get-blob! blob-id entity-id confirm-ch))

;ROOT LOG
;commit operations
(defn set-last-saved-root-log [blob-id confirm-ch]
  (set-last-saved-root-log! blob-id confirm-ch))
(defn get-last-saved-root-log [confirm-ch]
  (get-last-saved-root-log! confirm-ch))

;regular
(defn save-root-log-remote [b-tree-root-log-entries blob-id confirm-ch]
  (let [byte-array-data (serializer/protobufs-to-byte-array b-tree-root-log-entries)]
    (save-root-log-remote! byte-array-data blob-id confirm-ch)))

;(defn get-root-log-remote
;  [blob-id confirm-ch]
;  (get-root-log-remote! blob-id confirm-ch))

(defn get-root-logs-remote-between [blob-id-start blob-id-end confirm-ch]
  (get-root-logs-remote-between! blob-id-start blob-id-end confirm-ch))

(defn delete-root-log-remote [blob-id confirm-ch]
  (delete-root-log-remote! blob-id confirm-ch))
;==============

;master
(defn save-root-log-master-remote [b-tree-root-log-entries blob-id confirm-ch]
  (let [byte-array-data (serializer/protobufs-to-byte-array b-tree-root-log-entries)]
    (save-root-log-master-remote! byte-array-data blob-id confirm-ch)))

(defn get-root-log-master-remote [blob-id confirm-ch]
  (get-root-log-master-remote! blob-id confirm-ch))

(defn delete-root-log-master-remote [blob-id confirm-ch]
  (delete-root-log-master-remote! blob-id confirm-ch))

;DANGER ZONE
(defn delete-all-data
  "DELETES ALL DATA, use with care. Used in tests only."
  []
  (delete-all-data!))

