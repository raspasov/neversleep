(ns neversleep-db.b-plus-tree-root-log
  (:require [flatland.protobuf.core :as proto]
            [neversleep-db.util :as util]
            [neversleep-db.env :as env]
            [clojure.java.io :refer [output-stream input-stream delete-file file]]
            [neversleep-db.println-m :refer [println-m]]
            [clojure.core.async :refer [chan go <! >! <!! >!! go-loop put! thread alts! alt!! alts!! timeout]]
            [neversleep-db.io-utils :as io-utils])
  (:import (clojure.lang APersistentVector Volatile Agent Keyword)
           com.durablenode.Durablenode$BtreeRootLogEntry
           (java.io File)))

;Protocol buffer definitions
(def BtreeRootLogEntryProtoDef (proto/protodef Durablenode$BtreeRootLogEntry))
;end Protocol buffer definitions

(defn log-dir-location [] (str (env/data-dir) "b_plus_tree_root_logs/"))

(def log-file-extension "bptrl")
(def log-file-extension-temp "bptrltemp")

(defn get-log-file-path [blob-id]
  (str (log-dir-location) blob-id "." log-file-extension))

(defn clean-log-file-path []
  (str (log-dir-location) "0." log-file-extension))

(defn new-clean-log-file-path []
  (str (log-dir-location) "0." log-file-extension-temp))

(defn write-protobufs-to-log [^APersistentVector protobufs file-path confirm-ch]
  (try (do
         (with-open [out (output-stream (file file-path))]
           (apply proto/protobuf-write out protobufs))
         ;IO ok, return true
           (>!! confirm-ch true))
       ;not ok, catch exception, return e on the channel
       (catch Exception e (>!! confirm-ch e))))

(defn- read-protobufs-from-log [all-types-of-roots blob-id]
  (let [protobufs (proto/protobuf-seq BtreeRootLogEntryProtoDef (io-utils/blocking-loop io-utils/file-to-input-stream (get-log-file-path blob-id)))]
    (doseq [p protobufs]
      (doseq [{:keys [^Keyword log-key-wanted ^Volatile root-collector]} all-types-of-roots]
        (let [entity-id (-> p :entity)
              {:keys [cnt root]} (-> p log-key-wanted)]
          (vswap! root-collector assoc entity-id {:cnt cnt :root (util/byte-string-to-byte-array root)}))))))

(defn- gc-read-protobufs-from-log [^Volatile most-recent-roots blob-id]
  (let [protobufs (proto/protobuf-seq BtreeRootLogEntryProtoDef (io-utils/blocking-loop io-utils/file-to-input-stream (get-log-file-path blob-id)))]
    (doseq [p protobufs]
      (vswap! most-recent-roots assoc (-> p :entity) p))))


(defn- get-all-logs-sorted
  "Returns a sequence of maps: {:keys [^long blob-id ^String extension ^java.io.File file]}"
  []
  (->> (io-utils/blocking-loop io-utils/ls-directory (log-dir-location))
       ;only loop over files, not dirs
       (filter #(.isFile ^File %))
       (map (fn [file] (let [[file-name extension] (clojure.string/split (.getName ^File file) #"\.")]
                         {:blob-id file-name :extension extension :file file})))
       ;only use the file if it matches the log extension (safety check against foreign files in directory)
       (filter #(= log-file-extension (get % :extension)))
       (map #(assoc % :blob-id (util/parse-long (-> % :blob-id))))
       ;parse long from blob-id string
       ;sort by blob-id
       (sort-by :blob-id)))


(def gc-agent ^Agent (agent nil))

(defn- get-logs-less-than-or-equal-to [^long last-saved-blob-id]
  (->> (get-all-logs-sorted)
       (filter #(<= ^long (:blob-id %) last-saved-blob-id))))

(defn write-zero-log
  "Writes to a temp file and then uses mv shell command to atomically replace the zero log file"
  [protobufs]
  ;write to 0 temp
  (io-utils/blocking-loop write-protobufs-to-log protobufs (new-clean-log-file-path))
  ;move ("overwrite") 0 temp to 0
  (io-utils/blocking-loop io-utils/mv (new-clean-log-file-path) (clean-log-file-path)))

(defn start-gc
  "Called by the pipeline as soon as a blob-id is confirmed as a successful commit.
   Garbage collects the specified sequence of files by merging with the 0.bptrl file which
   contains the latest list of garbage collected BtreeRootLogEntry protobufs"
  [^long last-saved-blob-id]
  ;start GC
  ;only send to the agent if there isn't one running
  (when (= 0 (.getQueueCount ^Agent gc-agent))
    (send-off gc-agent (fn [_]
                         (let [logs-to-gc (get-logs-less-than-or-equal-to last-saved-blob-id)
                               most-recent-roots (volatile! {})]
                           ;walk all the files
                           (doseq [{:keys [blob-id]} logs-to-gc]
                             (gc-read-protobufs-from-log most-recent-roots blob-id))
                           ;write to zero log
                           (write-zero-log (vals @most-recent-roots))
                           ;done
                           ;delete all logs except the 0
                           (io-utils/delete-files (->> logs-to-gc
                                                       (remove #(= 0 (-> % :blob-id)))
                                                       (map :blob-id)
                                                       (map get-log-file-path))))))))


(defn get-roots-less-than-or-equal-to
  "Called at server startup to load all current b-tree roots"
  [last-saved-blob-id all-types-of-roots]
  (let [files (get-logs-less-than-or-equal-to last-saved-blob-id)]
    (doseq [{:keys [blob-id]} files]
      (read-protobufs-from-log all-types-of-roots blob-id))
    ;return, server ready to go
    (into [] (map :root-collector) all-types-of-roots)))


(defn delete-logs-bigger-than
  "Called at server startup to delete all logs that are ahead of last-saved-blob-id"
  [last-saved-blob-id]
  (let [paths-to-delete (->> (get-all-logs-sorted)
                             (filter #(< ^long last-saved-blob-id ^long (:blob-id %)))
                             (map :blob-id)
                             (map get-log-file-path))]
    (io-utils/delete-files paths-to-delete)))
