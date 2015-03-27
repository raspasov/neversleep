(ns neversleep-db.command-log
  (:require [taoensso.nippy :as nippy]
            [flatland.protobuf.core :refer :all]
            [clojure.core.async :as async :refer [chan go <! >! <!! >!! go-loop put! thread alts! alt!! alts!! timeout]]
            [clojure.java.io :refer [file output-stream input-stream delete-file]]
            [neversleep-db.constants :as constants]
            [neversleep-db.println-m :refer [println-m]]
            [neversleep-db.util :as util]
            [neversleep-db.io-utils :as io-utils]
            [neversleep-db.env :as env])
  (:import (flatland.protobuf PersistentProtocolBufferMap PersistentProtocolBufferMap$Def PersistentProtocolBufferMap$Def$NamingStrategy Extensions)
           (com.google.protobuf ByteString)
           com.durablenode.Durablenode$CommandLogEntry
           (clojure.lang APersistentVector)
           (java.io File)))

(def command-log-chan (chan 1024))

;Protobufs
(def CommandLogEntryProtoDef (protodef Durablenode$CommandLogEntry))

(defn- command-log-entry->protobuf
  "Returns a protobuf for this command log entry."
  [entity-type-id command params]
  (protobuf CommandLogEntryProtoDef
                            :entitytype entity-type-id
                            :command command
                            :params (util/copy-to-byte-string (util/serialize params))))

(def log-file-extension "cmdl")
(defn log-dir-location [] (str (env/data-dir) "command_log/"))

(defn get-log-file-path [blob-id]
  (str (log-dir-location) blob-id "." log-file-extension))

;File name helpers
(defn- command-log-file-name-for-blob
  "Creates the file for the command log for the blob-id.  Each blob-id will have it's own file."
  [blob-id]
  (str (log-dir-location) blob-id "." log-file-extension))

(def writer (agent nil))

;Writing and reading from file
(defn- write-protobufs-and-confirm
  "Write the given protobufs to the given output stream then confirm"
  [blob-id confirm-chans protobufs num-of-cmds-written pipeline-command-log-ch]
  (try
    (send-off writer (fn [_] (when (< 0 (count protobufs))
                               ;Only write if we actually have some data saved up
                               (with-open [out (output-stream (file (command-log-file-name-for-blob blob-id)) :append true)]
                                 (apply protobuf-write out protobufs))
                               (doseq [ch confirm-chans] (>!! ch 1))
                               ;confirmation for the pipeline
                               (when (= constants/blob-size num-of-cmds-written)
                                 (>!! pipeline-command-log-ch true))
                               ;return nil because agent does not need state
                               nil)))
    (catch Exception e (>!! neversleep-db.exception-log/incoming-exceptions (agent-error writer)))))


(defn- read-protobufs-from-blob-id
  "Reads command log protobufs from the command log file for this blob-id."
  [blob-id]
  (protobuf-seq CommandLogEntryProtoDef (input-stream (file (command-log-file-name-for-blob blob-id)))))

(def timeout-length 1)

;Loop
(def loop-started? (atom false))

(defn start-write-protobuf-loop []
  (println-m "starting command log loop")
  ;start loop only once
  (when (= false @loop-started?)
    (thread
      (loop [protobufs []
             confirm-chans []
             last-fsync-ts (System/nanoTime)
             timeout-chan (async/timeout timeout-length)
             last-blob-id -1
             num-of-cmds-written 1
             current-pipeline-command-log-ch nil]

        (let [nano-secs-since-fsync (- (System/nanoTime) last-fsync-ts)]
          ;if less than 1 ms has passed since fsync...
          (if (<= nano-secs-since-fsync 1000000)
            ;continue buffering ops since it has been less than 1ms (1000000 nano seconds)
            (let [[[data api-confirm-ch pipeline-command-log-ch blob-id] _] (alts!! [command-log-chan
                                                                                     timeout-chan])]
              (if data
                ;recur and buffer more ops before writing to to disk
                (if (= blob-id last-blob-id)
                  ;Same blob so we can continue our collection
                  (recur (conj protobufs data) (conj confirm-chans api-confirm-ch) last-fsync-ts timeout-chan (long blob-id) (+ 1 num-of-cmds-written) current-pipeline-command-log-ch)
                  ;Different blob-id so we should close this collection and write to disk
                  (do (write-protobufs-and-confirm last-blob-id confirm-chans protobufs num-of-cmds-written current-pipeline-command-log-ch)
                      ;Add the data we received in this loop and recur
                      ;also new pipeline-command-log-ch
                      (recur (conj [] data) (conj [] api-confirm-ch) (System/nanoTime) (async/timeout timeout-length) (long blob-id) 1 pipeline-command-log-ch)))
                ;data is nil - time to write to disk for safety.  Happens via alts!
                (do (write-protobufs-and-confirm last-blob-id confirm-chans protobufs num-of-cmds-written current-pipeline-command-log-ch)
                    (recur [] [] (System/nanoTime) (async/timeout timeout-length) last-blob-id num-of-cmds-written current-pipeline-command-log-ch))))
            ;else time to write to disk because it has been longer than 1 ms since our last write.  (not triggered from a write)
            (do (write-protobufs-and-confirm last-blob-id confirm-chans protobufs num-of-cmds-written current-pipeline-command-log-ch)
                (recur [] [] (System/nanoTime) (async/timeout timeout-length) last-blob-id num-of-cmds-written current-pipeline-command-log-ch))))))
    (reset! loop-started? true)))

;Public
(defn write-to-command-log
  "Writes to the command log file for this blob-id."
  [^Long entity-type-id ^String command ^APersistentVector params api-confirm-ch pipeline-command-log-ch blob-id]
  (>!! command-log-chan [(command-log-entry->protobuf entity-type-id command params) api-confirm-ch pipeline-command-log-ch blob-id]))

;File IO functions
;======================================================================================================================
(defn get-all-logs-sorted
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


(defn get-logs-bigger-than [blob-id]
  (->> (get-all-logs-sorted)
       (remove #(<= (-> % :blob-id) blob-id))
       (map #(assoc % :protobufs (read-protobufs-from-blob-id (-> % :blob-id))))))


(defn get-all-commands [log-files]
  (->> log-files
       (map #(assoc % :protobufs (read-protobufs-from-blob-id (-> % :blob-id))))
       (mapcat :protobufs)
       (flatten)))

(defn delete-command-log
  "Removes the command log from the file system. Called when we are assured the blob has been written."
  [blob-id]
  (io-utils/blocking-loop io-utils/delete-file (command-log-file-name-for-blob blob-id)))


(defn delete-logs [log-files]
  (->> log-files
       (map :blob-id)
       (map get-log-file-path)
       (io-utils/delete-files)))

(defn delete-logs-smaller-than-or-equal-to
  [last-saved-blob-id]
  (let [paths-to-delete (->> (get-all-logs-sorted)
                             (filter #(<= (-> % :blob-id) last-saved-blob-id))
                             (map :blob-id)
                             (map get-log-file-path))]
    (io-utils/delete-files paths-to-delete)))


(defn init
  "Called once at server start"
  []
  (start-write-protobuf-loop))

;EXPLORE LOGS
;(defn print-all-commands []
;  (let [log-files (get-all-logs-sorted)
;        all-commands (get-all-commands log-files)]
;    (doseq [command all-commands]
;      (println command))))
;
;(defn count-all-commands [blob-id]
;  (let [log-files (get-logs-bigger-than blob-id)
;        all-commands (get-all-commands log-files)]
;    (count all-commands)))
;
;(defn get-entity-id-from-commands [entity-id blob-id]
;  (filter #(= entity-id (-> % :entity)) (get-all-commands (get-logs-bigger-than blob-id))))
;
;(defn get-all-commands-and-files [log-files]
;  (map (juxt :file :protobufs) log-files))
;
;(defn find-entity-and-key [entity k blob-id]
;  (let [return (atom nil)]
;
;    (doseq [[f protobufs] (get-all-commands-and-files (get-logs-bigger-than blob-id))]
;      (doseq [p protobufs]
;        (when (and (= entity (-> p :entity)) (= k (-> p :key)))
;          (println-m (.getName f))
;          (reset! return f))))
;    @return))