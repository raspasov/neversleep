(ns neversleep-db.b-plus-tree-root-log-remote
  (:require [flatland.protobuf.core :as proto]
            [neversleep-db.util :as util]
            [clojure.java.io :refer [output-stream input-stream delete-file file]]
            [neversleep-db.println-m :refer [println-m]]
            [clojure.core.async :refer [chan go <! >! <!! >!! go-loop put! thread alts! alt!! alts!! timeout]]
            [neversleep-db.io-utils :as io-utils]
            [neversleep-db.to-disk :as to-disk]
            [neversleep-db.state :as state])
  (:import (clojure.lang Volatile Agent)
           com.durablenode.Durablenode$BtreeRootLogEntry
           (java.io ByteArrayInputStream)))


;Protocol buffer definitions
(def BtreeRootLogEntryProtoDef (proto/protodef Durablenode$BtreeRootLogEntry))
;end Protocol buffer definitions


(defn gc-read-protobufs-from-log-data [^Volatile most-recent-roots ^bytes b-a]
  (let [protobufs (proto/protobuf-seq BtreeRootLogEntryProtoDef (ByteArrayInputStream. b-a))]
    (doseq [p protobufs]
      (vswap! most-recent-roots assoc (-> p :entity) p))))


(defn restore-local-zero-log [last-saved-root-log]
  (let [byte-array-data (io-utils/blocking-loop to-disk/get-root-log-master-remote last-saved-root-log)
        ;mutable collector
        most-recent-roots (volatile! {})]
    ;if root log is found, read from it, otherwise just return
    (if-not (= false byte-array-data)
      (gc-read-protobufs-from-log-data most-recent-roots byte-array-data))
    (vals @most-recent-roots)))


(def gc-agent ^Agent (agent nil))

(defn start-gc
  "Called by the pipeline as soon as a blob-id is confirmed as a successful commit.
   Garbage collects the specified sequence of files by merging with the master log which
   contains the latest list of garbage collected BtreeRootLogEntry protobufs"
  ([^long last-saved-blob-id]
    ;start GC
    ;only send to the agent if there isn't one running
   (start-gc last-saved-blob-id (chan 1)))
  ([^long last-saved-blob-id confirm-ch]
   (if (= 0 (.getQueueCount ^Agent gc-agent))
     (send-off gc-agent (fn [_]
                          (let [last-saved-root-log (io-utils/blocking-loop to-disk/get-last-saved-root-log)
                                ;limit the amount of searching (for test execution)
                                blob-id-end (if (and (= 0 last-saved-root-log) (< 100 (- last-saved-blob-id last-saved-root-log))) (- last-saved-blob-id 100) last-saved-root-log)
                                ;return a sequence like this ({:blob-id 99 :data byte-array}, etc)
                                logs-to-gc (io-utils/blocking-loop to-disk/get-root-logs-remote-between last-saved-blob-id (+ blob-id-end 1))
                                ;mutable collector
                                most-recent-roots (volatile! {})
                                ;get last clean root log state if it exists
                                clean-root-logs (if-not (= 0 last-saved-root-log) (io-utils/blocking-loop to-disk/get-root-log-master-remote last-saved-root-log))]
                            ;load clean root logs if they exist
                            (when (instance? util/byte-array-class clean-root-logs)
                              (gc-read-protobufs-from-log-data most-recent-roots clean-root-logs))
                            ;walk the rest of the root logs
                            (doseq [{:keys [data]} logs-to-gc]
                              (gc-read-protobufs-from-log-data most-recent-roots data))
                            ;write to master root log
                            (io-utils/blocking-loop to-disk/save-root-log-master-remote (vals @most-recent-roots) last-saved-blob-id)
                            ;bump the last_saved_root_log var
                            (io-utils/blocking-loop to-disk/set-last-saved-root-log last-saved-blob-id)
                            ;done
                            ;gc root logs
                            (doseq [blob-id (map :blob-id logs-to-gc)]
                              (io-utils/blocking-loop to-disk/delete-root-log-remote blob-id))
                            ;gc master root logs
                            (doseq [blob-id @state/gc-master-root-logs]
                              (io-utils/blocking-loop to-disk/delete-root-log-master-remote blob-id))
                            (reset! state/gc-master-root-logs [])
                            ;schedule the current master root log to be GC-ed on the next pass
                            (swap! state/gc-master-root-logs conj last-saved-blob-id)
                            ;confirm end of GC
                            (>!! confirm-ch true)
                            nil)))
     ;GC is not going to run, return false to confirm-ch
     (>!! confirm-ch false))))


(defn delete-logs-bigger-than
  "Called at server startup to delete all logs that are ahead of last-saved-blob-id"
  [last-saved-blob-id]
  ;TODO this can never exceed 100, but still, how can we make this better so we don't generate excessive IO?
  (doseq [blob-id (range (inc last-saved-blob-id) (+ 100 last-saved-blob-id))]
    (io-utils/blocking-loop to-disk/delete-root-log-remote blob-id)))
