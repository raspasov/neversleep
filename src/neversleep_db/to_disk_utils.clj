(ns neversleep-db.to-disk-utils
  (:require [neversleep-db.serializer :as serializer]
            [clojure.core.async :refer [chan timeout go <! <!! >!! >! go-loop put! thread alts!! alts! dropping-buffer pipeline-blocking]]
            [neversleep-db.util :as util]
            [neversleep-db.constants :as constants])
  (:import (clojure.lang IFn)))

(defn- construct-byte-array
  "Given an initial chunk of a byte-array, constructs the whole blob (uses IO)"
  [^IFn io-fn ^bytes part-zero blob-id entity-id]
  ;last idx in the chain, if it's equal to zero, the chain has only one part, i.e. part-zero
  (let [last-part-idx (serializer/get-last-four-bytes-int part-zero)]
    (loop [next-part-idx 1
           ^bytes current-chunk part-zero
           result (byte-array 0)]
      ;check if we've reached the end of the byte-array chain...
      (if (or (= last-part-idx 0) (= next-part-idx 0))
        ;at the end of chunk sequence, finalize and return
        (reduce serializer/combine-byte-arrays result [current-chunk])
        ;else - fetch next chunk of byte-array
        (let [next-chunk (io-fn blob-id next-part-idx entity-id)
              new-result (reduce serializer/combine-byte-arrays result [current-chunk])]
          (recur (serializer/get-last-four-bytes-int next-chunk)
                 next-chunk
                 new-result))))))

;BLOBS
(defn save-blob! [io-fn byte-array-data blob-id entity-id confirm-ch]
  (try (let [byte-array-parts (serializer/partition-byte-array byte-array-data constants/blob-part-size)]
         (doseq [{:keys [part data]} byte-array-parts]
           (io-fn blob-id part entity-id data))
         (>!! confirm-ch true))
       (catch Exception e (>!! confirm-ch e))))

(defn get-blob! [io-fn blob-id entity-id confirm-ch]
  (try (let [part-zero (io-fn blob-id 0 entity-id)]
         (cond
           (instance? util/byte-array-class part-zero)
           ;ok - return
           (let [result (construct-byte-array io-fn part-zero blob-id entity-id)]
             (>!! confirm-ch result))
           ;not found
           (= nil part-zero)
           (>!! confirm-ch (Exception. (str "could not find blob-id " blob-id " entity-id " entity-id)))
           :else
           (>!! confirm-ch (Exception. (str "wrong data type, could not find blob-id " blob-id " entity-id " entity-id)))))
       (catch Exception e (>!! confirm-ch e))))

;ROOT LOGS
(defn save-root-log-remote! [io-fn byte-array-data blob-id confirm-ch]
  (try (let [byte-array-parts (serializer/partition-byte-array byte-array-data constants/blob-part-size)]
         ;save all the parts
         (doseq [{:keys [part data]} byte-array-parts]
           (io-fn blob-id part data))
         (>!! confirm-ch true))
       (catch Exception e (>!! confirm-ch e))))

(defn get-root-log-remote! [io-fn blob-id confirm-ch]
  (try
    (let [part-zero (io-fn blob-id 0 nil)]
      (cond
        (instance? util/byte-array-class part-zero)
        ;ok - return
        (let [result (construct-byte-array io-fn part-zero blob-id nil)]
          (>!! confirm-ch result))
        ;not found
        (= nil part-zero)
        (>!! confirm-ch false)
        :else
        (>!! confirm-ch (Exception. (str "wrong data type, could not find root-log " blob-id)))))
    (catch Exception e (>!! confirm-ch e))))


(defn get-root-logs-remote-between! [io-fn blob-id-start blob-id-end confirm-ch]
  (println "going to look for logs between " blob-id-start "and" blob-id-end)
  (try
    (loop [current-blob-id blob-id-start
           root-logs ()]
      (if (< current-blob-id blob-id-end)
        ;out of range, return
        (>!! confirm-ch root-logs)
        (let [part-zero (io-fn current-blob-id 0 nil)]
          (if (= nil part-zero)
            ;recur, no log at this index
            (recur (- current-blob-id 1) root-logs)
            ;otherwise, there is a log, construct it and recur
            (recur (- current-blob-id 1) (conj root-logs {:blob-id current-blob-id
                                                          :data    (construct-byte-array io-fn part-zero current-blob-id nil)}))))))
    (catch Exception e (>!! confirm-ch e))))

(defn delete-root-log-remote! [io-fn blob-id confirm-ch]
  ;TODO
  )

(defn delete-root-log-master-remote! [io-fn blob-id confirm-ch]
  ;TODO
  )

