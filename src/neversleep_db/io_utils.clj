(ns neversleep-db.io-utils
  (:require [neversleep-db.constants :as constants]
            [neversleep-db.exception-log :as exception-log]
            [neversleep-db.println-m :refer [println-m]]
            [clojure.java.io :as jio]
            [clojure.core.async :refer [chan go >! <! <!! >!! go-loop put! thread alts! alts!! timeout pipeline close!]]
            [clojure.java.shell :as shell :refer [sh with-sh-dir]]
            [neversleep-db.backpressure :as backpressure])
  (:import (clojure.lang IFn ASeq)))


(defn blocking-loop
  "Blocking loop. Blocks a real thread until successfully completed.
   f must be a function that takes params plus a confirm channel to put the result on when done.
   If the result on the channel is an Exception, the exception will be logged. In that case, or in a case of timeout
   the operation is re-tried. Handles timeouts via alts!!"
  [^IFn f & params]
  (let [params (into [] params)]
    (loop [num-of-tries 0]
      (if (= num-of-tries constants/max-io-retries)
        ;block
        (backpressure/block-writes))
      (let [confirm-ch (chan 1)
            _ (apply f (conj params confirm-ch))
            [result _] (alts!! [confirm-ch (timeout 5000)])]
        (cond
          (and (not (instance? Exception result)) (not (nil? result)))
          ;write ok
          (do
            ;unblock
            (if (<= constants/max-io-retries num-of-tries)
              ;unblock
              (backpressure/unblock-writes))
            ;out of the loop
            result)
          (instance? Exception result)
          (do (>!! exception-log/incoming-exceptions result)
              ;wait with expontential backoff
              (<!! (timeout (* 5000 (inc num-of-tries))))
              (recur (inc num-of-tries)))
          (= nil result)
          (do (>!! exception-log/incoming-exceptions (Exception. "Timeout exception"))
              ;wait with expontential backoff
              (<!! (timeout (* 5000 (inc num-of-tries))))
              (recur (inc num-of-tries)))
          :else
          ;not ok, retry
          (do
            (println-m "going to retry blocking loop")
            ;wait with expontential backoff
            (<!! (timeout (* 5000 (inc num-of-tries))))
            (recur (inc num-of-tries))))))))


(defn ls-directory
  "Returns a sequence of File. objects"
  [path confirm-ch]
  (try (>!! confirm-ch (doall (file-seq (clojure.java.io/file path))))
       (catch Exception e (>!! confirm-ch e))))

(defn file-to-input-stream [path confirm-ch]
  (try
    (>!! confirm-ch (jio/input-stream path))
    (catch Exception e (>!! confirm-ch e))))


(defn delete-file
  "Helper function"
  [path-to-file confirm-ch]
  (try (.delete (jio/file path-to-file))
       ;IO ok, return true
       (>!! confirm-ch true)
       ;not ok, catch exception, return e on the channel
       (catch Exception e (>!! confirm-ch e))))


(defn delete-files
  "Delete multiple files, each operation is a potentially blocking-loop"
  [^ASeq paths-to-files]
  (doseq [path-to-file paths-to-files]
    (blocking-loop delete-file path-to-file)))


(def ^:const sh-success-result {:exit 0, :out "", :err ""})

(defn mv
  "Executes a shell mv command"
  [from-path to-path confirm-ch]
  (let [result (sh "mv" from-path to-path)]
    (if (= result sh-success-result)
      (>!! confirm-ch true)
      (>!! confirm-ch (Exception. (str "Unable to move file from " from-path " to " to-path))))))