(ns neversleep-db.to-disk-local
  (:require [clojure.java.io :refer [file output-stream input-stream]]
            [clojure.java.shell :as shell :refer [sh with-sh-dir]]
            [neversleep-db.println-m :refer [println-m]]
            [clojure.core.async :refer [chan timeout go <! <!! >!! >! go-loop put! thread alts!! alts! dropping-buffer pipeline-blocking]])
  (:import (com.google.common.io ByteStreams)
           (java.io OutputStream)))

;TODO this namespace is work in progress, not functional atm

;(def byte-array-class (class (byte-array [])))
;
;(def base-path-blobs "resources/blobs/")
;(def base-path-blob-id "resources/blob_id/")
;
;(defn get-blob-file-path [blob-id entity-id]
;  (str base-path-blobs blob-id "|" entity-id ".nvrslpd"))
;
;(defn save-blob! [^bytes byte-array-data blob-id entity-id confirm-ch]
;  ;TODO catch exception
;  (try (do (with-open [out (output-stream (file (get-blob-file-path blob-id entity-id)))]
;             (.write out byte-array-data))
;           (println-m "local save-blob! about to return true")
;           (>!! confirm-ch true))
;       (catch Exception e (>!! confirm-ch e))))
;
;(defn get-blob! [blob-id entity-id confirm-ch]
;  (try (let [result (-> (input-stream (file (get-blob-file-path blob-id entity-id)))
;                        (ByteStreams/toByteArray))]
;         (>!! confirm-ch result))
;       (catch Exception e (>!! confirm-ch e))))
;
;(defn set-last-saved-blob-id! [blob-id confirm-ch]
;  (spit (str base-path-blob-id "last_saved_blob_id") blob-id)
;  (>!! confirm-ch true))
;
;(defn get-last-saved-blob-id! [confirm-ch]
;  (try
;    (->> (slurp (str base-path-blob-id "last_saved_blob_id"))
;         (neversleep-db.util/parse-long)
;         (>!! confirm-ch))
;    (catch Exception e (>!! confirm-ch e))))
;
;(defn delete-all-data! []
;  (with-sh-dir "resources"
;               (sh "rm" "-rf" "blobs")
;               (sh "mkdir" "blobs")
;               (sh "touch" "blobs/.empty")))
;
;(defn save-root-log! [b-tree-root-log-entries blob-id confirm-ch]
;  (>!! confirm-ch true))