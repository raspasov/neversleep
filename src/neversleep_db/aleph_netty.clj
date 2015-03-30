(ns neversleep-db.aleph-netty
  (:require
    [manifold.deferred :as d]
    [manifold.stream :as s]
    [aleph.tcp :as tcp]
    [gloss.core :as gloss]
    [clojure.core.async :refer [<!! >!! go <! >! alts! chan thread timeout pipe close!]]
    [gloss.io :as io]
    [neversleep-db.write-dispatch :as write-dispatch]
    [neversleep-db.read-dispatch :as read-dispatch]
    [neversleep-db.persistent-durable-map :as pdm]
    [neversleep-db.api-byte-parser :as parser]
    [neversleep-db.util :as util]
    [neversleep-db.state :as state])
  (:import (clojure.lang APersistentMap)))



(def protocol-server
  (gloss/compile-frame
    (gloss/finite-frame :int32
                        (gloss/repeated :byte :prefix :none))
    (fn [x] x)
    (fn [x] x)))

(def protocol-client
  (gloss/compile-frame
    (gloss/finite-block :int32)
    (fn [x] x)
    (fn [x] x)))

(defn wrap-duplex-stream
  [protocol s]
  (let [out (s/stream)]
    (s/connect
      (s/map #(io/encode protocol %) out)
      s)
    (s/splice
      out
      (io/decode-stream s protocol))))


(defn start-server
  [handler port]
  (tcp/start-server
    (fn [s info]
      (handler (wrap-duplex-stream protocol-server s) info))
    {:port port}))

(defn ch-modifier [{:keys [request-uuid language]}]
  ;determine if we're responding to Clojure or another language
  (if (= (byte 0) language)
    ;nippy serialization for Clojure
    (comp #(util/serialize %)
          #(merge % {:request-uuid request-uuid}))
    ;else - json
    (comp #(util/encode %)
          #(merge % {:request-uuid request-uuid}))))

(defn send-to-api [stream-in-ch stream-out-ch]
  (go
    (loop []
      (let [tcp-data (<! stream-in-ch)]
        (when-not (nil? tcp-data)
          ;(println "RAW TCP DATA::" tcp-data)
          (let [p (parser/parse-byte-vector tcp-data)]
            ;(println "GOT DATA ON TCP SOCKET::" p)
            (cond
              ;exception during parsing, respond with error
              (instance? Exception p)
              (do
                (>! stream-out-ch (util/encode {:error (.getMessage p)}))
                (recur))
              ;parsing was OK
              (instance? APersistentMap p)
              (let [{:keys [command request-uuid entity-id key val timestamp language]} p
                    timestamp (if timestamp (util/parse-timestamp timestamp))
                    request-uuid (str request-uuid)]
                (condp = command
                  ;WRITES
                  ;=============
                  ;io-assoc json
                  (byte 0)
                  (do
                    (write-dispatch/io-pdm-write pdm/io-assoc entity-id key val stream-out-ch (ch-modifier {:request-uuid request-uuid :language language}))
                    (recur))
                  ;io-assoc
                  (byte 1)
                  (do
                    (write-dispatch/io-pdm-write pdm/io-assoc entity-id key val stream-out-ch (ch-modifier {:request-uuid request-uuid :language language}))
                    (recur))
                  ;io-dissoc
                  (byte 2)
                  (do
                    (write-dispatch/io-pdm-write pdm/io-without entity-id key nil stream-out-ch (ch-modifier {:request-uuid request-uuid :language language}))
                    (recur))
                  ;io-assoc-in json
                  (byte 3)
                  (do
                    ;conj the entity key to the end of the deep-key vector
                    (write-dispatch/io-pdm-write pdm/io-assoc-in-json entity-id (conj (:deep-key p) key) val stream-out-ch (ch-modifier {:request-uuid request-uuid :language language}))
                    (recur))
                  ;io-dissoc-in json
                  (byte 4)
                  (do
                    ;conj the entity key to the end of the deep-key vector
                    (write-dispatch/io-pdm-write pdm/io-dissoc-in-json entity-id (conj (:deep-key p) key) nil stream-out-ch (ch-modifier {:request-uuid request-uuid :language language}))
                    (recur))
                  ;READS
                  ;=============
                  ;io-get-key-as-of
                  (byte -128)
                  (do
                    (>! @state/read-pipeline-from-ch
                        #(read-dispatch/io-find-as-of! entity-id key timestamp stream-out-ch (ch-modifier {:request-uuid request-uuid :language language})))
                    (recur))
                  ;io-get-entity
                  (byte -127)
                  (do
                    (>! @state/read-pipeline-from-ch
                        #(read-dispatch/io-get-entity-as-of! entity-id timestamp stream-out-ch (ch-modifier {:request-uuid request-uuid :language language})))
                    (recur))
                  ;io-get-all-versions-between
                  (byte -126)
                  (do
                    (>! @state/read-pipeline-from-ch
                        #(read-dispatch/io-get-all-versions-between!
                          entity-id (util/parse-timestamp (:timestamp-start p)) (util/parse-timestamp (:timestamp-end p)) (:limit p)
                          stream-out-ch (ch-modifier {:request-uuid request-uuid :language language})))
                    (recur))
                  ;else
                  (do
                    (>! stream-out-ch ((ch-modifier {:request-uuid request-uuid :language language}) {:error "Unknown API command"}))
                    (recur))))
              ;in any other case we have unrecognized data, return error
              :else
              (do
                (>! stream-out-ch ((ch-modifier {:request-uuid ""}) {:error "Unrecognized data structure during parsing in aleph-netty/send-to-api"}))
                (recur)))))))))

(defn tcp-api-handler
  []
  (fn [s info]
    (let [stream-in-ch (chan 1024)
          stream-out-ch (chan 1024)]
      (s/connect
        s
        stream-in-ch)
      (s/connect stream-out-ch
                 s)
      (send-to-api stream-in-ch stream-out-ch))))
