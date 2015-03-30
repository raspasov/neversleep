(ns neversleep-db.api-byte-parser
  (:require [neversleep-db.util :as util]
            [cheshire.core :as cheshire]
            [neversleep-db.serializer :as serializer]
            [clojure.edn])
  (:import (clojure.lang APersistentVector LazySeq)
           (java.nio ByteBuffer)))

(defmacro try-parse [& body]
  `(try ~@body
        (catch Exception e# e#)))

(defn- parse-json [string]
  (let [json-data (clojure.walk/keywordize-keys (cheshire/parse-string string))]
    ;avoid cheshire inconsistency when parsing top level json array
    (if (instance? LazySeq json-data)
      (into [] json-data)
      json-data)))

(defn- val-parse [^APersistentVector byte-vector]
  (let [;1 byte for the type
        val-type (nth byte-vector 0)
        ;rest is actual data
        b-a (byte-array (subvec byte-vector 1))]
    (condp = val-type
      ;json
      (byte 0) (parse-json (String. b-a "UTF-8"))
      ;string
      (byte 1) (String. b-a "UTF-8")
      ;byte
      (byte 2) (nth byte-vector 1)
      ;short
      (byte 3) (serializer/get-short b-a)
      ;int
      (byte 4) (serializer/get-int b-a)
      ;long
      (byte 5) (serializer/get-long b-a)
      ;bool
      (byte 6) (if (= (byte 1) (nth byte-vector 1)) true false)
      ;nil
      (byte 7) nil
      ;nippy
      (byte 8) (util/de-serialize b-a)
      (throw (Exception. (str "Unrecognized type in val-parse: " val-type))))))

(defn- io-dissoc-parse [command ^APersistentVector byte-vector]
  (try-parse
    (let [;1 byte
          verbose-mode (long (nth byte-vector 1))
          ;16 bytes
          request-uuid (util/bytes-to-uuid (byte-array (subvec byte-vector 2 18)))
          ;1 byte
          entity-id-length (long (nth byte-vector 18))
          ;offset from start - how many bytes are fixed at the start of request
          offset 19
          ;n bytes
          entity-id (String. (byte-array (subvec byte-vector offset (+ offset entity-id-length))))
          ;1 byte
          key-length (long (nth byte-vector (+ offset entity-id-length)))
          ;n bytes
          key (String. (byte-array (subvec byte-vector (+ offset entity-id-length 1) (+ offset entity-id-length 1 key-length))))]

      ;[command verbose-mode request-uuid entity-id key]
      {:command command :verbose-mode verbose-mode :request-uuid request-uuid :entity-id entity-id :key key})))



(defn- io-assoc-in-json-parse [command ^APersistentVector byte-vector]
  (try-parse
    (let [deep-key-xf (map #(cond (string? %) (keyword %)
                                  (number? %) %
                                  :else (throw (Exception. "Only numbers and strings allowed in deep key"))))
          ;1 byte
          verbose-mode (long (nth byte-vector 1))
          ;16 bytes
          request-uuid (util/bytes-to-uuid (byte-array (subvec byte-vector 2 18)))
          ;1 byte
          entity-id-length (long (nth byte-vector 18))
          ;offset from start - how many bytes are fixed at the start of request
          offset 19
          ;n bytes
          entity-id (String. (byte-array (subvec byte-vector offset (+ offset entity-id-length))))
          ;1 byte
          key-length (long (nth byte-vector (+ offset entity-id-length)))
          ;n bytes
          key (String. (byte-array (subvec byte-vector (+ offset entity-id-length 1) (+ offset entity-id-length 1 key-length))))
          ;4 bytes
          deep-key-length (long (serializer/four-bytes-to-int (byte-array (subvec byte-vector (+ offset entity-id-length 1 key-length) (+ offset entity-id-length 1 key-length 4)))))
          ;n bytes
          ;transform strings to keywords and ensure only strings and numbers present
          deep-key (into [] deep-key-xf (clojure.edn/read-string (String. (byte-array (subvec byte-vector (+ offset entity-id-length 1 key-length 4) (+ offset entity-id-length 1 key-length 4 deep-key-length))))))
          ;4 bytes
          val-length (long (serializer/four-bytes-to-int (byte-array (subvec byte-vector (+ offset entity-id-length 1 key-length 4 deep-key-length) (+ offset entity-id-length 1 key-length 4 deep-key-length 4)))))
          ;n bytes
          val (val-parse (subvec byte-vector (+ offset entity-id-length 1 key-length 4 deep-key-length 4) (+ offset entity-id-length 1 key-length 4 deep-key-length 4 val-length)))]
      ;[command verbose-mode request-uuid entity-id key deep-key val]
      {:command command :verbose-mode verbose-mode :request-uuid request-uuid :entity-id entity-id :key key :deep-key deep-key :val val})))

(defn- io-dissoc-in-json-parse [command ^APersistentVector byte-vector]
  (try-parse
    (let [deep-key-xf (map #(cond (string? %) (keyword %)
                                  (number? %) %
                                  :else (throw (Exception. "Only numbers and strings allowed in deep key"))))
          ;1 byte
          verbose-mode (long (nth byte-vector 1))
          ;16 bytes
          request-uuid (util/bytes-to-uuid (byte-array (subvec byte-vector 2 18)))
          ;1 byte
          entity-id-length (long (nth byte-vector 18))
          ;offset from start - how many bytes are fixed at the start of request
          offset 19
          ;n bytes
          entity-id (String. (byte-array (subvec byte-vector offset (+ offset entity-id-length))))
          ;1 byte
          key-length (long (nth byte-vector (+ offset entity-id-length)))
          ;n bytes
          key (String. (byte-array (subvec byte-vector (+ offset entity-id-length 1) (+ offset entity-id-length 1 key-length))))
          ;4 bytes
          deep-key-length (long (serializer/four-bytes-to-int (byte-array (subvec byte-vector (+ offset entity-id-length 1 key-length) (+ offset entity-id-length 1 key-length 4)))))
          ;n bytes
          ;transform strings to keywords and ensure only strings and numbers present
          deep-key (into [] deep-key-xf (clojure.edn/read-string (String. (byte-array (subvec byte-vector (+ offset entity-id-length 1 key-length 4) (+ offset entity-id-length 1 key-length 4 deep-key-length))))))]
      ;[command verbose-mode request-uuid entity-id key deep-key val]
      {:command command :verbose-mode verbose-mode :request-uuid request-uuid :entity-id entity-id :key key :deep-key deep-key :val val})))

(defn- io-assoc-parse [command ^APersistentVector byte-vector]
  (try-parse
    (let [;1 byte
          verbose-mode (long (nth byte-vector 1))
          ;16 bytes
          request-uuid (util/bytes-to-uuid (byte-array (subvec byte-vector 2 18)))
          ;1 byte
          entity-id-length (long (nth byte-vector 18))
          ;offset from start - how many bytes are fixed at the start of request
          offset 19
          ;n bytes
          entity-id (String. (byte-array (subvec byte-vector offset (+ offset entity-id-length))))
          ;1 byte
          key-length (long (nth byte-vector (+ offset entity-id-length)))
          ;n bytes
          key (String. (byte-array (subvec byte-vector (+ offset entity-id-length 1) (+ offset entity-id-length 1 key-length))))
          ;4 bytes
          val-length (long (serializer/four-bytes-to-int (byte-array (subvec byte-vector (+ offset entity-id-length 1 key-length) (+ offset entity-id-length 1 key-length 4)))))
          ;n bytes
          val (val-parse (subvec byte-vector (+ offset entity-id-length 1 key-length 4) (+ offset entity-id-length 1 key-length 4 val-length)))]
      ;[command verbose-mode request-uuid entity-id key val]
      {:command command :verbose-mode verbose-mode :request-uuid request-uuid :entity-id entity-id :key key :val val})))


(defn- io-get-key-as-of-parse [command ^APersistentVector byte-vector]
  (try-parse
    (let [;1 byte
          verbose-mode (long (nth byte-vector 1))
          ;16 bytes
          request-uuid (util/bytes-to-uuid (byte-array (subvec byte-vector 2 18)))
          ;1 byte
          entity-id-length (long (nth byte-vector 18))
          ;offset from start - how many bytes are fixed at the start of request
          offset 19
          ;n bytes
          entity-id (String. (byte-array (subvec byte-vector offset (+ offset entity-id-length))))
          ;1 byte
          key-length (long (nth byte-vector (+ offset entity-id-length)))
          ;n bytes
          key (String. (byte-array (subvec byte-vector (+ offset entity-id-length 1) (+ offset entity-id-length 1 key-length))))
          ;19 bytes - nano timestamp as a string
          timestamp (String. (byte-array (subvec byte-vector (+ offset entity-id-length 1 key-length) (+ offset entity-id-length 1 key-length 19))))]
      ;[command verbose-mode request-uuid entity-id key timestamp]
      {:command command :verbose-mode verbose-mode :request-uuid request-uuid :entity-id entity-id :key key :timestamp timestamp})))

(defn- io-get-entity-as-of-parse [command ^APersistentVector byte-vector]
  (try-parse
    (let [;1 byte
          verbose-mode (long (nth byte-vector 1))
          ;16 bytes
          request-uuid (util/bytes-to-uuid (byte-array (subvec byte-vector 2 18)))
          ;1 byte
          entity-id-length (long (nth byte-vector 18))
          ;offset from start - how many bytes are fixed at the start of request
          offset 19
          ;n bytes
          entity-id (String. (byte-array (subvec byte-vector offset (+ offset entity-id-length))))
          ;19 bytes - nano timestamp as a string
          timestamp (String. (byte-array (subvec byte-vector (+ offset entity-id-length) (+ offset entity-id-length 19))))]
      ;[command verbose-mode request-uuid entity-id timestamp]
      {:command command :verbose-mode verbose-mode :request-uuid request-uuid :entity-id entity-id :timestamp timestamp})))

(defn- io-get-all-version-between-parse [command ^APersistentVector byte-vector]
  (try-parse
    (let [;1 byte
          verbose-mode (long (nth byte-vector 1))
          ;16 bytes
          request-uuid (util/bytes-to-uuid (byte-array (subvec byte-vector 2 18)))
          ;1 byte
          entity-id-length (long (nth byte-vector 18))
          ;offset from start - how many bytes are fixed at the start of request
          offset 19
          ;n bytes
          entity-id (String. (byte-array (subvec byte-vector offset (+ offset entity-id-length))))
          ;19 bytes - nano timestamp as a string
          timestamp-start (String. (byte-array (subvec byte-vector (+ offset entity-id-length) (+ offset entity-id-length 19))))
          ;19 bytes - nano timestamp as a string
          timestamp-end (String. (byte-array (subvec byte-vector (+ offset entity-id-length 19) (+ offset entity-id-length 19 19))))
          ;4 bytes - limit
          limit (int (serializer/four-bytes-to-int (byte-array (subvec byte-vector (+ offset entity-id-length 19 19) (+ offset entity-id-length 19 19 4)))))]
      ;[command verbose-mode request-uuid entity-id timestamp-start timestamp-end limit]
      {:command command :verbose-mode verbose-mode :request-uuid request-uuid :entity-id entity-id :timestamp-start timestamp-start :timestamp-end timestamp-end :limit limit})))

(defn parse-byte-vector [^APersistentVector byte-vector]
  ;first byte determines the command
  (let [language (nth byte-vector 0)
        api-version (nth byte-vector 1)
        command (nth byte-vector 2)
        ;slice off language, version byte
        byte-vector (subvec byte-vector 2)]
    (merge {:language language :api-version api-version}
           (condp = command
             ;io-assoc any type
             (byte 1) (io-assoc-parse command byte-vector)
             ;io-dissoc
             (byte 2) (io-dissoc-parse command byte-vector)
             ;io-assoc-in json
             (byte 3) (io-assoc-in-json-parse command byte-vector)
             ;io-dissoc-in json
             (byte 4) (io-dissoc-in-json-parse command byte-vector)
             ;io-get-key-as-of
             (byte -128) (io-get-key-as-of-parse command byte-vector)
             ;io-get-entity-as-of
             (byte -127) (io-get-entity-as-of-parse command byte-vector)
             ;io-get-all-version-between
             (byte -126) (io-get-all-version-between-parse command byte-vector)
             ;else - command not recognized
             {}))))

