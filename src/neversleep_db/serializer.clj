(ns neversleep-db.serializer
  (:require [neversleep-db.util :as util]
            [flatland.protobuf.core :as proto]
            [neversleep-db.constants :as constants])
  (:import java.nio.ByteBuffer
           (java.util Arrays)
           (java.io ByteArrayOutputStream)))


(defn node-pointer-to-typed-byte-array [uuid ^long blob-id]
  (-> (ByteBuffer/allocate 13)
      (.put (byte 1))
      (.putInt (int uuid))
      (.putLong blob-id)
      (.array)))

(defn nippy-byte-array-to-typed-byte-array [^bytes b-a]
  (-> (ByteBuffer/allocate (+ 1 (count b-a)))
      (.put (byte 2))
      (.put b-a)
      (.array)))


(defn get-node-id [^bytes b-a]
  (-> (ByteBuffer/wrap b-a)
      (.getInt 1)))

(defn get-blob-id [^bytes b-a]
  (-> (ByteBuffer/wrap b-a)
      (.getLong 5)))

(defn get-data
  "Returns all the bytes after the first one"
  [^ByteBuffer b-b]
  (let [new-b-a (byte-array (unchecked-subtract-int (count (.array b-b)) 1))]
    (-> b-b
        (.get new-b-a))
    new-b-a))

(defn byte-array-type-dispatch [^bytes b-a]
  (let [byte-buffer (ByteBuffer/wrap b-a)
        byte-array-type (.get byte-buffer)]
    (condp = byte-array-type
           (byte 1) [(byte 1) (get-data byte-buffer)]
           (byte 2) [(byte 2) (get-data byte-buffer)])))

;TCP
(defn get-short [^bytes b-a]
  (-> (ByteBuffer/wrap b-a)
      (.getShort)))

(defn get-long [^bytes b-a]
  (-> (ByteBuffer/wrap b-a)
      (.getLong)))

(defn get-int [^bytes b-a]
  (-> (ByteBuffer/wrap b-a)
      (.getInt)))

(defn short-to-b-a [a-short]
  (-> (ByteBuffer/allocate 2)
      (.putShort (short a-short))
      (.array)))


;blob partitioning
(defn int-to-four-bytes [i]
  (-> (ByteBuffer/allocate 4)
      (.putInt (int i))
      (.array)))

(defn four-bytes-to-int [^bytes b-a]
  (-> (ByteBuffer/wrap b-a)
      (.getInt)))

(defn take-byte-array [^long n ^bytes an-array]
  (Arrays/copyOfRange an-array 0 n))

(defn partition-byte-array
  "Partitions byte arrays in chunks equal to partition size.
   Adds four bytes to the end of each chunk.
   For the zero-part chunk, those bytes represent the last idx in the chain;
   For all parts after that, the four bytes point to the next chunk idx;
   The chain ends whenever the last four bytes in a chunk are equal to (int 0)"
  [^bytes b-a ^long partition-size]
  ;determine last index
  (let [last-part-idx (-> (/ (count b-a) partition-size) (Math/ceil) (int) (- 1))
        ;_ (println last-part-idx)
        xf (comp (partition-all partition-size) ;partition the byte array
                 (util/map-indexed-xf (fn [idx byte-array-chunk]
                                        ;if we're at the last index, use -1, otherwise idx from map-indexed
                                        (let [^int idx' (cond (= idx last-part-idx)
                                                              (int -1)
                                                              ;prefix the total chain length to part zero byte array
                                                              (= idx 0)
                                                              (int (- last-part-idx 1))
                                                              :else
                                                              idx)]
                                          {:part idx :data (byte-array (concat byte-array-chunk (int-to-four-bytes (inc idx'))))}))))]
    (sequence xf b-a)))


(defn get-last-four-bytes-int [^bytes byte-array-chunk]
  (let [last-idx (dec (count byte-array-chunk))
        last-four-bytes (byte-array [(aget byte-array-chunk (- last-idx 3))
                                     (aget byte-array-chunk (- last-idx 2))
                                     (aget byte-array-chunk (- last-idx 1))
                                     (aget byte-array-chunk last-idx)])]
    (get-int last-four-bytes)))

(defn merge-byte-arrays [^bytes b-a-1 ^bytes b-a-2]
  (let [new-byte-array (byte-array (+ (alength b-a-1) (alength b-a-2)))]
    (System/arraycopy b-a-1 0 new-byte-array 0 (alength b-a-1))
    (System/arraycopy b-a-2 0 new-byte-array (alength b-a-1) (alength b-a-2))
    new-byte-array))

(defn combine-byte-arrays
  "Reverses partition-byte-array"
  ([^bytes b-a]
    b-a)
  ([^bytes init-b-a ^bytes b-a]
   (merge-byte-arrays init-b-a (take-byte-array (- (alength b-a) 4) b-a))))

(defn slice-byte-array [^bytes an-array ^long start ^long end]
  ;prevent trailing zeros in the result byte array
  (if (< (alength an-array) end)
    (let [end (alength an-array)]
      (Arrays/copyOfRange an-array start end))
    ;else, cut without adjusting
    (Arrays/copyOfRange an-array start end)))


(defn protobufs-to-byte-array [protobufs]
  (let [^ByteArrayOutputStream out (ByteArrayOutputStream.)
        _ (apply proto/protobuf-write out protobufs)]
    (.toByteArray out)))