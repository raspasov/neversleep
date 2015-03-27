(ns neversleep-db.util
  (:require [clojure.core.async :refer [chan timeout go <! <!! >!! >! go-loop put! thread alts!! alts! dropping-buffer]]
            [neversleep-db.println-m :refer [println-m]]
            [taoensso.nippy :as nippy]
            [cheshire.core :as cheshire])
  (:import (clojure.lang APersistentVector IFn)
           (com.google.protobuf ByteString)
           (jv SystemClock)
           (java.util UUID)
           (java.nio ByteBuffer)))

(def byte-array-class (class (byte-array 0)))


(defn timestamp[]
  (quot (System/currentTimeMillis) 1000))

(defn timestamp-with-mili []
  (/ (double (System/currentTimeMillis)) 1000))


(defn uuid [] (UUID/randomUUID))

(defn uuid-to-bytes
  "Generates a new UUID and converts it to byte array (16 bytes long)"
  [uuid]
  (-> (ByteBuffer/allocate 16)
      (.putLong (.getMostSignificantBits uuid))
      (.putLong (.getLeastSignificantBits uuid))
      (.array)))

(defn bytes-to-uuid
  "Takes a byte array of size 16 and converts it to UUID object"
  [^bytes b-a]
  (let [byte-buffer (ByteBuffer/wrap b-a)
        most-significant-bits (-> byte-buffer
                                  (.getLong 0))
        least-significant-bits (-> byte-buffer
                                   (.getLong 8))]
    (new UUID most-significant-bits least-significant-bits)))



(defn parse-long [s]
  (if (number? s)
    s
    (Long. ^String (re-find #"\d+" s))))

(defn parse-timestamp [s]
  (if (= s "___________________")
    (SystemClock/getTime)
    (parse-long s)))

(defn client-token []
  "12495f5fbb099a643c376adf0e5aa84d6c7e9ac995f97b81c0734e242aac931f")

(defn memo-cache-length
  "Returns number of miliseconds after which the memoize caches auto-expire"
  []
  ;20 min
  (* 60 20 1000))

(defn distinct-xf
  "Distinct transducer"
  []
  (fn [rf]
    (let [seen (volatile! #{})]
      (fn
        ([] (rf))              ;; init arity
        ([result] (rf result)) ;; completion arity
        ([result input]        ;; reduction arity
          (if (contains? @seen input)
            result
            (do (vswap! seen conj input)
                (rf result input))))))))

(defn distinct-mod-xf
  "Distinct transducer with modifier of the input"
  [^IFn modifier-f]
  (fn [rf]
    (let [seen (volatile! #{})]
      (fn
        ([] (rf))              ;; init arity
        ([result] (rf result)) ;; completion arity
        ([result input]        ;; reduction arity
         (if (contains? @seen (modifier-f input))
           result
           (do (vswap! seen conj (modifier-f input))
               (rf result input))))))))

(defn dedupe-xf
  "De-dupe transducer. Removes consecutive identical items"
  ([^IFn modifier-f]
   (fn [rf]
     (let [pv (volatile! ::none)]
       (fn
         ([] (rf))
         ([result] (rf result))
         ([result input]
          (let [prior @pv]
            (vreset! pv (modifier-f input))
            (if (= prior (modifier-f input))
              result
              (rf result input)))))))))

(defn map-indexed-xf
  "Returns a lazy sequence consisting of the result of applying f to 0
  and the first item of coll, followed by applying f to 1 and the second
  item in coll, etc, until coll is exhausted. Thus function f should
  accept 2 arguments, index and item. Returns a stateful transducer when
  no collection is provided."
  {:added "1.2"
   :static true}
  ([f]
   (fn [rf]
     (let [i (volatile! -1)]
       (fn
         ([] (rf))
         ([result] (rf result))
         ([result input]
          (rf result (f (vswap! i inc) input))))))))

;serialize/de-serialize
(defn serialize ^bytes [data]
  (let [frozen-data (nippy/freeze data {:skip-header? true :compressor nil :encryptor nil})]
    frozen-data))

(defn de-serialize [blob]
  (if blob
    (nippy/thaw blob {:skip-header? true :compressor nil :encryptor nil})
    nil))

(defn copy-to-byte-string ^ByteString [^bytes b]
  (ByteString/copyFrom b))

(defn byte-string-to-byte-array ^bytes [^ByteString bs]
  (.toByteArray bs))

(defn seq-to-map
  "Transform from a sequence to a map; if a-seq contains odd number of items last one will be ignored"
  [a-seq]
  (let [pair (volatile! [])]
    (persistent! (reduce (fn
                           ([result] result)
                           ([result item]
                             (let [-pair (vswap! pair conj item)]
                               (if (= 2 (count -pair))
                                 (do
                                   (vreset! pair [])
                                   (assoc! result (nth -pair 0) (nth -pair 1)))
                                 result)))) (transient {}) a-seq))))

(defn create-countdown-trigger
  "Creates a countdown trigger that runs in timeout-ms unless a value is put onto the trigger-control channel,
   which resets the timeout"
  [timeout-ms ^IFn f ^APersistentVector params]
  (let [trigger-control (chan (dropping-buffer 1))]
    (go (loop [timeout-ch (timeout timeout-ms)]
          (let [[value _] (alts! [timeout-ch trigger-control])]
            (if (= nil value)
              ;time to trigger the trigger
              (apply f params)
              ;otherwise reset the trigger
              (recur (timeout timeout-ms))
              ))))
    trigger-control))

(defn recur-circuit-breaker []
  (loop [num-of-recurs 0]
    (if (< num-of-recurs 3)
      (do (println-m "trying")
          (recur (+ 1 num-of-recurs)))
      false)))


(def redef-signal (chan 1))

(defn redef-loop
  "A way to redefine a running loop"
  []
  (thread (loop []
            (let [[value _] (alts!! [redef-signal] :default :no-redef)]
              (if (= true value)
                ;time to redef the loop
                (redef-loop)
                ;normal execution
                (do (<!! (timeout 3000))
                    (println-m "456-new-2")
                    (recur)))))))

(defn <!!-till-timeout [ch ^long timeout-ms]
  (let [[value _] (alts!! [ch (timeout timeout-ms)])] value))

(defn <!!-till-exception
  "Experimental, for development"
  [ch]
  (let [[value _] (alts!! [ch (timeout 3000)])]
    (if (= nil value)
      (throw (Exception. (str "waited " 3000 " ms")))
      ;else - ok
      value)))

(defmacro chan-and-print
  "Helper macro for the repl
  Takes chan-name, and optional buff-n and xf, creates a chan and starts printing out of it"
  [chan-name & more]
  (let [[buff-n xf] more]
    `(do (def ~chan-name (chan (if ~buff-n ~buff-n 1) ~xf))
         (go (while true (println (str '~chan-name) "::" (<! ~chan-name)))))))

;Y combinator
(defn sum-seq-fn-gen [f]
  (fn [s]
    (if (empty? s)
      0
      (+ (first s) (f (rest s))))))

(defn fact-gen [fact-in]
  (fn [n]
    (if (= n 0)
      1
      (* n (fact-in (- n 1))))))

(defn Y
  "Y combinator"
  [r]
  ((fn [f]
     (f f))
    (fn [f]
      (r (fn [arg-state]
           ((f f) arg-state))))))


(defn transform-into-self
  "Takes collection coll and returns collection of the same type,
   subject to transducer xf"
  [xf coll]
  (into (empty coll) xf coll))

(defn byte-array-n-kb
  "Creates a dummy byte-array"
  [n]
  (byte-array (repeatedly (* 1000 n) #(byte (- ^long (rand-int 256) 128)))))




(defn encode [data]
  (cheshire/encode data {:escape-non-ascii true}))





