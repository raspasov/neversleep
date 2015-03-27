(ns neversleep-db.persistent-durable-vector
  (:require [neversleep-db.util :as util]
            [neversleep-db.println-m :refer [println-m]]
            [neversleep-db.to-disk :as to-disk]
            [aprint.core :as aprint]))

;
;(defn mask
;  "Binary magic. Returns the integer index for the specified hash/shift combination"
;  [^Integer hash ^Integer shift]
;  (-> hash
;      (unsigned-bit-shift-right shift)
;      (bit-and 0x01f)))
;
;(defn bitpos
;  "Maps hash/shift combination to a number that
;  has binary representation in the form 10^n, n>=0"
;  [^Integer hash ^Integer shift]
;  (bit-shift-left 1 (mask hash shift)))
;
;(defn index
;  "Returns count of 0 or 1 depending on whether index exists"
;  ;TODO figure out how to get bitmap
;  [^Integer bitmap ^Integer bitpos]
;  (Integer/bitCount (bit-and bitmap (- bitpos 1))))
;
;(defn sub-index
;  "For any count/shift combination, returns the correct
;  index for that level (0-31)"
;  [cnt shift]
;  (bit-and (unsigned-bit-shift-right (- cnt 1) shift) 0x01f))
;
;;NOTES
;;bitmap gets updated on every copy with bit-or operation
;
;;map that stores vector states (versions) like this: {:vector-id [v1 v2 ...]}, v1, v2 being a root node
;;THINK OF THIS AS DURABLE
;;TODO how do we sort this efficiently so we have timestamp lookup of version? in-memory sort and periodically merge onto disk?
;;TODO maybe max heap data structure for each [v1 v2 ...]?
;(def all-vectors (atom {}))
;
;;map that looks like this {:uuid node}
;;THINK OF THIS AS DURABLE
;(def all-nodes (atom {}))
;
;(defn save-node! [node]
;  ;(swap! all-nodes assoc (-> node :uuid) node)
;  (to-disk/save-node (-> node :uuid) node)
;  )
;
;(defn get-node! [uuid]
;  ;(get @all-nodes uuid nil)
;  (to-disk/get-node uuid)
;  )
;
;
;(defn get-vector-current! [^String id]
;  ;TODO check durable storage
;  ;(-> ((keyword id) @all-vectors)
;  ;    peek)
;  (to-disk/get-root id)
;  )
;
;
;(defn tail-off
;  "Returns a Long of how much to 'cut off' from the tail when determining
;  if there's room in the tail"
;  [cnt]
;  (if (< cnt 32)
;    0
;    (-> (unsigned-bit-shift-right (- cnt 1) 5)
;        (bit-shift-left 5))))
;
;(defn persistent-durable-vector [cnt shift root tail]
;  {:cnt   cnt
;   :shift shift
;   :root  root
;   :tail  tail})
;
;(defn new-node! [array]
;  {:pre [(do                                                ;(println-m "pre:" array)
;                                         (vector? array)
;           )]}
;  ;TODO store durably only uuids instead of the whole array?
;  (let [uuid (util/uuid)
;        n {:array array
;           :uuid  uuid}]
;    ;TODO make this durable
;    (save-node! n)
;    (assoc n :array :io)
;    ))
;
;;BASIC FUNCTIONALITY
;;pdv/io-conj!
;;pdv/io-nth!
;;pdv/as-of!
;
;
;(defn save-vector! [id io-vector]
;  ;(swap! all-vectors (fn [a]
;  ;                     (let [vector-keyword (keyword id)
;  ;                           many-vectors (vector-keyword a)]
;  ;                       (assoc a vector-keyword (into [] (conj many-vectors io-vector))))))
;  (to-disk/save-root id io-vector)
;  )
;
;(defn new-path [shift node]
;  (if (= 0 shift)
;    node
;    (new-node! [(new-path (- shift 5) node)])))
;
;(defn push-tail [io-vector shift parent-node tail-node]
;  (let [sub-index (sub-index (:cnt io-vector) shift)
;        ;resolve the array-parent-node dynamically via io
;        array-parent-node (:array (get-node! (:uuid parent-node)) [])
;        ;_ (println-m "array-parent-node: " array-parent-node)
;        ;_ (println-m "sub-index:" sub-index)
;        ]
;    (let [node-to-insert (if (= 5 shift)
;                           ;First level special case
;                           (do                              ;(println-m "first level special case")
;                               ;(new-node! (assoc array-parent-node sub-index tail-node))
;                               tail-node
;                               )
;                           ;All other levels
;                           (do                              ;(println-m "all other levels, level:" shift)
;                               (let [node-child (nth array-parent-node sub-index nil)
;                                     ;_ (println-m "node-child: " node-child)
;                                     ]
;                                 (if (not (nil? node-child))
;                                   (do                      ;(println-m "all other levels, level:" shift " , push-tail")
;                                       (push-tail io-vector (- shift 5) node-child tail-node))
;                                   (do                      ;(println-m "all other levels, level:" shift " , new-path")
;                                       (new-path (- shift 5) tail-node)) )))
;                           )]
;      (new-node! (assoc array-parent-node sub-index node-to-insert)))
;    ))
;
;(defn io-conj! [^String id val]
;  (let [{:keys [cnt tail shift root] :as io-vector} (get-vector-current! id)]
;    (cond
;      ;room in tail, put into tail
;      (< (- cnt (tail-off cnt)) 32) (do                     ;(println-m "room in tail. put into tail")
;                                        (save-vector! id (persistent-durable-vector (+ 1 cnt) shift root (conj tail val))))
;      ;full tail, going to push into tree
;      ;overflowing root?
;      (> (unsigned-bit-shift-right cnt 5) (bit-shift-left 1 shift)) (do
;                                                                      ;(println-m "OVERFLOWING ROOT")
;                                                                      (let [new-root (new-node! [root (new-path shift (new-node! tail))])]
;                                                                        (save-vector! id (persistent-durable-vector (+ 1 cnt) (+ shift 5) new-root [val]))))
;      :else (do
;              ;(println-m "push tail")
;              (let [new-root (push-tail io-vector shift root (new-node! tail))]
;                (save-vector! id (persistent-durable-vector (+ 1 cnt) shift new-root [val]))))
;      )))
;
;
;(defn io-get-leaf-node-for-index [i shift node]
;  "Digs into the nodes to get to the leaf based on the index we are looking for.  Separated from array-for for simplicity reasons."
;  (let [index-in-node-array (mask i shift)                              ;Get the 0-31 index of the node at the shift we are currently at
;        next-node (get-node! (:uuid (nth (-> node :array) index-in-node-array)))]
;    (if (= 0 (- shift 5))
;      (do (:array next-node))                                            ;Leaf -> Return this nodes array since we are at the leaf
;      (do (recur i (- shift 5) next-node)))                              ;Not Leaf -> Recur with new node and shift - 5
;    ))
;
;(defn io-array-for
;  "Returns the leaf OR tail array which the value at index i exisits"
;  [durable-vector i]
;  (let [cnt (:cnt durable-vector)
;        tail (:tail durable-vector)]
;    (if (>= i (tail-off cnt))                               ;check if the index is in the current tail
;      (do tail)                                             ;The index is in the tail, so return it
;      (do (io-get-leaf-node-for-index i (:shift durable-vector) (get-node! (-> durable-vector :root :uuid))))) ;Find the leaf node where this index lives starting from the vector's root
;    ))
;
;;FORWARD DECLARATION OF FN
;(declare current-state!)
;
;(defn io-nth!
;  "Returns the index i in vector with identifier id.  Returns nil if out of bounds"
;  ([id i]
;   (let [durable-vector (current-state! id)
;         cnt (:cnt durable-vector)]
;     (if (and (>= i 0) (< i cnt))                                   ;Make sure the index is not out of bounds to prevent wasted func call
;       (do (nth (io-array-for durable-vector i) (bit-and i 0x01f))) ;Find the leaf and return the value at the bit-shifted index (last 5 bits)
;       (do nil))                                                    ;Out of bounds no point to go further, return nil
;     )))
;
;(defn new-io-vector! [id]
;  (let [v (persistent-durable-vector 0 5 (new-node! []) [])]
;    (save-vector! id v)
;    ;TODO make v durable
;    v))
;
;(defn io-count! [id]
;  ;(-> @all-vectors
;  ;    ((keyword id))
;  ;    peek
;  ;    :cnt)
;
;  (-> (to-disk/get-root id)
;      :cnt)
;  )
;
;
;(defn current-state! [id]
;  ;(-> @all-vectors
;  ;    ((keyword id))
;  ;    peek)
;  (-> (to-disk/get-root id))
;  )
;
;(defn all-states! [id]
;  (-> @all-vectors
;      ((keyword id))
;      (clojure.pprint/pprint)))
;
;
;
;(defn as-of! [id timestamp])
;
;;TESTS
;;(new-io-vector! "users")
;;(dotimes [i 3000] (io-conj! "users" i))
;
;;TODO
;;save in Cassandra
;;disk + memory sort merge - how?