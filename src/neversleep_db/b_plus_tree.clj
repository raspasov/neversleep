(ns neversleep-db.b-plus-tree
  (:require [neversleep-db.b-plus-tree-protocol :refer :all]
            [neversleep-db.println-m :refer [println-m]]
            [clojure.core.rrb-vector :as rrbv]
            [neversleep-db.util :as util]
            [clojure.core.async :refer [chan go >! <! <!! >!! go-loop put! thread alts! alts!! timeout pipeline pipeline-blocking pipeline-async]]
            [criterium.core :as criterium :refer [quick-bench]])
  (:import (clojure.core.rrb_vector.rrbt Vector)
           (clojure.lang APersistentVector IFn)))



(declare
  create-leaf-node
  create-inner-node
  empty-inner-node
  empty-leaf-node
  get-root
  add-op
  execute-ops)

;{:bpt-id-1 root-1, :bpt-id-2 root-2}
(def all-b-plus-trees (ref {}))

(defn get-b-plus-tree [id]
  (get @all-b-plus-trees id))


(defn save-b-plus-tree [id b-plus-tree]
  (alter all-b-plus-trees (fn [x] (assoc x id b-plus-tree))))

(def max-degree (int 127))


(deftype LeafNode [parent array left-pointer right-pointer]
  DurableBPlusTreeNode

  ;example structure: [key val key val]
  (get-array [_] @array)

  (get-parent [_] @parent)

  (set-parent! [this new-parent]
    (reset! parent new-parent)
    this)

  (add-to-array! [this key val]
    ;TODO determine place in the array for the new key/val
    (let [last-key (get array (- (count @array) 2))]
      (if (not (nil? last-key))
        (cond (<= ^long last-key ^long key)
              (dosync (alter array conj key val))
              :else "TODO: walk array and determine insertion indexes for key/val"
              )
        ;else, array is empty add to it
        (dosync (alter array conj key val))))
    this)

  (set-array! [this new-array]
    (ref-set array new-array)
    this)

  (set-right-pointer! [this new-right-pointer]
    (ref-set right-pointer new-right-pointer)
    this)

  (get-left-pointer [this] @left-pointer)

  (get-right-pointer [this] @right-pointer)

  (n-assoc [this key val ops-collector]

    ;TODO verify this with tests
    (if (< (unchecked-divide-int (count @array) 2) ^int (unchecked-subtract-int max-degree 1))
      ;yes, there is space, regular add
      (add-to-array! this key val)
      ;no, there is no space, split the leaf
      ;right-leaf-array gets one-more-than-the-middle items to put into the new right leaf
      (do
        ;add to array and then split - always results in an even (count @array), aka odd number of keys in leaf
        (add-to-array! this key val)
        ;do the split
        (let [middle-point (unchecked-divide-int (unchecked-dec-int (count @array)) 2)
              right-leaf-array (rrbv/subvec @array middle-point)
              left-leaf-array (rrbv/subvec @array 0 middle-point)
              new-right-leaf-node (create-leaf-node (get-parent this) right-leaf-array this nil)
              key-to-push-up (nth right-leaf-array 0)       ;push up the first key from the new right leaf
              new-or-old-parent (if (= nil @parent)
                                  ;create new root/inner-node
                                  (create-inner-node (get-parent this) (rrbv/vec [this key-to-push-up new-right-leaf-node]))
                                  ;otherwise push key up the tree
                                  (n-push-up (get-parent this) key-to-push-up new-right-leaf-node ops-collector))
              ;modify this
              ;_ (set-array! this left-leaf-array)
              _ (add-op ops-collector set-array! [this left-leaf-array])
              _ (add-op ops-collector set-right-pointer! [this new-right-leaf-node])]
          ;return the root by walking up the tree
          new-or-old-parent))))

  (n-find [this key not-found responce-ch]
    (let [array (get-array this)
          ;_ (println-m "searching node with array:: " array)
          array-count (count array)]
      (loop [idx 0]
        (if (< ^long (- array-count 1) idx)
          (>!! responce-ch {:result not-found}) ;not-found
          (let [node-key (get array idx)]                   ;else do the search
            (if (= ^long key ^long node-key)
              (>!! responce-ch {:result {(str node-key) (get array (unchecked-add-int idx 1))}})
              (recur (unchecked-add-int idx 2))))))))



  (n-find-range-forward [this key-start key-end limit not-found responce-ch results]
    ;deref the array
    (let [array (get-array this)
          right-pointer (get-right-pointer this)
          array-count (count array)]
      (loop [idx 0
             results results]
        (let [current-key (get array idx)]
          (if (< ^long (- array-count 1) idx)
            ;if at the end of the array...
            (cond (or (= (count results) ^long limit) (= nil right-pointer))
                  ;at the end of array with enough results or nil right-pointer - return!
                  (>!! responce-ch {:result results})
                  :else
                  ;continue search on right-pointer
                  (n-find-range-forward right-pointer key-start key-end limit not-found responce-ch results))
            ;not at the end of array...
            (cond (or (= (count results) limit) (< ^long key-end ^long current-key))
                  ;either collected enough items or already beyond the end of valid results - return!
                  (>!! responce-ch {:result results})

                  (<= ^long key-start ^long current-key)
                  ;else - found a valid item, add it to results and recur
                  (recur (+ idx 2) (conj results [(str current-key) (get array (+ idx 1))]))

                  (< ^long current-key ^long key-start)
                  ;else - current key is still smaller than the start, continue searching this node without adding to results
                  (recur (+ idx 2) results)))))))

  (n-find-range-backward [this key-start key-end limit not-found responce-ch results]
    (let [array (get-array this)
          left-pointer (get-left-pointer this)
          array-count (count array)]
      (loop [idx (- array-count 2)
             results results]
        (let [current-key (get array idx)]
          (if (< idx 0)
            ;if at the start of the array...
            (cond (or (= (count results) limit) (= nil left-pointer))
                  ;at the end of array with enough results or nil left-pointer - return!
                  (>!! responce-ch {:result results})
                  :else
                  ;continue search on left-pointer
                  (n-find-range-backward left-pointer key-start key-end limit not-found responce-ch results))
            ;not at the end of array...
            (cond (or (= (count results) limit) (< ^long current-key ^long key-end))
                  ;either collected enough items or already beyond the end of valid results - return!
                  (>!! responce-ch {:result results})

                  (<= ^long current-key ^long key-start)
                  ;else - found a valid item, add it to results and recur
                  (recur (- idx 2) (conj results [(str current-key) (get array (+ idx 1))]))

                  (< ^long key-start ^long current-key)
                  ;else - current key is still bigger than the start, continue searching this node without adding to results
                  (recur (- idx 2) results))))))))


(deftype InnerNode [parent array]
  DurableBPlusTreeNode

  (get-parent [_] @parent)

  (set-parent! [this new-parent]
    (reset! parent new-parent)
    this)

  ;example structure: ;["pointer-1" "key-1" "pointer-2" "key-2" "pointer-3" "key-3" "pointer-4"]
  (get-array [_] @array)

  (set-array! [this new-array]
    (ref-set array new-array)
    this)

  (add-to-array! [this key pointer]
    (let [last-key (nth @array (- (count @array) 2))]
      (cond
        ;first option: bigger than last item in the array, add to the end
        (<= ^long last-key ^long key)
        ;via atoms (swap! array conj key pointer)
        (dosync (alter array conj key pointer))
        ;walk the array
        :else (throw (Exception. "TODO: walk array and determine the next deeper level pointer to go to"))
        )
      this))

  (n-push-up [this key pointer ops-collector]
    ;TODO probably correct - verify
    (if (< (unchecked-divide-int (count @array) 2) ^int (unchecked-subtract-int max-degree (int 1)) )
      ;yes, there's space, add key/pointer
      (add-to-array! this key pointer)
      ;no, there's no space - split
      (do (add-to-array! this key pointer)
          ;split the inner node's array
          (let [middle-point (unchecked-divide-int (unchecked-dec-int (count @array)) 2)
                right-inner-array (rrbv/subvec @array (+ 1 middle-point))
                left-inner-array (rrbv/subvec @array 0 middle-point)
                new-right-inner-node (create-inner-node (get-parent this) right-inner-array)
                key-to-push-up (nth @array middle-point)
                new-or-old-parent (if (= nil @parent)
                                    ;create new root
                                    (create-inner-node (get-parent this) (rrbv/vec [this key-to-push-up new-right-inner-node]))
                                    ;otherwise push key up the tree
                                    (n-push-up (get-parent this) key-to-push-up new-right-inner-node ops-collector))
                ;modify this
                _ (add-op ops-collector set-array! [this left-inner-array])
                ]
            ;TODO read here - CONCURENCY BUG VERY LIKELY
            ;return the root by walking up the tree
            new-or-old-parent))))

  (n-assoc [this key val ops-collector]
    (let [last-key (nth @array (- (count @array) 2))
          last-pointer (nth @array (- (count @array) 1))]
      (cond
        ;first option: bigger than last item in the array, proceed on last-pointer
        (<= ^long last-key ^long key)
        (n-assoc last-pointer key val ops-collector)
        ;walk the array
        :else (throw (Exception. "TODO: walk array and determine the next deeper level pointer to go to"))
        )))


  (n-find [this key not-found responce-ch]

    (let [array (get-array this)
          ;_ (println-m "searching node with array:: " array)
          array-count (count array)
          first-key (get array 1)
          last-key (get array (- array-count 2))]
      (cond (< ^long key ^long first-key)                               ;if search key < first-key, then n-find on the first branch of current node
            (n-find (get array 0) key not-found responce-ch)
            (<= ^long last-key ^long key)                               ;if search key >= last-key, then n-find on the last branch of current node
            (n-find (get array (- array-count 1)) key not-found responce-ch)
            :else                                           ;else, find the first key-at-idx >= key, and n-find on the i+1 branch of current node
            (loop [idx 1]
              (let [key-at-idx (get array idx)]
                (if (< ^long key ^long key-at-idx)
                  (n-find (get array (- idx 1)) key not-found responce-ch)
                  (recur (+ idx 2))))))))

  (n-find-range-forward [this key-start key-end limit not-found responce-ch results]
    (let [array (get-array this)
          array-count (count array)
          first-key (get array 1)
          last-key (get array (- array-count 2))]
      (cond (< ^long key-start ^long first-key)                               ;if search key < first-key, then n-find on the first branch of current node
            (n-find-range-forward (get array 0) key-start key-end limit not-found responce-ch results)
            (<= ^long last-key ^long key-start)                               ;if search key >= last-key, then n-find on the last branch of current node
            (n-find-range-forward (get array (- array-count 1)) key-start key-end limit not-found responce-ch results)
            :else                                           ;else, find the first key-at-idx >= key, and n-find on the i+1 branch of current node
            (loop [idx 1]
              (let [key-at-idx (get array idx)]
                (if (< ^long key-start ^long key-at-idx)
                  (n-find-range-forward (get array (- idx 1)) key-start key-end limit not-found responce-ch results)
                  (recur (+ idx 2))))))))

  (n-find-range-backward [this key-start key-end limit not-found responce-ch results]
    (let [array (get-array this)
          array-count (count array)
          first-key (get array 1)
          last-key (get array (- array-count 2))]
      (cond (< ^long key-start ^long first-key)                               ;if search key < first-key, then n-find on the first branch of current node
            (n-find-range-backward (get array 0) key-start key-end limit not-found responce-ch results)
            (<= ^long last-key ^long key-start)                               ;if search key >= last-key, then n-find on the last branch of current node
            (n-find-range-backward (get array (- array-count 1)) key-start key-end limit not-found responce-ch results)
            :else                                           ;else, find the first key-at-idx >= key, and n-find on the i+1 branch of current node
            (loop [idx 1]
              (let [key-at-idx (get array idx)]
                (if (< ^long key-start ^long key-at-idx)
                  (n-find-range-backward (get array (- idx 1)) key-start key-end limit not-found responce-ch results)
                  (recur (+ idx 2))))))))

  )

;leaf node constructors
(defn create-leaf-node [parent array left-pointer right-pointer]
  ;{:pre [(instance? Vector array)]}
  (let [new-node (LeafNode. (atom parent) (ref array) (ref left-pointer) (ref right-pointer))]
    ;set the parents
    (doseq [item array :when (instance? (:on-interface DurableBPlusTreeNode) item)]
      (set-parent! item new-node))
    new-node))

(defn empty-leaf-node []
  (create-leaf-node nil (rrbv/vector) nil nil))
;end leaf node constructors

;inner node constructors
(defn create-inner-node [parent array]
  ;{:pre [(instance? Vector array)]}
  (let [new-node (InnerNode. (atom parent) (ref array))]
    ;set the parents
    (doseq [item array :when (instance? (:on-interface DurableBPlusTreeNode) item)]
      (set-parent! item new-node))
    new-node))

(defn get-root
  "Walks up the tree to find the root"
  [node]
  (loop [current-node node]
    (if (= nil (get-parent current-node))
      current-node
      (recur (get-parent current-node)))))

(defn empty-inner-node []
  (create-inner-node nil (rrbv/vector)))

(defn b-tree [cnt root]
  {:cnt cnt :root root})

(defn new-b-tree []
  (b-tree 0 (empty-leaf-node)))


(defn new-ops-collector
  "Create a new op collector (called at each io-assoc)
   The collector is a vector of vectors, each internal vector
   if a tuple of f and vector-of-params, i.e. [f vector-of-params].
   Later on we execute all the ops in a single transaction via (execute-ops collector)
   Example structure: [[f [param1 param2 etc]] etc ] "
  []
  (atom []))

(defn add-op
  "Schedule an op to be executed in a transaction"
  [ops-collector f params]
  (swap! ops-collector conj [f params]))

(defn execute-ops
  "Execute all accumulated ops in a single STM transaction"
  [ops-collector]
  (dosync (doseq [[^IFn f ^APersistentVector params] @ops-collector] (apply f params))))

(defn io-assoc
  "Uses Clojure STM to ensure all mutations with a recursion happen together with the root change"
  [id key val confirm-ch]

  (let [{:keys [root] :as tree-current} (if-let [tree-current (get-b-plus-tree id)]
                                          ;tree exists, return it
                                          tree-current
                                          ;initiate a new tree
                                          (new-b-tree))
        ops-collector (new-ops-collector)
        new-root (-> (n-assoc root key val ops-collector)
                     (get-root))]

    ;(println-m @ops-collector)
    ;add root mutation to the ops
    (add-op ops-collector save-b-plus-tree [id (b-tree 0 new-root)])
    ;execute all mutations via Clojure's STM
    (execute-ops ops-collector)
    (>!! confirm-ch true)))

(defn io-find
  "Searches the b-plus tree for a key"
  [id key responce-ch]
  (if-let [tree-current (get-b-plus-tree id)]
    ;search the tree
    (dosync (n-find (-> tree-current :root) key nil responce-ch))
    ;tree doesn't exist
    nil))

(defn io-find-range
  "Searches the b-plus tree for a range of keys.
   If key-start > key-end, search is DESC-like
   If key-start <= key-end, search is ASC-like"
  [id key-start key-end limit responce-ch]
  (if-let [tree-current (get-b-plus-tree id)]
    ;search the tree
    (let [n-find-range-direction (if (<= ^long key-start ^long key-end) n-find-range-forward n-find-range-backward)]
      ;determine search direction
      (dosync (n-find-range-direction (-> tree-current :root) key-start key-end limit nil responce-ch [])))
    ;tree doesn't exist
    nil))

(defn test-insert-key [id]
  (io-assoc "users" id (str id "-val") (chan 1))
  (-> (:root (get @all-b-plus-trees "users"))
      (get-array)))

;how to make the tree correctly concurrent:
; - never actually add to a node that we're about to split - it temporarily creates inconsistent state
; - adjust the code to compensate for not actually adding the array (determining the index where to split will have to change + or - one)
; - do all the (add-to-array!) non-splitting calls within the op-collector transaction as well
; - do we have consistent tree at all times in this case?


;b-plus-tree on disk
; 1. bulk load into the tree
; 2. when a BulkLeafNode is full in memory, merge into disk
; 3. add a pointer to the InnerNode above to the new BulkLeafNode
; 4. when InnerNode is full, split it

;;test perf
;(time (let [op-1 (future (dotimes [i 100000] (io-find "users" (rand-int 100000) r-c)))
;            op-2 (future (dotimes [i 100000] (io-find "users" (rand-int 100000) r-c)))
;            op-3 (future (dotimes [i 100000] (io-find "users" (rand-int 100000) r-c)))]
;        [@op-1 @op-2 @op-3]))
;
;(time (let [op-1 (future (dotimes [i 100000] (io-find-range "users" (rand-int 100000) 0 1 r-c)))
;            op-2 (future (dotimes [i 100000] (io-find-range "users" (rand-int 100000) 0 1 r-c)))
;            op-3 (future (dotimes [i 100000] (io-find-range "users" (rand-int 100000) 0 1 r-c)))]
;        [@op-1 @op-2 @op-3]))

;bulk insertion:
; - walk down the tree till an InnerNode with LeafNodes is found
; - if there's space in the InnerNode, insert the BulkLeafNode - done!
; - if there isn't space in the InnerNode, create a new InnerNode and insert the BulkLeafNode there