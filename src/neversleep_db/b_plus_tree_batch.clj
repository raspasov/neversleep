(ns neversleep-db.b-plus-tree-batch
  "An implementation of batch b-plus tree. Allows time-indexing of all entities."
  (require [neversleep-db.b-plus-tree-protocol :refer :all]
           [neversleep-db.println-m :refer [println-m]]
           [flatland.protobuf.core :refer :all]
           [neversleep-db.blob-unpacker :as blob-unpacker]
           [clojure.core.async :refer [chan go >! <! <!! >!! go-loop put! thread alts! alts!! timeout pipeline pipeline-blocking pipeline-async]]
           [clojure.core.rrb-vector :as rrbv]
           [neversleep-db.util :as util]
           [neversleep-db.state :as state]
           [neversleep-db.exception-log :as exception-log]
           [neversleep-db.serializer :as serializer])
  (:import (clojure.lang Atom APersistentMap)
           (com.google.protobuf ByteString)
           com.durablenode.Durablenode$BtreeLeafNode
           com.durablenode.Durablenode$BtreeInnerNode))


;protobufs
(def LeafNodeProtoDef (protodef Durablenode$BtreeLeafNode))
(def InnerNodeProtoDef (protodef Durablenode$BtreeInnerNode))


(declare create-inner-node
         create-leaf-node
         inner-node-array->repeatable-bytes
         leaf-node-array->repeatable-bytes
         node->node-tuple-byte-array
         n-assoc-bulk-dispatcher
         n-find-range-backward-dispatcher)


(defn get-b-plus-tree [id]
  (get @state/all-b-plus-trees-on-disk id))

(defn save-b-plus-tree [id b-plus-tree]
  (swap! state/all-b-plus-trees-on-disk (fn [x] (assoc x id b-plus-tree))))
;end

;tree config
(def max-degree (int 127))
;end

;deftypes
;======================================================================================================================
(deftype InnerNode [array uuid blob-identifier]
  DurableBPlusTreeNode

  (to-remote-pointer [this] (node->node-tuple-byte-array this))

  (to-protobuf [this] (protobuf InnerNodeProtoDef
                                :array (inner-node-array->repeatable-bytes array)
                                :uuid uuid))
  (get-blob-id [this] blob-identifier)

  (get-node-id [this] uuid)

  (get-node-type [this] 2)

  (get-array [this] array)

  (path-copy-on-write [this new-rightmost-pointer parent-pointers node-collector blob-id entity-id]
    ;TODO GC this
    (let [parent (peek parent-pointers)
          new-inner-node (create-inner-node (assoc array (- (count array) 1) new-rightmost-pointer) node-collector blob-id entity-id)]
      (if (= nil parent)
        new-inner-node
        (path-copy-on-write parent new-inner-node (pop parent-pointers) node-collector blob-id entity-id))))

  (n-push-up-bulk [this vector-of-keys-and-pointers parent-pointers node-collector blob-id entity-id]
    ;determine if we're pushing from a LeafNode (2) or InnerNode (3)
    (let [array (if (= 3 (count vector-of-keys-and-pointers)) (pop array) array)
          parent (peek parent-pointers)]
      (if (< (unchecked-divide-int (count array) 2) (unchecked-subtract-int max-degree 1))
        ;yes, there is space - copy-on-write this node
        (do
            (let [new-inner-node (create-inner-node (rrbv/vec (apply conj array vector-of-keys-and-pointers)) node-collector blob-id entity-id)]
              (if (= nil parent)
                new-inner-node
                ;copy-on-write the whole path until parent is nil
                (path-copy-on-write parent new-inner-node (pop parent-pointers) node-collector blob-id entity-id)))
            ;ends recursion?
            )
        ;else, full - create a brand new inner node, also copy on write this node with fewer keys left in it
        (let [array (cond
                      ;first option: bigger than last item in the array, add to the end
                      ;(<= ^long last-key ^long pushed-up-key)
                      (= true true)
                      ;(conj array pushed-up-key pushed-up-pointer)
                      ;replace last pointer in the array with first element from vector-of-keys-and-pointers
                      ;if pushed from InnerNode
                      (apply conj array vector-of-keys-and-pointers)
                      ;walk the array
                      :else (throw (Exception. "TODO: walk array and determine the next deeper level pointer to go to")))
              middle-point (unchecked-divide-int (unchecked-dec-int (count array)) 2)
              right-inner-array (rrbv/subvec array (+ 1 middle-point))
              left-inner-array (rrbv/subvec array 0 middle-point)
              ;TODO GC this
              new-left-inner-node (create-inner-node (rrbv/vec left-inner-array) node-collector blob-id entity-id)
              new-right-inner-node (create-inner-node (rrbv/vec right-inner-array) node-collector blob-id entity-id)
              key-to-push-up (nth array middle-point)
              new-parent (if (= nil parent)
                           ;create new root (ends recursion?)
                           (create-inner-node (rrbv/vec [new-left-inner-node key-to-push-up new-right-inner-node]) node-collector blob-id entity-id)
                           ;otherwise continue push up the tree
                           (n-push-up-bulk parent [new-left-inner-node key-to-push-up new-right-inner-node] (pop parent-pointers) node-collector blob-id entity-id))]
          new-parent))))

  (n-assoc-bulk [this key-vals parent-pointers node-collector blob-id entity-id]
    (let [last-key (nth array (- (count array) 2))
          last-pointer (nth array (- (count array) 1))
          key-vals-first-key (nth key-vals 0)]
      (cond
        ;first option: bigger than last item in the array, proceed on last-pointer
        (<= ^long last-key ^long key-vals-first-key)
        ;add this to parent-pointers vector, continue the recursion down
        (n-assoc-bulk-dispatcher last-pointer key-vals (conj parent-pointers this) node-collector blob-id entity-id)
        :else (throw (Exception. (str "writing to the middle of the b-tree tree not supported, last-key " last-key " key-vals-first-key " key-vals-first-key))))))

  (n-find [this key not-found responce-ch]
    (let [array (get-array this)
          ;_ (println-m "searching node with array:: " array)
          array-count (count array)
          first-key (get array 1)
          last-key (get array (- array-count 2))]
      (cond (< ^long key ^long first-key)
            ;if search key < first-key, then n-find on the first branch of current node
            (n-find (get array 0) key not-found responce-ch)
            (<= ^long last-key ^long key)
            ;if search key >= last-key, then n-find on the last branch of current node
            (n-find (get array (- array-count 1)) key not-found responce-ch)
            :else
            ;else, find the key in the middle
            (loop [idx 1]
              (let [key-at-idx (get array idx)]
                (if (< ^long key ^long key-at-idx)
                  (n-find (get array (- idx 1)) key not-found responce-ch)
                  (recur (+ idx 2))))))))

  (n-find-range-backward [this key-start key-end limit not-found responce-ch results entity-id]
    (let [array (get-array this)
          array-count (count array)
          first-key (get array 1)
          last-key (get array (- array-count 2))]
      (cond (< ^long key-start ^long first-key)
            ;if search key < first-key, then n-find on the first branch of current node
            (n-find-range-backward-dispatcher (get array 0) key-start key-end limit not-found responce-ch results entity-id)
            (<= ^long last-key ^long key-start)
            ;if search key >= last-key, then n-find on the last branch of current node
            (n-find-range-backward-dispatcher (get array (- array-count 1)) key-start key-end limit not-found responce-ch results entity-id)
            :else
            ;else, find the the key in the middle
            (loop [idx 1]
              (let [key-at-idx (get array idx)]
                (if (< ^long key-start ^long key-at-idx)
                  (n-find-range-backward-dispatcher (get array (- idx 1)) key-start key-end limit not-found responce-ch results entity-id)
                  (recur (+ idx 2)))))))))

(deftype LeafNode [array left-pointer uuid blob-identifier]
  DurableBPlusTreeNode

  (to-remote-pointer [this] (node->node-tuple-byte-array this))

  (to-protobuf [_] (if left-pointer
                     (protobuf LeafNodeProtoDef
                               :array (leaf-node-array->repeatable-bytes array)
                               :uuid uuid
                               :leftpointer (util/copy-to-byte-string (node->node-tuple-byte-array left-pointer)))
                     (protobuf LeafNodeProtoDef
                               :array (leaf-node-array->repeatable-bytes array)
                               :uuid uuid)))

  (get-blob-id [_] blob-identifier)

  (get-node-id [_] uuid)

  (get-node-type [_] 1)

  (get-array [_] array)

  (get-left-pointer [_] left-pointer)

  (n-assoc-bulk [this key-vals parent-pointers node-collector blob-id entity-id]
    ;CREATE A NEW LEAF FROM key-vals, with a leftpointer to this
    (let [new-leaf-node (create-leaf-node key-vals this node-collector blob-id entity-id)
          key-vals-first-key (nth key-vals 0)]
      ;PUSH the (nth key-vals 0) up to parent
      (if (= (count parent-pointers) 0)
        ;parent is nil, create a new root
        (create-inner-node (rrbv/vec [this key-vals-first-key new-leaf-node]) node-collector blob-id entity-id)
        ;else, push up
        (n-push-up-bulk (peek parent-pointers) [key-vals-first-key new-leaf-node] (pop parent-pointers) node-collector blob-id entity-id))))

  (n-find [this key not-found responce-ch]
    (let [array (get-array this)
          ;_ (println-m "searching node with array:: " array)
          array-count (count array)]
      (loop [idx 0]
        (if (< ^long (- array-count 1) idx)
          ;not-found
          (>!! responce-ch {:result not-found})
          ;else do the search
          (let [node-key (get array idx)]
            (if (= ^long key ^long node-key)
              (>!! responce-ch {:result {node-key (get array (+ idx 1))}})
              (recur (+ idx 2))))))))

  (n-find-range-backward [this key-start key-end limit not-found responce-ch results entity-id]
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
                  (n-find-range-backward-dispatcher left-pointer key-start key-end limit not-found responce-ch results entity-id))
            ;not at the end of array...
            (cond (or (= (count results) limit) (< ^long current-key ^long key-end))
                  ;either collected enough items or already beyond the end of valid results - return!
                  (>!! responce-ch {:result results})

                  (<= ^long current-key ^long key-start)
                  ;else - found a valid item, add it to results and recur
                  (recur (- idx 2) (conj results [current-key (get array (+ idx 1))]))

                  (< ^long key-start ^long current-key)
                  ;else - current key is still bigger than the start, continue searching this node without adding to results
                  (recur (- idx 2) results))))))))
;end deftypes


;node collector
;======================================================================================================================
;data looks like this [{uuid leaf-node, uuid leaf-node} {uuid inner-node, uuid inner-node}]
(defn- new-node-collector []
  (atom [{} {}]))

(defn- get-node-collector-idx
  "Based on node's type (1 - LeafNode, 2 - InnerNode),
   returns the corresponding index in the node-collector"
  [node]
  (- ^long (get-node-type node) 1))

(defn add-to-node-collector [node-collector node entity-id]
  ;add to cache
  (blob-unpacker/add-to-strong-node-cache (get-blob-id node) entity-id (get-node-id node) node)
  (swap! node-collector (fn [old-state]
                          (-> old-state
                              (assoc-in [(get-node-collector-idx node) (get-node-id node)] node)))))
;end node collector

;constructors
;======================================================================================================================
(defn create-leaf-node [array left-pointer ^Atom node-collector blob-id entity-id]
  (let [uuid (state/dispense blob-id)
        _ (println-m "create leaf-node uuid" uuid "blob-id" blob-id)
        new-node (LeafNode. array left-pointer uuid blob-id)]
    (add-to-node-collector node-collector new-node entity-id)
    new-node))

(defn create-leaf-node-from-proto [array left-pointer uuid blob-id]
  (LeafNode. array left-pointer uuid blob-id))

(defn create-inner-node [array ^Atom node-collector blob-id entity-id]
  (let [uuid (state/dispense blob-id)
        _ (println-m "create inner node uuid" uuid "blob-id" blob-id)
        new-node (InnerNode. array uuid blob-id)]
    (add-to-node-collector node-collector new-node entity-id)
    new-node))

(defn create-inner-node-from-proto [array uuid blob-id]
  (InnerNode. array uuid blob-id))

(defn b-tree [cnt root]
  ;replace the root pointer with byte-array
  {:cnt cnt :root (to-remote-pointer root)})

(defn new-b-tree [key-vals blob-id ^Atom node-collector entity-id]
  ;create the tree's first leaf node
  (b-tree 0 (create-leaf-node (rrbv/vec key-vals) nil node-collector blob-id entity-id)))
;end

;Public
;======================================================================================================================
(defn io-assoc [entity-id entity-type key-vals blob-id node-collector-ch root-ch]
  (let [{:keys [root] :as tree} (get-b-plus-tree entity-id)
        [blob-id a-chan] [blob-id (chan 1)]
        node-collector (new-node-collector)
        tree (if (= nil root)
               (new-b-tree key-vals blob-id node-collector entity-id)
               (b-tree 0 (n-assoc-bulk-dispatcher root key-vals [] node-collector blob-id entity-id)))]
    ;save the tree
    (save-b-plus-tree entity-id tree)
    (>!! node-collector-ch {:entity-id entity-id :node-collector @node-collector})
    (>!! root-ch {:io-b-tree tree :last-entity-root (peek key-vals) :entity-info {:entity-id entity-id :entity-type entity-type}})))


(defn io-find-range
  "Searches the b-plus tree for a range of keys.
  key-start >= key-end must be true, i.e. only backwards search is supported"
  [entity-id key-start key-end limit responce-ch]
  (if-let [tree-current (get-b-plus-tree entity-id)]
    ;search the tree
    (let [n-find-range-direction (if (< ^long key-start ^long key-end) (throw (Exception. "searching forward not supported")) n-find-range-backward-dispatcher)]
      ;determine search direction
      (n-find-range-direction (-> tree-current :root) key-start key-end limit nil responce-ch [] entity-id))
    ;tree doesn't exist
    (do (println-m "tree doesnt exist")
        (>!! responce-ch {:result []}))))
;end

;ARRAY ITEM CONVERSIONS
;----------------------
(defn- node->node-tuple-byte-array
  [^neversleep_db.b_plus_tree_protocol.DurableBPlusTreeNode node]
  (serializer/node-pointer-to-typed-byte-array (get-node-id node) (get-blob-id node)))


(defn- inner-node-array-item->byte-string
  [x]
  (-> (cond
        ;if instance of LeafNode or InnerNode node, convert to tuple
        (or (instance? LeafNode x) (instance? InnerNode x))
        (-> x (node->node-tuple-byte-array))
        ;if already a byte-array (from a node loaded from disk)
        (instance? util/byte-array-class x)
        x
        ;else, it's a key (Long)
        :else
        (util/serialize x))
      (util/copy-to-byte-string)))


(defn- leaf-node-array-item->byte-string
  "No need for byte-array magic here"
  [idx x]
  (-> (cond
        ;if key...
        (even? idx)
        (util/serialize x)
        ;else, it's a val (MapRoot)
        :else
        (protobuf-dump x))
      (util/copy-to-byte-string)))
;--------------------------
;END ARRAY ITEM CONVERSIONS

(defn- inner-node-array->repeatable-bytes
  "Takes a node's array and maps to-byte-string to each item"
  [^"[Ljava.lang.Object;" array]
  (map inner-node-array-item->byte-string array))

(defn- leaf-node-array->repeatable-bytes
  "Takes a node's array and maps to-byte-string to each item"
  [^"[Ljava.lang.Object;" array]
  (map-indexed leaf-node-array-item->byte-string array))
;end to-protobuf helpers

(defn bpt-type-dispatch
  "Take a-node (protobuf map) and returns deftype representation, either LeafNode or InnerNode"
  [^APersistentMap a-node ^long blob-id ^long node-type]
  (condp = node-type
    1 (create-leaf-node-from-proto (into [] (map-indexed blob-unpacker/b-tree-leaf-node-byte-string->array-item (:array a-node)))
                                   (if-let [left-pointer (:leftpointer a-node)]
                                     (util/byte-string-to-byte-array left-pointer))
                                   (:uuid a-node)
                                   blob-id)
    2 (create-inner-node-from-proto (into [] (map-indexed blob-unpacker/b-tree-inner-node-byte-string->array-item (:array a-node)))
                                    (:uuid a-node)
                                    blob-id)
    (Exception. "unrecognized b-tree node type")))

(defn n-assoc-bulk-dispatcher [node-pointer key-vals parent-pointers node-collector blob-id entity-id]
  (if (instance? util/byte-array-class node-pointer)
    ;either a map
    (let [node-or-exception (let [blob (serializer/get-blob-id node-pointer)
                                  uuid (serializer/get-node-id node-pointer)
                                  node-chan (chan 1)]
                              (blob-unpacker/fetch-node-onto-chan blob entity-id uuid node-chan)
                              ;wait
                              (<!! node-chan))]
      (n-assoc-bulk node-or-exception key-vals parent-pointers node-collector blob-id entity-id))
    ;else call pointer directly
    (n-assoc-bulk node-pointer key-vals parent-pointers node-collector blob-id entity-id)))

(defn n-find-range-backward-dispatcher [node-pointer key-start key-end limit not-found responce-ch results entity-id]
  (println-m "node-pointer" node-pointer)
  (if (instance? util/byte-array-class node-pointer)
    ;either a map
    (let [node-or-exception (let [blob (serializer/get-blob-id node-pointer)
                                  uuid (serializer/get-node-id node-pointer)
                                  node-chan (chan 1)]
                              (blob-unpacker/fetch-node-onto-chan blob entity-id uuid node-chan)
                              (let [[value _] (alts!! [node-chan (timeout 10000)])]
                                (if (= nil value)
                                  (do
                                    ;log the exception
                                    (>!! exception-log/incoming-exceptions (Exception. (str "b-plus-tree-batch/n-find-range-backward-dispatcher could not find node within 10000 ms :blob " blob " :uuid " uuid)))
                                    (>!! responce-ch {:result [] :error {:message (str "timeout 10000ms")}}))
                                  value)))]
      (n-find-range-backward node-or-exception key-start key-end limit not-found responce-ch results entity-id))
    ;else call pointer directly
    (do
      (n-find-range-backward node-pointer key-start key-end limit not-found responce-ch results entity-id))))

(defn init
  "Called at server start"
  []
  (blob-unpacker/register-type-dispatch-fn-1 bpt-type-dispatch))
