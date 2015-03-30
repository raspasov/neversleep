(ns neversleep-db.persistent-durable-map
  (:require [neversleep-db.persistent-durable-map-protocol :refer :all]
            [aprint.core :refer [aprint]]
            [neversleep-db.node-keeper :as node-keeper]
            [clojure.core.async :refer [chan go >! <! <!! >!! go-loop put! thread alts! alts!! timeout pipeline close!]]
            [flatland.protobuf.core :refer :all]
            [neversleep-db.command-log :as command-log]
            [neversleep-db.println-m :refer [println-m]]
            [neversleep-db.blob-unpacker :as blob-unpacker]
            [clojure.core.incubator :refer [dissoc-in]]
            [neversleep-db.b-plus-tree-sorted-map :as bpt-sm]
            [neversleep-db.util :as util]
            [neversleep-db.state :as state]
            [neversleep-db.serializer :as serializer]
            [neversleep-db.exception-log :as exception-log])
  (:import (clojure.lang Util Box IFn)
           (jv BinaryOps SystemClock MutableUtils)
           (com.google.protobuf ByteString)
           com.durablenode.Durablenode$ArrayNode
           com.durablenode.Durablenode$BitmapIndexedNode
           com.durablenode.Durablenode$HashCollisionNode
           com.durablenode.Durablenode$ValNode
           (java.util.concurrent.atomic AtomicReference)))

(declare create-bitmap-indexed-node
         create-bitmap-indexed-node-from-proto
         empty-bitmap-indexed-node
         create-array-node
         create-array-node-from-proto
         create-hash-collision-node
         create-hash-collision-node-from-proto
         create-node
         clone-and-set
         bitmap-indexed-node->proto
         array-node->proto
         hash-collision-node->proto
         new-io-map!
         n-find-dispatcher
         n-assoc-dispatcher
         get-node-collector-idx
         add-to-node-collector
         node->node-tuple-byte-array
         collect-key-vals
         collect-key-vals-dispatcher
         remove-pair
         n-without-dispatcher
         val-node-fetch
         create-val-node
         create-val-node-from-proto
         val-node->proto
         node-equiv?)

(def durable-node-class neversleep_db.persistent_durable_map_protocol.DurableNode)

;Protocol buffer defininitions
(def BitmapIndexedNodeProtoDef (protodef Durablenode$BitmapIndexedNode))
(def ArrayNodeProtoDef (protodef Durablenode$ArrayNode))
(def HashCollisionNodeProtoDef (protodef Durablenode$HashCollisionNode))
(def ValNodeProtoDef (protodef Durablenode$ValNode))
;end

(defn- new-node-collector
  "Create a new node collector atom instance (called at the begging of io-assoc)"
  []
  (atom [{} {} {} {}]))

(defn new-box
  "Holds state during tree recursion to determine cnt of entity"
  [a-val]
  (new Box a-val))

(defn get-box-val [^Box box]
  (-> box .val))

(defn set-box-val! [^Box box a-val]
  (set! (. box val) a-val))

(defn mask
  "Returns the integer index for the specified hash/shift combination, 0-31 integer always"
  [^Integer hash ^Integer shift]
  ;{:pre  [(instance? Integer hash) (instance? Integer shift)]
  ; :post [(instance? Integer %)]}
  (-> hash
      (BinaryOps/unsignedBitShiftRight shift)
      (BinaryOps/bitAnd (int 0x01f))))

(defn bitpos
  "Maps hash/shift combination to a number that
  has binary representation in the form 10^n, n>=0, e.g. 10000000000
  (one and a bunch of zeros because we are bit-shifting left 1 with 0-31"
  [^Integer hash ^Integer shift]
  ;{:pre  [(and (instance? Integer hash) (instance? Integer shift))]
  ; :post [(instance? Integer %)]}
  (BinaryOps/bitShiftLeft (int 1) (mask hash shift)))


(defn index
  [^Integer bitmap ^Integer bitpos]
  ;{:pre  [(instance? Integer bitmap) (instance? Integer bitpos)]
  ; :post [(instance? Integer %)]}
  (Integer/bitCount (BinaryOps/bitAnd bitmap (unchecked-subtract-int bitpos (int 1)))))


(defn hash-code [key]
  (Util/hasheq key))


(defn save-map! [^String id io-map]
  (swap! state/all-maps assoc id io-map))


(defn persistent-durable-map
  "Returns a new root for the map (new root with each io-assoc)"
  [cnt root]
  {:cnt cnt :root (to-remote-pointer root)})


(defn- new-io-map! [entity-id blob-id]
  (let [new-node (empty-bitmap-indexed-node blob-id)]
    (blob-unpacker/add-to-strong-node-cache (get-blob-id new-node) entity-id (get-node-id new-node) new-node)
    (persistent-durable-map 0 new-node)))

(defn get-map-current! [^String id]
  (get @state/all-maps id))
;END MAPS STATE

(defn atomic-ref []
  (AtomicReference.))


(deftype BitmapIndexedNode [atom-or-nil ^:volatile-mutable bitmap ^:volatile-mutable ^"[Ljava.lang.Object;" array uuid blob-identifier]
  DurableNode

  (to-remote-pointer [this] (node->node-tuple-byte-array this))

  (to-protobuf [this] (bitmap-indexed-node->proto this))

  (get-node-type [_] 1)

  (get-node-id [_] uuid)

  (get-blob-id [_] blob-identifier)

  (get-bitmap [_] bitmap)
  (get-array [_] array)

  (set-array! [this new-array]
    (set! array new-array)
    this)

  (set-in-array! [this i o]
    (aset array i o)
    this)

  (bitmap-bit-or! [this new-bit]
    (set! bitmap (BinaryOps/bitOr bitmap new-bit))
    this)

  (array-copy-on-this! [this src-pos dest-pos how-many]
    (System/arraycopy array src-pos array dest-pos how-many))

  (ensure-editable! [this edit blob-id]
    (if (= atom-or-nil edit)
      this
      (let [n (Integer/bitCount bitmap)
            new-array (object-array (if (<= 0 n) (* 2 (+ n 1)) 4))]
        (System/arraycopy array 0 new-array 0 (* 2 n))
        (create-bitmap-indexed-node edit bitmap new-array blob-id))))

  (edit-and-set! [this edit i a blob-id]
    (-> (ensure-editable! this edit blob-id)
        (set-in-array! i a)))

  (edit-and-set! [this edit i a j b blob-id]
    (-> (ensure-editable! this edit blob-id)
        (set-in-array! i a)
        (set-in-array! j b)))


  ;REGULAR N-ASSOC
  ;===================================
  (n-assoc [this shift hash key val added-leaf node-collector blob-id entity-id]

    (println-m "n-assoc REGULAR val" val (class val))

    (let [bit (bitpos hash shift)                           ;bit is a number with 1 and a bunch of zeros after it, i.e. 1000000000
          ^int idx (index bitmap bit)]                           ;idx goes only up to 16

      ;PRINT
      ;(println-m "NEW LEVEL ============================================")
      ;(println-m "shift::" shift)
      ;(println-m "hash:: " hash)
      ;(println-m "bit::" bit)
      ;(println-m "bit (binary):: " (Integer/toBinaryString bit))
      ;(println-m "idx::" idx)
      ;(println-m "array::")
      ;(aprint array)
      ;(println-m "bitmap:" bitmap)
      ;(println-m "bitmap binary:" (Integer/toBinaryString bitmap))
      ;END PRINT

      ;check if index (current level) is in the bitmap
      (if (not= (BinaryOps/bitAnd bitmap bit) (int 0))
        ;if true, there is something at those indices, guaranteed
        ;first option
        (let [key-or-nil  (nth array (unchecked-multiply-int 2 idx))
              val-or-node (nth array (unchecked-add-int (unchecked-multiply-int 2 idx) 1))]
          ;(println-m "LOGIC TREE:: big if, true")
          (cond
            ;first option
            (= nil key-or-nil)
            (do
              ;(println-m "LOGIC TREE:: big if, true | key-or-nil is nil")
              ;must be a node here
              (let [n (n-assoc-dispatcher val-or-node (unchecked-add-int shift 5) hash key val added-leaf node-collector blob-id entity-id)]
                ;adding a "node"
                (if (node-equiv? n val-or-node)
                  ;true
                  this
                  ;false
                  ;RETURN
                  (create-bitmap-indexed-node nil bitmap (clone-and-set array (unchecked-add-int (unchecked-multiply-int 2 idx) 1) n) blob-id))))
            ;second option
            (= key key-or-nil)
            ;replacing the "val"
            (do
              ;(println-m "LOGIC TREE:: big if, true | key-or-nil is equal to key")
              (if (= val (get-value (val-node-fetch entity-id val-or-node)))
                ;true
                (do
                  ;(println-m "LOGIC TREE: this")
                  ;RETURN
                  ;NO VAL CHANGE
                  this)
                ;false
                (do                                         ;(println-m "replacing val")
                  ;RETURN
                  ;OVERWRITE
                  (create-bitmap-indexed-node nil bitmap (clone-and-set array (+ (* 2 ^int idx) 1) (create-val-node val node-collector blob-id entity-id)) blob-id))))
            ;taking a key/value down with us into the inception hole (aka creating a new node and putting it into the same position)
            :else (do
                    ;(println-m "LOGIC TREE:: big if, true | NEW LEVEL OF INCEPTION")
                    (set-box-val! added-leaf added-leaf)
                    ;RETURN
                    (create-bitmap-indexed-node nil bitmap (clone-and-set array
                                                                          ;assoc two things (key => val) in one fn call
                                                                          ;key      val
                                                                          (* 2 idx) nil
                                                                          (+ (* 2 idx) 1) (create-node (unchecked-add-int shift 5) key-or-nil val-or-node hash key val node-collector blob-id entity-id)) blob-id))))
        ;else - there is nothing at those indices, proceed towards adding to the array?
        (let [n (Integer/bitCount bitmap)]
          ;(println-m "LOGIC TREE:: big if, else")
          ;(println-m "n::" n)
          ;(println-m "(count array)" (count array))
          (cond
            (<= 16 n)
            ;first case - building a full node
            (do
              ;(println-m "LOGIC TREE:: big if, else | (>= n 16)")
              (let [nodes-array! (object-array 32)          ;empty array that we're building up in the loop below
                    jdx (mask hash shift)]
                (aset nodes-array! jdx (-> (empty-bitmap-indexed-node blob-id)
                                           (n-assoc-dispatcher (unchecked-add-int shift 5) hash key val added-leaf node-collector blob-id entity-id)))

                ;loop over the whole bitmap one by one (i index) and check if there's something there with a binary & 1
                (loop [i 0
                       j 0]
                  (if (< i 32)

                    (do (if (-> (BinaryOps/unsignedBitShiftRight bitmap (int i)) (BinaryOps/bitAnd (int 1)) (not= 0))
                          ;true - yes there is
                          ;check if there's something on the current i index in the bitmap
                          (do (if (= (nth array j) nil)
                                (aset nodes-array! i (nth array (+ j 1))) ;true
                                (aset nodes-array! i (-> (empty-bitmap-indexed-node blob-id) ;else - create a BitmapIndexedNode and put the key/val pair in it
                                                         (n-assoc-dispatcher (unchecked-add-int shift 5), (hash-code (nth array j)), (nth array j), (get-value (val-node-fetch entity-id (nth array (+ j 1)))), added-leaf, node-collector, blob-id, entity-id))))
                              (recur (+ i 1) (+ j 2)))
                          ;false
                          (recur (+ i 1) j)))))
                ;return
                ;(println-m "node array before being set in an ArrayNode:::")
                ;(aprint nodes-array!)
                ;RETURN
                (create-array-node nil (unchecked-add-int n 1) nodes-array! blob-id)))
            :else
            (do
              ;(println-m "LOGIC TREE::  big if, else | else")
              (let [new-java-array! (object-array (* 2 (+ n 1)))]
                ;copy head of the old array to the new array (could be nothing if idx = 0)
                (System/arraycopy array 0 new-java-array! 0 (* 2 ^int idx))
                ;set the box leaf to communicate up the recursion (inception) levels
                (set-box-val! added-leaf added-leaf)
                ;set the new vals in the new array (based on idx)
                (aset new-java-array! (unchecked-multiply-int 2 idx) key)
                (aset new-java-array! (+ (* 2 idx) 1) (create-val-node val node-collector blob-id entity-id))
                ;copy the tail over from the old to the new array
                (System/arraycopy array (* 2 idx) new-java-array! (unchecked-multiply-int 2 (unchecked-add-int idx 1)) (unchecked-multiply-int 2 (unchecked-subtract-int n idx)))
                ;RETURN
                (create-bitmap-indexed-node nil (BinaryOps/bitOr bitmap bit) new-java-array! blob-id))))))))

  ;N-ASSOC WITH EDIT
  ;==================================================
  (n-assoc [this edit shift hash key val added-leaf node-collector blob-id entity-id]

    (println-m "n-assoc EDIT val" val (class val))
    (let [bit (bitpos hash shift)                           ;bit is a number with 1 and a bunch of zeros after it, i.e. 1000000000
          ^int idx (index bitmap bit)]                           ;idx goes only up to 16

      ;PRINT
      ;(println-m "NEW LEVEL ============================================")
      ;(println-m "shift::" shift)
      ;(println-m "hash:: " hash)
      ;(println-m "bit::" bit)
      ;(println-m "bit (binary):: " (Integer/toBinaryString bit))
      ;(println-m "idx::" idx)
      ;(println-m "array::")
      ;(aprint array)
      ;(println-m "bitmap:" bitmap)
      ;(println-m "bitmap binary:" (Integer/toBinaryString bitmap))
      ;END PRINT

      ;check if index (current level) is in the bitmap
      (if (not= (BinaryOps/bitAnd bitmap bit) 0)
        ;there is something at those indices, guaranteed
        ;first option
        (do
          ;(println-m "LOGIC TREE:: big if, true")
          (let [key-or-nil (nth array (* 2 idx))
                val-or-node (nth array (+ (* 2 idx) 1))]
            (cond
              ;first option
              (= nil key-or-nil)
              (do
                ;(println-m "LOGIC TREE:: big if, true | key-or-nil is nil")
                ;must be a node here
                ;TODO check this - edit was missing
                (let [n (n-assoc-dispatcher val-or-node edit (unchecked-add-int shift 5) hash key val added-leaf node-collector blob-id entity-id)]
                  ;adding a "node"
                  (if (node-equiv? n val-or-node)
                    ;true
                    this
                    ;false
                    (edit-and-set! this edit (+ (* 2 idx) 1) (create-val-node val node-collector blob-id entity-id) blob-id))))
              ;second option
              (= key key-or-nil)
              ;replacing the "val"
              (do
                ;(println-m "LOGIC TREE:: big if, true | key-or-nil is equal to key")
                (if (= val (get-value (val-node-fetch entity-id val-or-node)))
                  ;true
                  (do                                       ;(println-m "LOGIC TREE: this")
                    this)
                  ;false
                  (do                                       ;(println-m "replacing val")
                    (edit-and-set! this edit (+ (* 2 idx) 1) (create-val-node val node-collector blob-id entity-id) blob-id))))
              :else (do
                      ;taking a key/value down with us into the inception hole (aka creating a new node and putting it into the same position)
                      ;(println-m "LOGIC TREE:: big if, true | NEW LEVEL OF INCEPTION")
                      (set-box-val! added-leaf added-leaf)
                      ;set a nil/node pair
                      (edit-and-set! this edit
                                     (* 2 idx) nil
                                     (+ (* 2 idx) 1) (create-node edit (unchecked-add-int shift 5) key-or-nil val-or-node hash key val node-collector blob-id entity-id) blob-id)))))
        ;else - there is nothing at those indices, proceed towards adding to the array
        (do
          ;(println-m "LOGIC TREE:: big if, else")
          (let [n (Integer/bitCount bitmap)]

            ;PRINT
            ;(println-m "n::" n)
            ;(println-m "(count array)::" (count array))
            ;END PRINT

            (cond
              (< (* 2 n) (count array))
              (do
                ;(println-m "LOGIC TREE:: big if, else | first case (aka the edit/do not make unneeded children nodes)")
                (set-box-val! added-leaf added-leaf)

                (let [editable-node (ensure-editable! this edit blob-id)]
                  (array-copy-on-this! editable-node (* 2 idx) (* 2 (+ idx 1)) (* 2 (- n idx)))
                  (-> editable-node
                      (set-in-array! (* 2 idx) key)
                      (set-in-array! (+ (* 2 idx) 1) (create-val-node val node-collector blob-id entity-id))
                      (bitmap-bit-or! bit))))
              (<= 16 n)
              ;first case
              (do
                ;(println-m "LOGIC TREE:: big if, else | (>= n 16)")
                (let [nodes-array! (object-array 32)
                      jdx (mask hash shift)]
                  (aset nodes-array! jdx (-> (empty-bitmap-indexed-node blob-id)
                                             (n-assoc-dispatcher edit (unchecked-add-int shift 5) hash key val added-leaf node-collector blob-id entity-id)))
                  (loop [i 0
                         j 0]
                    (if (< i 32)
                      (do (if (-> (BinaryOps/unsignedBitShiftRight bitmap (int i)) (BinaryOps/bitAnd (int 1)) (not= 0))
                            ;true
                            (do (if (= (nth array j) nil)
                                  (aset nodes-array! i  (nth array (+ j 1)))
                                  (aset nodes-array! i (-> (empty-bitmap-indexed-node blob-id)
                                                           (n-assoc-dispatcher edit (unchecked-add-int shift 5), (hash-code (nth array j)), (nth array j), (get-value (val-node-fetch entity-id (nth array (+ j 1)))), added-leaf, node-collector, blob-id, entity-id))))
                                (recur (+ i 1) (+ j 2)))
                            ;false
                            (recur (+ i 1) j)))))
                  ;return
                  ;(println-m "node array before being set in an ArrayNode:::")
                  ;(aprint nodes-array!)
                  (create-array-node edit (+ n 1) nodes-array! blob-id)))
              :else
              (do
                ;(println-m "LOGIC TREE::  big if, else | else")
                (let [new-java-array! (object-array (* 2 (+ n 4)))]
                  ;copy head of the old array to the new array (could be nothing if idx = 0)
                  (System/arraycopy array 0 new-java-array! 0 (* 2 idx))
                  ;set the box leaf to communicate up the recursion (inception) levels
                  (set-box-val! added-leaf added-leaf)
                  ;set the new vals in the new array (based on idx)
                  (aset new-java-array! (* 2 idx) key)
                  (aset new-java-array! (+ (* 2 idx) 1) (create-val-node val node-collector blob-id entity-id))
                  ;copy the tail over from the old to the new array
                  (System/arraycopy array (* 2 idx) new-java-array! (* 2 (+ idx 1)) (* 2 (- n idx)))

                  (-> (ensure-editable! this edit blob-id)
                      (set-array! new-java-array!)
                      (bitmap-bit-or! bit))))))))))

  ;REGULAR n-without
  (n-without [this shift hash key node-collector blob-id entity-id]
    (let [bit (bitpos hash shift)]
      (cond (= 0 (BinaryOps/bitAnd bitmap bit))
            this
            :else
            (let [^int idx (index bitmap bit)
                  key-or-nil (nth array (unchecked-multiply-int 2 idx))
                  val-or-node (nth array (unchecked-add-int 1 (unchecked-multiply-int 2 idx)))]
              (cond (= nil key-or-nil)
                    (let [n (n-without-dispatcher val-or-node (unchecked-add-int shift (int 5)) hash key node-collector blob-id entity-id)]
                      (cond (node-equiv? n val-or-node)
                            this
                            (not= n nil)
                            (create-bitmap-indexed-node nil bitmap (clone-and-set array (+ (* 2 idx) 1) n) blob-id)
                            (= bitmap bit)
                            nil
                            :else
                            (create-bitmap-indexed-node nil (BinaryOps/bitXOr bitmap bit) (remove-pair array idx) blob-id)))
                    (Util/equiv key key-or-nil)
                    (create-bitmap-indexed-node nil (BinaryOps/bitXOr bitmap bit) (remove-pair array idx) blob-id)
                    :else
                    this)))))

  ;disk implementation
  (n-find [this shift hash key not-found responce-ch entity-id]
    (let [bit (bitpos hash shift)]
      (cond
        ;case 1
        (= 0 (BinaryOps/bitAnd (get-bitmap this) bit))
        (>!! responce-ch {:result nil})
        :else
        (let [^int idx (index (get-bitmap this) bit)
              ;get the nth element from the array, toByteArray it, thaw it
              key-or-nil (-> this (get-array) (nth (* 2 idx)))
              val-or-node (-> this (get-array) (nth (+ (* 2 idx) 1)))]
          (cond
            ;if it's a nil there must be a node
            (= nil key-or-nil)
            ;it's a node, we need to fetch it
            (n-find-dispatcher val-or-node (unchecked-add-int shift 5) hash key not-found responce-ch entity-id)
            ;key found
            (= key key-or-nil)
            (do
              ;(println-m "FOUND::: " {:result {(str key-or-nil) val-or-node}})
              (>!! responce-ch {:result (get-value (val-node-fetch entity-id val-or-node))}))
            :else
            (do
              ;not-found
              (>!! responce-ch {:result nil}))))))))

;STORES OTHER NODES BUT NOT ANY KEY/VALUE PAIRS - YES THIS IS TRUE
(deftype ArrayNode [atom-or-nil ^:volatile-mutable cnt ^:volatile-mutable ^"[Ljava.lang.Object;" array uuid blob-identifier]
  DurableNode

  (to-remote-pointer [this] (node->node-tuple-byte-array this))

  (to-protobuf [this] (array-node->proto this))

  (get-node-type [_] 2)

  (get-node-id [_] uuid)

  (get-blob-id [_] blob-identifier)

  (get-array [_] array)

  (get-count [_] cnt)

  (inc-cnt! [this]
    (set! cnt (unchecked-inc-int cnt))
    this)

  (set-in-array! [this i o]
    (aset array i o)
    this)

  (ensure-editable! [this edit blob-id]
    (if (= atom-or-nil edit)
      this
      (create-array-node edit cnt (aclone array) blob-id)))

  (edit-and-set! [this edit i n blob-id]
    (-> (ensure-editable! this edit blob-id)
        (set-in-array! i n)))

  ;ArrayNode - n-assoc
  (n-assoc [this shift hash key val added-leaf node-collector blob-id entity-id]
    (let [^int idx (mask hash shift)
          node (nth array idx)]
      (if (= nil node)
        ;true
        (create-array-node nil (+ ^int cnt 1) (clone-and-set array idx (-> (empty-bitmap-indexed-node blob-id)
                                                                      (n-assoc-dispatcher (unchecked-add-int shift (int 5)) hash key val added-leaf node-collector blob-id entity-id))) blob-id)
        ;false
        (do
          ;(println-m "node string?::" node)
          (let [n (n-assoc-dispatcher node (unchecked-add-int shift (int 5)) hash key val added-leaf node-collector blob-id entity-id)]
            (if (node-equiv? n node)
              this
              (create-array-node nil cnt (clone-and-set array idx n) blob-id)))))))

  ;ArrayNode - n-assoc with edit
  (n-assoc [this edit shift hash key val added-leaf node-collector blob-id entity-id]
    (let [idx (mask hash shift)
          node (nth array idx)]
      (if (= nil node)
        ;true
        (-> (edit-and-set! this edit idx (-> (empty-bitmap-indexed-node blob-id)
                                             (n-assoc-dispatcher edit (unchecked-add-int shift (int 5)) hash key val added-leaf node-collector blob-id entity-id)) blob-id)
            (inc-cnt!))
        ;false
        (let [n (n-assoc-dispatcher node edit (unchecked-add-int shift 5) hash key val added-leaf node-collector blob-id entity-id)]
          (if (node-equiv? n node)
            this
            (edit-and-set! this edit idx n blob-id))))))

  (pack [this edit idx blob-id]
    (let [return-array (MutableUtils/pack cnt array edit idx)]
      (create-bitmap-indexed-node edit (nth return-array 0) (nth return-array 1) blob-id)))

  (n-without [this shift hash key node-collector blob-id entity-id]
    (let [idx (mask hash shift)
          node (nth array idx)]
      (if (= nil node)
        this
        (let [n (n-without-dispatcher node (unchecked-add-int shift 5) hash key node-collector blob-id entity-id)]
          (cond (node-equiv? n node)
                this
                (= n nil)
                (if (<= ^int cnt 8)
                  (pack this nil idx blob-id)
                  (create-array-node nil (unchecked-subtract-int cnt 1) (clone-and-set array idx n) blob-id))
                :else
                (create-array-node nil cnt (clone-and-set array idx n) blob-id))))))

  ;disk implementation
  (n-find [this shift hash key not-found responce-ch entity-id]
    (let [idx (mask hash shift)
          node-remote-pointer (-> this (get-array) (nth idx))]
      (if (= node-remote-pointer nil)
        (>!! responce-ch {:result nil})
        (n-find-dispatcher node-remote-pointer (unchecked-add-int shift 5) hash key not-found responce-ch entity-id)))))

(deftype HashCollisionNode [atom-or-nil ^Integer node-hash ^:volatile-mutable cnt ^:volatile-mutable ^"[Ljava.lang.Object;" array uuid blob-identifier]
  DurableNode

  (to-remote-pointer [this] (node->node-tuple-byte-array this))

  (to-protobuf [this] (hash-collision-node->proto this))

  (get-node-type [_] 3)

  ;getters
  (get-hash [_] node-hash)
  (get-count [_] cnt)
  (get-array [_] array)
  (get-node-id [_] uuid)
  (get-blob-id [_] blob-identifier)

  (set-array! [this new-array]
    (set! array new-array)
    this)

  (set-in-array! [this i o]
    (aset array i o)
    this)

  (set-cnt! [this new-cnt]
    (set! cnt new-cnt)
    this)

  (inc-cnt! [this]
    (set! cnt (unchecked-inc-int cnt))
    this)

  (find-index [this key]
    (loop [i 0]
      (if (< i (* 2 ^int cnt)) ;loop until
        (if (= key (nth array i))
          i ;key found, return index
          (recur (+ i 2))) ;else continue the loop
        -1 ))) ;return -1 if key not found

  (edit-and-set! [this edit i a blob-id]
    (-> (ensure-editable! this edit blob-id)
        (set-in-array! i a)))

  (edit-and-set! [this edit i a j b blob-id]
    (-> (ensure-editable! this edit blob-id)
        (set-in-array! i a)
        (set-in-array! j b)))

  (ensure-editable! [this edit blob-id]
    (if (= atom-or-nil edit)
      this
      (let [new-array (object-array (* 2 (+ ^int cnt 1)))]
        (System/arraycopy array 0 new-array 0 (* 2 ^int cnt))
        (create-hash-collision-node edit node-hash cnt new-array blob-id))))

  (ensure-editable! [this edit -cnt -array blob-id]
    (if (= atom-or-nil edit)
      ;return
      (-> this
          (set-array! -array)
          (set-cnt! -cnt))
      ;else return
      (create-hash-collision-node edit node-hash -cnt -array blob-id)))

  ;REGULAR
  (n-assoc [this shift hash key val added-leaf node-collector blob-id entity-id]
    (if (= hash node-hash)
      ;if hashes are the same
      (let [idx (find-index this key)]
        (if (not= -1 idx)
          ;index found
          (if (= val (get-value (val-node-fetch entity-id (nth array (+ ^int idx 1)))))
            ;return
            this
            ;else return
            (create-hash-collision-node nil hash cnt (clone-and-set array (+ ^int idx 1) (create-val-node val node-collector blob-id entity-id)) blob-id))
          ;idx = -1, not found
          (let [new-array (object-array (* 2 (+ ^int cnt 1)))]
            (System/arraycopy array 0 new-array 0 (* 2 ^int cnt))
            (aset new-array (* 2 ^int cnt) key)
            (aset new-array (+ (* 2 ^int cnt) 1) (create-val-node val node-collector blob-id entity-id))
            (set-box-val! added-leaf added-leaf)
            ;return
            (create-hash-collision-node atom-or-nil hash (+ ^int cnt 1) new-array blob-id))))
      ;else - hashes are not the same
      (-> (create-bitmap-indexed-node nil (bitpos node-hash shift) (object-array [nil this]) blob-id)
          (n-assoc-dispatcher shift hash key val added-leaf node-collector blob-id entity-id))))

  ;WITH EDIT
  (n-assoc [this edit shift hash key val added-leaf node-collector blob-id entity-id]
    (if (= hash node-hash)
      ;if hashes are the same
      (let [^int idx (find-index this key)]
        (cond (not= -1 idx) ;index found
              (if (= val (get-value (val-node-fetch entity-id (nth array (+ idx 1)))))
                ;return
                this
                ;else return
                (edit-and-set! this edit (+ idx 1) (create-val-node val node-collector blob-id entity-id) blob-id))
              (< (* 2 ^int cnt) (alength array)) ;edit specific logic
              (do (set-box-val! added-leaf added-leaf)
                  ;return
                  (-> (edit-and-set! this edit (* 2 ^int cnt) key (+ (* 2 ^int cnt) 1) (create-val-node val node-collector blob-id entity-id) blob-id)
                      (inc-cnt!)))
              :else
              (let [new-array (object-array (+ (alength array) 2))]
                (System/arraycopy array 0 new-array 0 (alength array))
                (aset new-array (alength array) key)
                (aset new-array (+ (alength array) 1) (create-val-node val node-collector blob-id entity-id))
                (set-box-val! added-leaf added-leaf)
                ;return
                (ensure-editable! this edit (+ ^int cnt 1) new-array blob-id))))
      ;else, hashes are not the same
      (-> (create-bitmap-indexed-node edit (bitpos node-hash shift) (object-array [nil this nil nil]) blob-id)
          (n-assoc-dispatcher edit shift hash key val added-leaf node-collector blob-id entity-id))))

  (n-without [this shift hash key node-collector blob-id entity-id]
    (let [idx (find-index this key)]
      (cond (= idx -1)
            this
            (= cnt 1)
            nil
            :else
            (create-hash-collision-node nil hash (unchecked-subtract-int cnt 1) (remove-pair array (unchecked-divide-int idx 2)) blob-id))))

  (n-find [this shift hash key not-found responce-ch entity-id]

    (let [idx (find-index this key)]
      (cond (< ^int idx 0) ;first option - not found
            (>!! responce-ch {:result nil})

            (= key (nth array idx)) ;second option - found
            (do ;(println-m "found key from hash coll n-find::" key)
                (>!! responce-ch {:result (get-value (val-node-fetch entity-id (nth array (+ ^int idx 1))))}))
            ;else - not found
            :else (>!! responce-ch {:result nil})))))


(deftype ValNode [value uuid blob-identifier]
  DurableNode

  (to-remote-pointer [this] (node->node-tuple-byte-array this))

  (to-protobuf [this] (val-node->proto this))

  (get-value [this] value)

  (get-node-id [this] uuid)

  (get-node-type [this] 4)

  (get-blob-id [this] blob-identifier))

(defn pdm-type-dispatch
  "Take a-node (protobuf map) and returns deftype representation, either BitmapIndexedNode or ArrayNode or HashCollisionNode"
  [a-node ^long blob-id ^long node-type]
  (cond (= 1 node-type)
        (do (if (= nil (:bitmap a-node))
              (println-m "WRONG DATA FOR type-dispatch:: " a-node blob-id node-type))
            (create-bitmap-indexed-node-from-proto (:bitmap a-node) (map blob-unpacker/byte-string->array-item (:array a-node)) (:uuid a-node) blob-id))
        (= 2 node-type)
        (do (if (or (= nil (:cnt a-node)) (= nil (:array a-node)))
              (println-m "WRONG DATA FOR type-dispatch:: " a-node blob-id node-type))
            (create-array-node-from-proto (:cnt a-node) (map blob-unpacker/byte-string->array-item (:array a-node)) (:uuid a-node) blob-id))
        (= 3 node-type)
        (do ;(println-m "HashCollisionNode from type-dispatch::" a-node)
            (create-hash-collision-node-from-proto (:nodehash a-node) (:cnt a-node) (map blob-unpacker/byte-string->array-item (:array a-node)) (:uuid a-node) blob-id))
        (= 4 node-type)
        (do (create-val-node-from-proto (-> ^ByteString (:value a-node) (.toByteArray) (util/de-serialize)) (:uuid a-node) blob-id))
        :else
        (Exception. "unrecognized persistent durable map node type")))


(defn- get-node-collector-idx
  "Based on node's type (1 - BitmapIndexedNode, 2 - ArrayNode),
   returns the corresponding index in the node-collector"
  [node]
  (- ^long (get-node-type node) 1))

(defn val-node-fetch [entity-id node-pointer]
  (if (instance? util/byte-array-class node-pointer)
    (let [blob (serializer/get-blob-id node-pointer)
          uuid (serializer/get-node-id node-pointer)
          node-chan (chan 1)]                               ;chan to received the node on
      (blob-unpacker/fetch-node-onto-chan blob entity-id uuid node-chan)
      ;wait
      (<!! node-chan))
    ;else return pointer directly
    node-pointer))


(defn n-assoc-dispatcher
  ([node-pointer shift hash key val added-leaf node-collector blob-id entity-id]
   ;(println "n-assoc-dispatcher, node-pointer" (serializer/byte-array-type-dispatch node-pointer))
   (if (instance? util/byte-array-class node-pointer)
     ;retrieve node from either in-memory blobs or go to disk, block until received (code executed within send-off on an agent - OK)
     (let [node-or-exception (let [blob (serializer/get-blob-id node-pointer)
                                   uuid (serializer/get-node-id node-pointer)
                                   node-chan (chan 1)]      ;chan to received the node on
                               (blob-unpacker/fetch-node-onto-chan blob entity-id uuid node-chan)
                               ;wait
                               (<!! node-chan))]
       (let [node (n-assoc node-or-exception shift hash key val added-leaf node-collector blob-id entity-id)]
         (when-not (identical? node node-or-exception)
           (add-to-node-collector node-collector node entity-id))
         ;return
         node))
     ;else call pointer directly
     (let [node (n-assoc node-pointer shift hash key val added-leaf node-collector blob-id entity-id)]
       ;(println-m "identical?" (identical? node node-pointer) (if-not (= (get-blob-id node) (get-blob-id node-pointer) blob-id) "WARNING" "") "REGULAR n-assoc-dispatcher (get-blob-id node):" (get-blob-id node) "(get-blob-id node-pointer):" (get-blob-id node-pointer) "blob-id:" blob-id )
       (when-not (identical? node node-pointer)
         (add-to-node-collector node-collector node entity-id))
       ;return
       node)))
  ([node-pointer edit shift hash key val added-leaf node-collector blob-id entity-id]
    (let [node (n-assoc node-pointer edit shift hash key val added-leaf node-collector blob-id entity-id)]
      ;always add edits to the node-collector since they are local only and actually mutate the node
      (add-to-node-collector node-collector node entity-id)
      ;return
      node)))

(defn n-without-dispatcher
  ([node-pointer shift hash key node-collector blob-id entity-id]
    (if (instance? util/byte-array-class node-pointer)
      (let [node-or-exception (let [blob (serializer/get-blob-id node-pointer)
                                    uuid (serializer/get-node-id node-pointer)
                                    node-chan (chan 1)]
                                (blob-unpacker/fetch-node-onto-chan blob entity-id uuid node-chan)
                                ;wait
                                (<!! node-chan))]
        (let [node (n-without node-or-exception shift hash key node-collector blob-id entity-id)]
          (when (and (not (nil? node)) (not (identical? node node-or-exception)))
            (add-to-node-collector node-collector node entity-id))
          ;return
          node))
      ;else call pointer directly
      (let [node (n-without node-pointer shift hash key node-collector blob-id entity-id)]
        (when (and (not (nil? node)) (not (identical? node node-pointer)))
          (add-to-node-collector node-collector node entity-id))
        ;return
        node))))

(defn n-find-dispatcher [node-pointer shift hash key not-found responce-ch entity-id]
  ;waits for the the incoming node to come out out of the channel
  (if (instance? util/byte-array-class node-pointer)
    (let [node-or-exception (let [blob (serializer/get-blob-id node-pointer)
                                  uuid (serializer/get-node-id node-pointer)
                                  node-chan (chan 1)]
                              (blob-unpacker/fetch-node-onto-chan blob entity-id uuid node-chan)
                              ;wait 10 sec
                              (let [[value _] (alts!! [node-chan (timeout 10000)])]
                                (if (= nil value)
                                  (do
                                    ;log the exception
                                    (>!! exception-log/incoming-exceptions (Exception. (str "persistent-durable-map/n-find-dispatcher could not find node within 10000 ms :blob " blob " :uuid " uuid)))
                                    (>!! responce-ch {:result nil :error {:message (str "timeout 10000ms")}}))
                                  value)))]
      (n-find node-or-exception shift hash key not-found responce-ch entity-id))
    ;else call pointer directly
    (n-find node-pointer shift hash key not-found responce-ch entity-id)))


(defn collect-key-vals [node responce-ch entity-id]
  (println-m "collect-key-vals start, node: " node)
  (cond (instance? ArrayNode node)
        (doseq [array-item (get-array node) :when (not= nil array-item)]
          (collect-key-vals-dispatcher array-item responce-ch entity-id))
        (or (instance? BitmapIndexedNode node) (instance? HashCollisionNode node))
        (let [^int i (volatile! 0)
              node-array (get-array node)]
          (doseq [array-item node-array :when (do (vswap! i unchecked-add-int 1) (odd? @i))]
            (cond
              (= nil array-item)
              (collect-key-vals-dispatcher (nth node-array @i) responce-ch entity-id)
              ;not a nil - collect key/val
              :else
              (let [i @i]
                (>!! responce-ch [array-item (get-value (val-node-fetch entity-id (nth node-array i)))])))))))



(defn collect-key-vals-dispatcher [node-pointer responce-ch entity-id]
  (if (instance? util/byte-array-class node-pointer)
    (let [node-or-exception (let [blob (serializer/get-blob-id node-pointer)
                                  uuid (serializer/get-node-id node-pointer)
                                  node-chan (chan 1)]
                              (blob-unpacker/fetch-node-onto-chan blob entity-id uuid node-chan)
                              ;wait 10 sec
                              (let [[value _] (alts!! [node-chan (timeout 10000)])]
                                (if (= nil value)
                                  (do
                                    ;log the exception
                                    (>!! exception-log/incoming-exceptions (Exception. (str "persistent-durable-map/collect-key-vals-dispatcher could not find node within 10000 ms :blob " blob " :uuid " uuid)))
                                    (>!! responce-ch {:result nil :error {:message (str "timeout 10000ms")}}))
                                  value)))]
      (collect-key-vals node-or-exception responce-ch entity-id))
    (collect-key-vals node-pointer responce-ch entity-id)))

;END FIND IO


(defn create-bitmap-indexed-node-from-proto [bitmap array uuid blob-identifier]
  (let [new-node (BitmapIndexedNode. nil (int bitmap) (to-array array) uuid blob-identifier)]
    new-node
    ))

(defn create-array-node-from-proto [cnt array uuid blob-identifier]
  (let [new-node (ArrayNode. nil (int cnt) (to-array array) uuid blob-identifier)]
    new-node))

(defn create-hash-collision-node-from-proto [node-hash cnt array uuid blob-identifier]
  (HashCollisionNode. nil (int node-hash) cnt (to-array array) uuid blob-identifier))

(defn create-val-node-from-proto [value uuid blob-identfier]
  (ValNode. value uuid blob-identfier))

;END PROTO BUFFER TYPES

(defn create-node
  ([shift key-1 val-1 key-2-hash key-2 val-2 node-collector blob-id entity-id]
   (let [key-1-hash (hash-code key-1)]
     (if (= key-1-hash key-2-hash)
       ;COLLISION
       (do
         ;manually add hash collision nodes to node-collector here
         (let [new-h-node (create-hash-collision-node nil key-1-hash 2 (object-array [key-1 val-1 key-2 (create-val-node val-2 node-collector blob-id entity-id)]) blob-id)]
           (add-to-node-collector node-collector new-h-node entity-id)
           new-h-node))
       ;NOT
       (let [added-leaf (new-box nil)
             edit (atomic-ref)]
         (let [return (-> (empty-bitmap-indexed-node blob-id)
                          (n-assoc-dispatcher edit shift key-1-hash key-1 (get-value (val-node-fetch entity-id val-1)) added-leaf node-collector blob-id entity-id)
                          (n-assoc-dispatcher edit shift key-2-hash key-2 val-2 added-leaf node-collector blob-id entity-id))]
           return)))))

  ([edit shift key-1 val-1 key-2-hash key-2 val-2 node-collector blob-id entity-id]
   (let [key-1-hash (hash-code key-1)]
     (if (= key-1-hash key-2-hash)
       ;COLLISION
       (do
         ;manually add hash collision nodes to node-collector here
         (let [new-h-node (create-hash-collision-node nil key-1-hash 2 (object-array [key-1 val-1 key-2 (create-val-node val-2 node-collector blob-id entity-id)]) blob-id)]
           (add-to-node-collector node-collector new-h-node entity-id)
           new-h-node))
       ;NOT
       (let [added-leaf (new-box nil)]
         (let [return (-> (empty-bitmap-indexed-node blob-id)
                          (n-assoc-dispatcher edit shift key-1-hash key-1 (get-value (val-node-fetch entity-id val-1)) added-leaf node-collector blob-id entity-id)
                          (n-assoc-dispatcher edit shift key-2-hash key-2 val-2 added-leaf node-collector blob-id entity-id))]
           return))))))

(defn create-hash-collision-node [edit hash cnt array blob-id]
  (let [uuid (state/dispense blob-id)
        new-h-node (HashCollisionNode. edit (int hash) cnt array uuid blob-id)]
    new-h-node))

(defn create-bitmap-indexed-node [edit bitmap array blob-id]
  (let [uuid (state/dispense blob-id)
        new-node (BitmapIndexedNode. edit (int bitmap) array uuid blob-id)]
    new-node))

(defn create-val-node [value node-collector blob-id entity-id]
  (println-m "val node value::" value)
  (comment)
  (if (instance? ValNode value)
    value
    ;else
    (let [uuid (state/dispense blob-id)
          new-node (ValNode. value uuid blob-id)]
      (add-to-node-collector node-collector new-node entity-id)
      new-node)))


(defn empty-bitmap-indexed-node [blob-id]
  (create-bitmap-indexed-node nil (int 0) (object-array 0) blob-id))

(defn remove-pair [^"[Ljava.lang.Object;" array i]
  (let [new-array (object-array (- (count array) 2))]
    (System/arraycopy array 0 new-array 0 (unchecked-multiply-int 2 i))
    (System/arraycopy array
                      (unchecked-multiply-int 2 (unchecked-add-int i 1))
                      new-array
                      (unchecked-multiply-int 2 i)
                      (unchecked-subtract-int (count new-array) (unchecked-multiply-int 2 i)))
    new-array))

(defn clone-and-set ^"[Ljava.lang.Object;"
  ([^"[Ljava.lang.Object;" array ^Integer i ^Object a]
   (let [^"[Ljava.lang.Object;" new-array (aclone array)]
     (aset new-array i a)
     new-array))

  ([^"[Ljava.lang.Object;" array ^Integer i ^Object a ^Integer j ^Object b]
   (let [^"[Ljava.lang.Object;" new-array (aclone array)]
     (aset new-array i a)
     (aset new-array j b)
     new-array)))


(defn create-array-node [edit ^Integer cnt array blob-id]
  (let [uuid (state/dispense blob-id)]
    (let [new-node (ArrayNode. edit cnt array uuid blob-id)]
      new-node)))


(defn add-to-node-collector [node-collector node entity-id]
  ;add to cache
  (blob-unpacker/add-to-strong-node-cache (get-blob-id node) entity-id (get-node-id node) node)
  (swap! node-collector (fn [old-state]
                          (-> old-state
                              (assoc-in [(get-node-collector-idx node) (get-node-id node)] node)))))

(defn node-equiv?
  "Compares a byte-array pointer to an in-memory pointer for equivalence"
  [p-1 p-2]
  (cond
    ;two byte-arrays
    (and (instance? util/byte-array-class p-1) (instance? util/byte-array-class p-2))
    (and (= (serializer/get-node-id p-1) (serializer/get-node-id p-2))
         (= (serializer/get-blob-id p-1) (serializer/get-blob-id p-2)))
    ;byte-array, in memory pointer
    (and (instance? util/byte-array-class p-1) (instance? durable-node-class p-2))
    (and (= (serializer/get-node-id p-1) (get-node-id p-2))
         (= (serializer/get-blob-id p-1) (get-blob-id p-2)))
    ;in memory pointer, byte-array
    (and (instance? durable-node-class p-1) (instance? util/byte-array-class p-2))
    (and (= (get-node-id p-1) (serializer/get-node-id p-2))
         (= (get-blob-id p-1) (serializer/get-blob-id p-2)))
    ;two in memory pointers
    (and (instance? durable-node-class p-1) (instance? durable-node-class p-2))
    (= p-1 p-2)
    ;either one is nil (n-without case)
    (or (nil? p-1) (nil? p-2))
    (= p-1 p-2)
    :else
    (do (>!! exception-log/incoming-exceptions (Exception. "unrecognized node types in node-equiv?"))
        false)))


(defn io-assoc
  "Main write function for maps. Accepts two arities, one allows override of timestamp during startup"
  ([entity-id key val confirm-ch confirm-ch-modifier-fn]
    (io-assoc entity-id key val confirm-ch confirm-ch-modifier-fn (SystemClock/getTime)))
  ([entity-id key val confirm-ch ^IFn confirm-ch-modifier-fn timestamp]
    (let [{:keys [blob-id node-collector-ch b-tree-leaf-node-ch pipeline-command-log-ch]} (node-keeper/blob-id-and-chan-dispenser)
          command-log-confirm-ch (chan 1)
          bpt-in-mem-confirm-ch (chan 1)
          ;command log params
          params [entity-id key val timestamp]
          ;write to the command log
          _ (command-log/write-to-command-log 1 "io-assoc" params command-log-confirm-ch pipeline-command-log-ch blob-id)
          ;send OK to API when command log write is flushed to disk AND in-memory b-tree is written
          _ (go
              ;wait for command log
              (<! command-log-confirm-ch)
              ;wait for io-assoc save-map!
              (<! bpt-in-mem-confirm-ch)
              ;send ok to the API
              (>! confirm-ch (confirm-ch-modifier-fn {:result {:timestamp (str timestamp)}})))
          ;initiate a new map if one doesn't already exist
          {:keys [cnt ^bytes root] :as io-map} (if-let [map-current (get-map-current! entity-id)]
                                                 ;map already exists, return it
                                                 map-current
                                                 ;initiate a new map
                                                 (new-io-map! entity-id blob-id))
          added-leaf (new-box nil)
          node-collector (new-node-collector)
          new-root (n-assoc-dispatcher root (int 0) (hash-code key) key val added-leaf node-collector blob-id entity-id)
          io-map (if (node-equiv? root new-root)
                   ;true
                   io-map
                   ;save in-memory
                   (do (save-map! entity-id (persistent-durable-map (if (= nil (get-box-val added-leaf)) cnt (+ ^long cnt 1)) new-root))
                       (get-map-current! entity-id)))]

      ;save to in-mem b-plus tree
      (bpt-sm/io-assoc entity-id timestamp io-map bpt-in-mem-confirm-ch)

      ;send to node-keeper
      (>!! node-collector-ch {:entity-id entity-id :node-collector @node-collector})
      (>!! b-tree-leaf-node-ch {:timestamp timestamp :io-map io-map :entity-info {:entity-id entity-id :entity-type 1}})
      {key val})))


(defn io-assoc-startup
  "Convenience arity of io-assoc called at server start, re-arranged arguments to allow easy (apply f params)"
  [entity-id key val timestamp confirm-ch]
  (io-assoc entity-id key val confirm-ch identity timestamp))


(defn io-without
  "Main delete method for maps. Accepts two arrities, one allows override of timestamp during startup"
  ([entity-id key confirm-ch confirm-ch-modifier]
    (io-without entity-id key confirm-ch confirm-ch-modifier (SystemClock/getTime)))
  ([entity-id key confirm-ch confirm-ch-modifier timestamp]
    (let [{:keys [cnt root] :as io-map} (get-map-current! entity-id)]
      ;proceed if map exists and map is bigger than 0
      (if (and (not= nil root) (< 0 ^long cnt))
        (let [{:keys [blob-id node-collector-ch b-tree-leaf-node-ch pipeline-command-log-ch]} (node-keeper/blob-id-and-chan-dispenser)
              command-log-confirm-ch (chan 1)
              bpt-in-mem-confirm-ch (chan 1)
              ;command log params
              params [entity-id key timestamp]
              ;write to command log
              _ (command-log/write-to-command-log 1 "io-without" params command-log-confirm-ch pipeline-command-log-ch blob-id)
              _ (go
                  ;wait for command log
                  (<! command-log-confirm-ch)
                  ;wait for io-assoc save-map!
                  (<! bpt-in-mem-confirm-ch)
                  ;send ok to the API
                  (>! confirm-ch (confirm-ch-modifier {:result {:timestamp (str timestamp)}})))
              node-collector (new-node-collector)
              new-root (n-without-dispatcher root (int 0) (hash key) key node-collector blob-id entity-id)
              io-map (if (node-equiv? root new-root)
                       ;true
                       io-map
                       ;save in-memory
                       (do (save-map! entity-id (persistent-durable-map (- ^long cnt 1) new-root))
                           (get-map-current! entity-id)))]

          ;save to in-mem b-plus tree
          (bpt-sm/io-assoc entity-id timestamp io-map bpt-in-mem-confirm-ch)

          ;send to node-keeper
          (>!! node-collector-ch {:entity-id entity-id :node-collector @node-collector})
          (>!! b-tree-leaf-node-ch {:timestamp timestamp :io-map io-map :entity-info {:entity-id entity-id :entity-type 1}}))
        ;else - either entity doesn't exist, or the count is 0, confirm directly
        (>!! confirm-ch (confirm-ch-modifier {:timestamp timestamp}))))))

(defn io-without-startup
  "Convenience arity of io-assoc called at server start, re-arranged arguments to allow easy (apply f params)"
  [entity-id key timestamp confirm-ch]
  (io-without entity-id key confirm-ch identity timestamp))

(defn dissoc-in-vector-or-map
  "Dissociates an entry from a nested associative structure returning a new
  nested structure. keys is a sequence of keys. Any empty maps that result
  will not be present in the new structure."
  [m [k & ks :as keys]]
  (if ks
    (if-let [nextmap (get m k)]
      (let [newmap (dissoc-in nextmap ks)]
        (if (seq newmap)
          (assoc m k newmap)
          (dissoc m k)))
      m)
    (dissoc m k)))

(defn io-assoc-in-json [entity-id key deep-key old-val new-val confirm-ch confirm-ch-modifier]
  (let [new-val' (try (assoc-in old-val deep-key new-val)
                      (catch Exception e e))]
    (if (instance? Exception new-val')
      (>!! confirm-ch {:error "Index out of bounds during io-assoc-in-json"})
      (io-assoc entity-id key new-val' confirm-ch confirm-ch-modifier))))

(defn io-dissoc-in-json [entity-id key deep-key old-val confirm-ch confirm-ch-modifier]
  (let [new-val' (try (dissoc-in old-val deep-key)
                      (catch Exception e e))]
    (if (instance? Exception new-val')
      (>!! confirm-ch {:error "Invalid io-dissoc-in-json operation"})
      (io-assoc entity-id key new-val' confirm-ch confirm-ch-modifier))))


;ARRAY ITEM CONVERSIONS
;----------------------
(defn- node->node-tuple-byte-array
  "Converts in-memory pointer to a Protocol Buffer tuple"
  [^neversleep_db.persistent_durable_map_protocol.DurableNode node]
  (serializer/node-pointer-to-typed-byte-array (get-node-id node) (get-blob-id node)))


(defn- array-item->byte-string
  "Converts each element (either tuple or as-is object) in a node's array to byte string"
  [x]
  (-> (cond
        ;if instance of ArrayNode or BitmapIndexed or ValNode node, convert to tuple
        (or (instance? ArrayNode x) (instance? BitmapIndexedNode x) (instance? HashCollisionNode x) (instance? ValNode x))
        (node->node-tuple-byte-array x)
        ;if already a byte-array (from a node loaded from disk)
        (instance? util/byte-array-class x)
        x
        ;else serialize object using nippy - either key (java.lang.String) or nil
        :else
        (serializer/nippy-byte-array-to-typed-byte-array (util/serialize x)))
      (util/copy-to-byte-string)))
;--------------------------
;END ARRAY ITEM CONVERSIONS

(defn- node-array->repeatable-bytes
  "Takes a node's array and maps to-byte-string to each item"
  [^"[Ljava.lang.Object;" array]
  (map array-item->byte-string array))
;end to-protobuf helpers

;to-protobuf functions
;---------------------
(defn- bitmap-indexed-node->proto
  "Take a bitmap indexed node and returns a protocol buffer"
  [node]
  (protobuf BitmapIndexedNodeProtoDef
                            :bitmap (get-bitmap node)
                            :array (node-array->repeatable-bytes (get-array node))
                            :uuid (get-node-id node)))


(defn- array-node->proto
  "Take an array node and returns a protocol buffer representation"
  [node]
  (protobuf ArrayNodeProtoDef
                            :cnt (get-count node)
                            :array (node-array->repeatable-bytes (get-array node))
                            :uuid (get-node-id node)))


(defn- hash-collision-node->proto
  "Take a hash collision node and returns a protocol buffer representation"
  [node]
  (protobuf HashCollisionNodeProtoDef
            :nodehash (get-hash node)
            :cnt (get-count node)
            :array (node-array->repeatable-bytes (get-array node))
            :uuid (get-node-id node)))

(defn- val-node->proto
  [node]
  (protobuf ValNodeProtoDef
            :value (util/copy-to-byte-string (util/serialize (get-value node)))
            :uuid (get-node-id node)))
;-------------------------
;end to-protobuf functions



(defn init []
  (blob-unpacker/register-type-dispatch-fn-2 pdm-type-dispatch))


