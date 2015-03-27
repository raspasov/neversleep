(ns neversleep-db.blob-unpacker
  (:require [flatland.protobuf.core :refer :all]
            [neversleep-db.to-disk :as to-disk]
            [clojure.core.cache :as cc]
            [neversleep-db.println-m :refer [println-m]]
            [clojure.core.async :refer [chan go >! <! <!! >!! go-loop put! thread alts! alts!! timeout pipeline close!]]
            [clojure.core.incubator :refer [dissoc-in]]
            [neversleep-db.util :as util]
            [neversleep-db.state :as state]
            [neversleep-db.io-utils :as io-utils]
            [neversleep-db.serializer :as serializer])
  (:import com.durablenode.Durablenode$Blob
           com.durablenode.Durablenode$NodeTuple
           com.durablenode.Durablenode$MapRoot
           (clojure.lang Keyword IFn)
           (com.google.protobuf ByteString)))


(declare end-blob-unpack)


;Initialized at server start by their respective namespaces
(defonce bpt-b-type-dispatch nil)
(defonce pdm-type-dispatch nil)


;called once at server start
(defn register-type-dispatch-fn-1 [^IFn f]
  (alter-var-root #'neversleep-db.blob-unpacker/bpt-b-type-dispatch (constantly f)))
(defn register-type-dispatch-fn-2 [^IFn f]
  (alter-var-root #'neversleep-db.blob-unpacker/pdm-type-dispatch (constantly f)))

(def byte-array-class (class (byte-array [])))

(def BlobProtoDef (protodef Durablenode$Blob))
(def MapRootProtoDef (protodef Durablenode$MapRoot))


(defn clear-strong-node-cache [blob-id entity-id]
  (swap! state/strong-node-cache dissoc-in [(keyword entity-id) (keyword (str blob-id))]))

;SOFT CACHES
;==============================================
;LRU
;(defn add-to-soft-node-cache [blob-id uuid node]
;  (let [blob-id-uuid-kw (keyword (str blob-id "|" uuid))]
;    (swap! state/soft-node-cache (fn [old-state] (if (cc/has? old-state blob-id-uuid-kw)
;                                                   (cc/hit old-state blob-id-uuid-kw)
;                                                   (cc/miss old-state blob-id-uuid-kw node))))))
;SOFT REFERENCES
(defn add-to-soft-node-cache [blob-id uuid node]
  (let [blob-id-uuid-kw (keyword (str blob-id "|" uuid))]
    (if (cc/has? state/soft-node-cache blob-id-uuid-kw)
      (cc/hit state/soft-node-cache blob-id-uuid-kw)
      (cc/miss state/soft-node-cache blob-id-uuid-kw node))))

;LRU
;(defn get-from-soft-node-cache [blob-id-uuid]
;  (get @state/soft-node-cache (keyword blob-id-uuid)))
;SOFT REFERENCES
(defn get-from-soft-node-cache [blob-id-uuid]
  (get state/soft-node-cache (keyword blob-id-uuid)))


;STRONG CACHES
;==============================================
(defn add-to-strong-node-cache [blob-id entity-id uuid node]
  ;add to the soft cache first
  (add-to-soft-node-cache blob-id uuid node)
  ;add to strong cache
  (swap! state/strong-node-cache assoc-in [(keyword entity-id) (keyword (str blob-id)) (keyword (str uuid))] node))

(defn get-from-strong-node-cache [blob-id entity-id uuid]
  (get-in @state/strong-node-cache [(keyword entity-id) (keyword (str blob-id)) (keyword (str uuid))]))
;====

(defn byte-array->blob->nodes [byte-array-data]
  (protobuf-load BlobProtoDef byte-array-data))

(defn input-stream->blob->nodes [in]
  (protobuf-load-stream BlobProtoDef in))


(defn b-tree-leaf-node-byte-string->array-item
  "No need for byte-array magic here"
  [idx ^ByteString byte-string]
  (let [b-a (-> byte-string (.toByteArray))]
    (if (even? idx)
      ;key
      (util/de-serialize b-a)
      ;else, val
      (protobuf-load MapRootProtoDef b-a))))

(defn b-tree-inner-node-byte-string->array-item
  "No need for byte-array magic here"
  [idx ^ByteString byte-string]
  (let [b-a (-> byte-string (.toByteArray))]
    (if (odd? idx)
      ;convert (keys)
      (util/de-serialize b-a)
      ;leave as byte-array (pointers)
      b-a)))

(defn byte-string->array-item
  "Converts an array item taken from disk back to format that n-assoc understands"
  [^ByteString byte-string]
  (let [b-a (-> byte-string (.toByteArray))
        [type b-a-data] (serializer/byte-array-type-dispatch b-a)]
    (condp = type
      ;leave as byte-array (node pointers)
      (byte 1) b-a
      ;nippy data
      (byte 2) (util/de-serialize b-a-data))))

(def blobs-in-progress (atom {}))

(defn determine-dispatch-fn-and-type [node-proto-type]
  (println-m "determine, node-proto-type" node-proto-type)
  (condp = node-proto-type
    :pdmbitmapnodes [pdm-type-dispatch 1]
    :pdmarraynodes [pdm-type-dispatch 2]
    :pdmcollisionnodes [pdm-type-dispatch 3]
    :pdmvalnodes [pdm-type-dispatch 4]
    :btreeleafnodes [bpt-b-type-dispatch 1]
    :btreeinnernodes [bpt-b-type-dispatch 2]
    (throw (Exception. "no mathing determine-dispatch-fn-and-type"))))

(defn unpack-blob!
  "Fetches a blob from durable storage and unpacks it into the in-memory node cache atom
   As soon as the node-wanted is found, it returns it onto the node-wanted-ch to whoever requested that node"
  [blob-id entity-id uuid-wanted node-wanted-ch]
  (println-m "unpacking blob-id" blob-id "|" entity-id "uuid-wanted" uuid-wanted)
  (let [blob-id-entity-id (str blob-id "|" entity-id)
        blob (byte-array->blob->nodes (io-utils/blocking-loop to-disk/get-blob blob-id entity-id))
        internal-index (-> blob ^ByteString (:index) (.toByteArray) (util/de-serialize))
        ;_ (println-m "internal-index::" internal-index )
        [^Keyword node-proto-type ^long nth-i] (get internal-index uuid-wanted)]
    ;make the state available to the outside world
    (swap! blobs-in-progress assoc blob-id-entity-id {:blob blob :internal-index internal-index})
    ;send wanted node on the node-wanted-ch
    (let [[^IFn type-dispatch-fn ^long node-dispatch-type] (determine-dispatch-fn-and-type node-proto-type)]
      (>!! node-wanted-ch (type-dispatch-fn (get-in blob [node-proto-type nth-i]) blob-id node-dispatch-type)))
    ;unpack the whole blob
    (future)
    (do (doseq [[^Keyword node-proto-type nodes] blob :when (not= :index node-proto-type)]
          ;determine the dispatch function and node dispatch type (a number corresponding to (get-node-type))
          (let [[^IFn type-dispatch-fn ^long node-dispatch-type] (determine-dispatch-fn-and-type node-proto-type)]
            (do
              ;(println-m "node-type" node-type)
              (doseq [{:keys [uuid] :as one-node} nodes]
                (add-to-soft-node-cache blob-id uuid (type-dispatch-fn one-node blob-id node-dispatch-type))))))
        ;remove from blobs-in-progress
        (end-blob-unpack blob-id-entity-id))))


(defn wait-for-blob [blob-id entity-id uuid-wanted node-wanted-ch]
  (let [blob-id-entity-id (str blob-id "|" entity-id)
        blob-id-uuid (keyword (str blob-id "|" uuid-wanted))]
    (go (loop [blob-wrap (get @blobs-in-progress blob-id-entity-id)
               blob-in-cache (get-from-soft-node-cache blob-id-uuid)
               timeout-start (System/nanoTime)]
          (println-m "looking for" blob-id-uuid)
          (cond
            ;found the blob-wrap
            (not= nil blob-wrap)
            (let [{:keys [blob internal-index]} blob-wrap
                  [^Keyword node-proto-type ^long nth-i] (get internal-index uuid-wanted)]
              ;return the node to whoever needed it
              (let [[^IFn type-dispatch-fn ^long node-dispatch-type] (determine-dispatch-fn-and-type node-proto-type)]
                (>! node-wanted-ch (type-dispatch-fn (get-in blob [node-proto-type nth-i]) blob-id node-dispatch-type))))
            ;found the blob in cache
            (not= nil blob-in-cache)
            (>! node-wanted-ch blob-in-cache)
            ;searched enough, stop
            (< 10000000000 ^long (- (System/nanoTime) ^long timeout-start))
            (>! node-wanted-ch (Exception. (str "couldn't find blob-id-entity-id:" blob-id-entity-id ", uuid: " uuid-wanted " within 10 sec")))
            :else
            ;keep searching
            (recur (get @blobs-in-progress blob-id-entity-id) (get-from-soft-node-cache blob-id-uuid) timeout-start))))))

(def unpack-coordinator (agent {}))

(defn end-blob-unpack [blob-id-entity-id]
  (send unpack-coordinator (fn [old-state]
                             (let [new-state (dissoc old-state blob-id-entity-id)]
                               new-state))))

(defn request-blob-unpack [blob-id entity-id uuid-wanted node-wanted-ch]
  (let [blob-id-entity-id (str blob-id "|" entity-id)]
    (send unpack-coordinator (fn [old-state]
                                   (println-m "starting unpack-coordinator")
                                   (if (= true (get old-state blob-id-entity-id))
                                     ;true - unpack for that blob is in progress, wait for blob
                                     (do
                                       (wait-for-blob blob-id entity-id uuid-wanted node-wanted-ch)
                                       old-state)
                                     ;else - set the unpack bool to true for that blob, start the unpack
                                     (let [new-state (assoc old-state blob-id-entity-id true)]
                                       (add-watch unpack-coordinator blob-id-entity-id (fn [k r old-state new-state]
                                                                                         (if (= nil (get new-state k))
                                                                                           ;gc temp blob storage
                                                                                           (swap! blobs-in-progress dissoc blob-id-entity-id))))
                                       (future
                                         (unpack-blob! blob-id entity-id uuid-wanted node-wanted-ch))
                                       new-state))))))

;main public method
(defn fetch-node-onto-chan
  "Tries to fetch the node from the node-cache atom,
   if not found starts a request-blob-unpack call to fetch from durable storage"
  [blob-id entity-id uuid node-ch]
  (let [^Keyword blob-id-uuid (keyword (str blob-id "|" uuid))
        node-in-cache (get-from-soft-node-cache blob-id-uuid)]
    (cond
      (= nil node-in-cache)
      ;node not found in the soft cache, check strong cache
      (let [node-in-cache (get-from-strong-node-cache blob-id entity-id uuid)]
        (if (= nil node-in-cache)
          ;node NOT found in strong cache, request blob unpack
          (let [node-wanted-ch (chan 1)]
            (request-blob-unpack blob-id entity-id uuid node-wanted-ch)
            (go (let [node (<! node-wanted-ch)]
                  (>! node-ch node))))
          ;else, node is FOUND in strong cache, return directly
          (>!! node-ch node-in-cache)))
      ;otherwise return type directly, no wait
      :else
      (>!! node-ch node-in-cache))))
