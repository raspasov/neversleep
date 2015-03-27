(ns neversleep-db.state
  (:require [clojure.core.async :refer [chan go >! <! <!! >!! go-loop put! thread alts! alts!! timeout pipeline close!]]
            [clojure.core.cache :as cc]))


;tcp server
(def tcp-server (atom nil))

;read pipeline from channel
(def read-pipeline-from-ch (atom nil))

;holds mysql connect
(def mysql-connection-pool (atom nil))

;neversleep-db.persistent-durable-map
;=======================================
;hold all map roots {"entity-id" MapRoot, etc}
(def all-maps (atom {}))


;neversleep-db.b-plus-tree-batch
;=======================================
;tree states
;{:bpt-id-1 root-1, :bpt-id-2 root-2}
(def all-b-plus-trees-on-disk (atom {}))

;tree states
;{:bpt-id-1 root-1, :bpt-id-2 root-2}
(def all-b-plus-trees-in-mem (ref {}))
;GC

;(def cache-factory (cc/lru-cache-factory {} :threshold 250000))
(def cache-factory (cc/soft-cache-factory {}))
;looks like this {:blob-id|uuid a-node, etc}
(def soft-node-cache cache-factory)

;looks like this {:entity-id {:blob-id {:uuid "node"}}}
(def strong-node-cache (atom {}))

(def blob-id-ticket-dispenser (chan 50000))
;GC
(defn reset-ticket-dispenser []
  (def blob-id-ticket-dispenser (chan 50000)))

;looks like this {blob-id [chan-1 chan-2 chan-3], etc}
(def blob-channels (atom {}))
;GC
(defn garbage-collect-blob-channels [blob-id]
  (swap! blob-channels dissoc blob-id))

;holds a map like this {entity-id a-chan, etc}
(def write-chans (atom {}))

(defn reset-state! []
  (reset! all-maps {})
  (reset! all-b-plus-trees-on-disk {})
  (dosync (ref-set all-b-plus-trees-in-mem {}))
  ;for Soft References cache
  (def soft-node-cache (cc/soft-cache-factory {}))
  ;for LRU cache
  ;(reset! soft-node-cache (cc/lru-cache-factory {} :threshold 250000))
  (reset! strong-node-cache {})
  (reset-ticket-dispenser))

;keeps track of indexing within one blob
;data looks like this {blob-id-1 0, blob-id-2 1, etc}
(def blob-indexes (atom {}))

(defn garbage-collect-blob-indexes [blob-id]
  (swap! blob-indexes dissoc blob-id))

(defn- inc-blob-index
  "Increments the index for a blob-id and returns a full blob-indexes map as a result of the swap!"
  [blob-id]
  (swap! blob-indexes (fn [old-state]
                        (assoc old-state blob-id (inc (get old-state blob-id 0))))))

(defn dispense
  "Returns an index for a node in the current blob of nodes, called at Node creation time"
  [blob-id]
  (get (inc-blob-index blob-id) blob-id))

;holds blob-ids of master root logs to be gc-ed
(def gc-master-root-logs (atom []))

