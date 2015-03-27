(ns neversleep-db.to-disk-mem
  (:require [taoensso.nippy :as nippy]
            [neversleep-db.mysql-lib :as mysql-lib]
            [neversleep-db.println-m :refer [println-m]]
            [qbits.nippy-lz4 :refer [lz4-compressor lz4hc-compressor]]
            [neversleep-db.util :as util]))

;save-node [uuid data]
;get-node [uuid]
;DB TABLE
;nodes
;=============
;uuid | data
;
;roots
;id | uuid | timestamp | data

;DATA JUGGLING
(defn serialize [data]
  ;(println-m "data before serialize: " data)

  (let [frozen-data (nippy/freeze data
                                  ;{:compressor lz4hc-compressor}
                                  {:skip-header? true})]
    ; (println-m "frozen data: " frozen-data)
    frozen-data
    )
  )

(defn de-serialize [blob]
  (if blob
    (nippy/thaw blob
                ;{:compressor lz4-compressor}
                {:skip-header? true}
                )
    nil))

(defn save-node
  "Saves a node to durable storage"
  [uuid data]
  (mysql-lib/execute! ["INSERT INTO `nodes` (`uuid`, `data`) VALUES (?, ?);"
                       uuid (util/serialize data)]))


(defn get-node
  "Gets a node from durable storage"
  [uuid]
  (-> (mysql-lib/query ["SELECT `data` FROM `nodes` WHERE `uuid` = ? LIMIT 1;" uuid])
      (first)
      (:data nil)
      (util/de-serialize)))

(defn save-root
  "Saves a root to durable storage"
  [id data]
  (mysql-lib/execute! ["INSERT INTO `roots` (`id`, `data`, `timestamp`) VALUES (?, ?, CURRENT_TIMESTAMP);"
                       id (util/serialize data)]))

(defn get-root [id]
  (-> (mysql-lib/query ["SELECT * FROM `roots` WHERE `id` = ? ORDER BY `auto-inc` DESC;"
                        id])
      (first)
      (:data nil)
      (util/de-serialize)
      ))

(defn get-root-as-of [id timestamp]
  )

;DURABLE MAPS
;=========================

;in-memory storage
(def nodes-maps-table (atom {}))
(def roots-maps-table (atom {}))

(def allocate-id-counter (atom 0))

(defn allocate-id
  "Allocates a row in the db for the incoming blob of nodes"
  []
  (println-m "allocate-id MEMORY")
  (swap! allocate-id-counter + 1))


(defn save-blob [blob id]
  (println-m "saving to MySQL (mem) blob-id" id)
  ;(mysql-lib/execute! ["UPDATE `nodes-maps` SET `data` = ? WHERE `id` = ?;" blob id])
  (swap! nodes-maps-table (fn [old-state] (assoc old-state id blob))))

(defn get-blob [id]
  (get @nodes-maps-table id nil))

(defn save-root-maps
  "Saves a root to durable storage"
  [data id]
  (swap! roots-maps-table (fn [old-state] (assoc old-state id (conj (get old-state id []) data)))))


(defn get-root-maps
  "WARNING: experimental, do not use (non optimized query)"
  [id]
  (-> (get @roots-maps-table id nil)
      (peek)))

(defn get-root-maps-as-of
  "WARNING: experimental, do not use (non optimized query)"
  [id db-id]
  (-> (get @roots-maps-table id nil)
      (nth (- db-id 1))))

(defn truncate-all-tables []
  (reset! roots-maps-table {})
  (reset! nodes-maps-table {})
  (reset! allocate-id-counter 0)
  true)
