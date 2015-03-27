(ns neversleep-db.read-dispatch
  (:require [neversleep-db.persistent-durable-map :as pdm]
            [neversleep-db.b-plus-tree-batch :as bpt-b]
            [neversleep-db.b-plus-tree-sorted-map :as bpt-sm]
            [neversleep-db.println-m :refer [println-m]]
            [neversleep-db.state :as state]
            [flatland.protobuf.core :refer :all]
            [clojure.core.async :refer [chan go >! <! <!! >!! go-loop put! thread alts! alts!! timeout pipeline pipeline-blocking close! dropping-buffer]]
            [neversleep-db.util :as util :refer [<!!-till-exception]]
            [neversleep-db.time :as time]
            [neversleep-db.constants :as constants])
  (:import (clojure.lang APersistentMap)
           (jv SystemClock)
           (com.google.protobuf ByteString)))


(defn convert-root [{:keys [root cnt] :as a-map}]
  (if (instance? APersistentMap a-map)
    {:cnt cnt :root (if (instance? ByteString root) (util/byte-string-to-byte-array root) root)}))

(defn- get-root-from-ch-result [result]
  (-> result
      :result
      (first)
      (get 1)))

(defn- get-root-as-of-on-disk [entity-id ^long timestamp]
  (let [result-chan (chan 1)]
    (bpt-b/io-find-range entity-id timestamp 0 1 result-chan)
    (let [data (get-root-from-ch-result (<!! result-chan))]
      ;after server restart data is a map, before that it's a byte array
      (convert-root data))))



(defn process-read-request [io-fn]
  ;execute read request
  (io-fn))

(defn start-read-pipeline []
  (let [from-ch (chan 1)]
    (pipeline-blocking constants/read-pipeline-size (chan (dropping-buffer 0)) (map process-read-request) from-ch)
    (reset! state/read-pipeline-from-ch from-ch)))




(defn- get-root-as-of
  "Fetches entity's root as-of timestamp by looking it up in the index b-trees,
   either the in-memory or on-disk one
   You can visualize an index for an entity like this:
   ---index-on-disk--->|--->index-in-memory--->"
  [entity-id ^long timestamp]
  (let [responce-ch (chan 1)
        _ (bpt-sm/io-find-range entity-id timestamp 0 1 responce-ch)
        result (<!! responce-ch)]
    ;if found return the in-memory result
    (if-let [root (get-root-from-ch-result result)]
      ;found in-mem
      (do (println-m "in-mem")
          root)
      ;else, search on disk
      (do (println-m "on disk")
          (get-root-as-of-on-disk entity-id timestamp)))))

(defn io-find-as-of!
  "Searches a map given:
  id - the name of the m2ap
  key - the key in the map of key/value pairs
  timestamp"
  ([entity-id key timestamp]
    (io-find-as-of! entity-id key timestamp (chan 1) identity))
  ([entity-id key timestamp responce-ch responce-modifier-fn]
   (if-let [{:keys [root cnt] :as io-map} (get-root-as-of entity-id timestamp)]
     (let [find-ch (chan 1)]
       (pdm/n-find-dispatcher root (int 0) (pdm/hash-code key) key nil find-ch entity-id)
       (>!! responce-ch (responce-modifier-fn (<!! find-ch))))
     (>!! responce-ch (responce-modifier-fn {:result nil})))
   responce-ch))



(defn io-get-entity-as-of!
  "Fetches a whole entity as of timestmap"
  ([entity-id timestamp]
    (io-get-entity-as-of! entity-id timestamp (chan 1)))
  ([entity-id timestamp responce-ch]
   (io-get-entity-as-of! entity-id timestamp responce-ch identity))
  ([entity-id timestamp responce-ch responce-modifier-fn]
   (if-let [{:keys [root cnt] :as io-map} (get-root-as-of entity-id timestamp)]
     ;if entity exists as of this point in time
     (if (= cnt 0)
       ;if count is zero
       (>!! responce-ch (responce-modifier-fn {:result {}}))
       ;else, search the tree and collect key/vals
       (let [partition-ch (chan 1 (partition-all cnt))]
         (pdm/collect-key-vals-dispatcher root partition-ch entity-id)
         (>!! responce-ch (responce-modifier-fn {:result (into {} (<!! partition-ch))}))))
     ;else, no entity as of this point, return nil
     (>!! responce-ch (responce-modifier-fn {:result nil})))
    responce-ch))


(defn- io-get-entity-helper
  [entity-id {:keys [cnt root] :as io-map}]
  (if (and (instance? APersistentMap io-map) (instance? util/byte-array-class root) (number? cnt))
    (if (= cnt 0)
      ;if count is zero
      {}
      ;else, search the tree and collect key/vals
      (let [partition-ch (chan 1 (partition-all cnt))]
        (pdm/collect-key-vals-dispatcher root partition-ch entity-id)
        (into {} (<!! partition-ch))))
    (throw (Exception. "wrong params to io-get-entity-helper"))))

(defn io-get-all-versions-between!
  ([entity-id timestamp-start timestamp-end limit]
    (io-get-all-versions-between! entity-id timestamp-start timestamp-end limit (chan 1) identity))
  ([entity-id timestamp-start timestamp-end limit responce-ch responce-modifier-fn]
   (let [result-xf (comp (map (fn [[timestamp root]] [(str timestamp) (->> root
                                                                           (convert-root)
                                                                           (io-get-entity-helper entity-id))]))
                         (util/dedupe-xf second))
         in-mem-ch (chan 1)
         on-disk-ch (chan 1)
         ;fetch in-mem results
         _ (bpt-sm/io-find-range entity-id timestamp-start timestamp-end limit in-mem-ch)
         in-mem-results (:result (<!!-till-exception in-mem-ch))
         in-mem-results-count (count in-mem-results)
         ^long last-in-mem-timestamp (let [[timestamp #_last-in-mem-root] (peek in-mem-results)] timestamp)
         result (if (not (nil? last-in-mem-timestamp))
                  ;if the number of wanted items is bigger than the number of found results, and we haven't reached the timestamp-end , keep searching on disk
                  (if (and (< in-mem-results-count limit) (< timestamp-end last-in-mem-timestamp))
                    ;dec the last-in-mem-timestamp by 1 (avoid overlap), reduce the limit by the number of already found results
                    (let [_ (bpt-b/io-find-range entity-id (dec last-in-mem-timestamp) timestamp-end (- limit in-mem-results-count) on-disk-ch)
                          on-disk-results (:result (<!!-till-exception on-disk-ch))]
                      ;combine the results
                      (do
                        ;in mem + on disk
                        (concat in-mem-results on-disk-results)))
                    ;else - done, return
                    (do
                      ;in mem
                      in-mem-results))
                  ;else, search on disk directly
                  (do
                    ;on disk
                    (bpt-b/io-find-range entity-id timestamp-start timestamp-end limit on-disk-ch)
                    (:result (<!!-till-exception on-disk-ch))))
         ;if still no results found, one last try to fetch one version
         one-result (if (= 0 (count result))
                      [(str timestamp-start) (:result (<!!-till-exception (io-get-entity-as-of! entity-id timestamp-start)))])]
     (>!! responce-ch
          (responce-modifier-fn {:result (if one-result
                                           ;return one result
                                           [one-result]
                                           ;else transform the collected roots
                                           (into [] result-xf result))}))
     responce-ch)))

(defn init []
  (start-read-pipeline))
