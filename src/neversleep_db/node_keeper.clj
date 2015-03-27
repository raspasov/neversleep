(ns neversleep-db.node-keeper
  (:require [neversleep-db.persistent-durable-map-protocol :as pdm-p]
            [neversleep-db.b-plus-tree-protocol :as bpt-b-p]
            [neversleep-db.b-plus-tree-batch :as bpt-b]
            [neversleep-db.b-plus-tree-sorted-map :as bpt-sm]
            [neversleep-db.b-plus-tree-root-log :as bpt-root-log]
            [neversleep-db.b-plus-tree-root-log-remote :as bpt-root-log-remote]
            [neversleep-db.command-log :as command-log]
            [neversleep-db.println-m :refer [println-m]]
            [clojure.core.async :refer [chan timeout go <! <!! >!! >! go-loop put! thread alts!! alts! dropping-buffer pipeline-blocking pipeline-async]]
            [neversleep-db.to-disk :as to-disk]
            [clojure.java.io :refer [output-stream input-stream delete-file]]
            [aprint.core :refer [aprint]]
            [neversleep-db.blob-unpacker :as blob-unpacker]
            [flatland.protobuf.core :refer :all]
            [neversleep-db.constants :as constants]
            [neversleep-db.util :as util]
            [neversleep-db.state :as state]
            [neversleep-db.io-utils :as io-utils])
  (:import (com.google.protobuf ByteString)
           com.durablenode.Durablenode$Blob
           com.durablenode.Durablenode$BtreeRootLogEntry
           com.durablenode.Durablenode$BtreeRoot
           com.durablenode.Durablenode$MapRoot
           (clojure.lang APersistentVector Keyword APersistentMap MapEntry)))


(declare start-blob-collector)

;Protocol Buffer definitions
(def BlobProtoDef (protodef Durablenode$Blob))
(def BtreeRootLogEntryProtoDef (protodef Durablenode$BtreeRootLogEntry))
(def BtreeRootProtoDef (protodef Durablenode$BtreeRoot))
(def MapRootProtoDef (protodef Durablenode$MapRoot))


;b-tree transforms
(defn bpt-b-node-collector-xf
  "This transform collects the newly generated nodes from an io-assoc on a b-tree for saving to blob"
  [^long partition-size]
  (comp
    (map (fn [{:keys [node-collector] :as a-map}]
           (assoc a-map :node-collector (for [node-type node-collector]
                                          (map (fn [[_ v]] (bpt-b-p/to-protobuf v)) node-type)))))
    (partition-all partition-size)))

(defn bpt-b-root-xf
  "This transforms handles the B-tree roots and the last entry in the key-vals that was written to the b-tree"
  [^long partition-size]
  (comp
    (map (fn [{:keys [io-b-tree last-entity-root entity-info]}]
           ;returns a protobuf
           (protobuf BtreeRootLogEntryProtoDef
                     :root (protobuf BtreeRootProtoDef
                                     :root (util/copy-to-byte-string (:root io-b-tree))
                                     :cnt (:cnt io-b-tree))
                     :lastentityroot last-entity-root
                     :entity (:entity-id entity-info)
                     :entitytype (:entity-type entity-info))))
    (partition-all partition-size)))
;end b-tree transforms

;persistent durable map transforms
(defn pdm-node-collector-xf
  "This transform collects the newly generated nodes from an io-assoc on a PDM for saving to blob"
  [^long partition-size]
  (comp
    (map (fn [{:keys [node-collector] :as a-map}]
           (assoc a-map :node-collector (for [^APersistentMap node-type ^APersistentVector node-collector]
                                          (map (fn [[_ v :as ^MapEntry data]]
                                                 (pdm-p/to-protobuf v)) node-type)))))
    (partition-all partition-size)))

(defn b-tree-leaf-node-xf
  "This transform is used to build the LeafNode of the b-tree"
  [^long partition-size]
  (comp
    (map (fn [{:keys [io-map] :as a-map}]
           ;return a new map with root replaced with a remote pointer
           (assoc a-map :io-map (protobuf MapRootProtoDef
                                          :cnt (:cnt io-map)
                                          :root (util/copy-to-byte-string (:root io-map))))))
    (partition-all partition-size)))
;end persistent durable map transforms

(defn get-blob-channels [blob-id]
  (get @state/blob-channels blob-id))


(defn add-blob-channel
  "Allocates a new blob channel which will collect all the pending returns of node groups
  as well as the key-vals for the B-tree LeafNode

  Parameters:
  blob-id: the blob id for the current operation"
  [blob-id blob-size]
  (swap! state/blob-channels assoc blob-id [(chan blob-size (pdm-node-collector-xf blob-size)) ;node collector
                                            (chan blob-size (b-tree-leaf-node-xf blob-size)) ;btree leaf node
                                            (chan 1)        ;pipeline
                                            ]))

;DATA
;===========================

(defn blob-id-and-chan-dispenser
  "Returns a blob-id and core.async channels to receive data structure nodes"
  []
  (let [blob-id (<!! state/blob-id-ticket-dispenser)]
    (let [[chan-1 chan-2 chan-3] (get-blob-channels blob-id)]
      (if (nil? chan-1)
        (throw (Exception. "No tickets in the dispenser")))
      {:blob-id blob-id :node-collector-ch chan-1 :b-tree-leaf-node-ch chan-2 :pipeline-command-log-ch chan-3})))

(defn nodes->blob-proto->byte-array
  "Converts bitmap-indexed and array nodes to proto blob, and then to byte-array representation"
  [internal-blob-index bitmap-indexed-nodes array-nodes hash-collision-nodes val-nodes b-tree-leaf-nodes b-tree-inner-nodes]

  (println-m "saving bitmap-indexed-nodes" bitmap-indexed-nodes)
  (println-m "saving array-nodes" array-nodes)
  (println-m "saving hash-collision-nodes" hash-collision-nodes)
  (println-m "saving val-nodes" val-nodes)
  (println-m "b-tree-leaf-nodes" b-tree-leaf-nodes)
  (println-m "b-tree-inner-nodes" b-tree-inner-nodes)

  (protobuf-dump (protobuf BlobProtoDef
                           :index (util/copy-to-byte-string (util/serialize internal-blob-index))
                           ;lowercase because protobufs didNotLikeCamelCase
                           :pdmbitmapnodes bitmap-indexed-nodes
                           :pdmarraynodes array-nodes
                           :pdmcollisionnodes hash-collision-nodes
                           :pdmvalnodes val-nodes
                           :btreeleafnodes b-tree-leaf-nodes
                           :btreeinnernodes b-tree-inner-nodes)))

(defn allocate-next-blob-id
  "Allocates the next blob id as soon as the current one starts in the go loop below"
  [blob-size]
  (let [confirm-ch (chan 1)
        _ (to-disk/allocate-next-blob-id confirm-ch)
        new-blob-id (<!! confirm-ch)]
    ;allocate blob-channel
    (add-blob-channel new-blob-id blob-size)
    ;start the next blob collector
    (start-blob-collector new-blob-id)
    ;add all the tickets to the dispenser
    (dotimes [_ blob-size] (>!! state/blob-id-ticket-dispenser new-blob-id))))

(defn build-b-tree-time-indices
  "Calls the io-assoc on b-tree the desired number of times (depending on how many different entities are in this blob)"
  [b-tree-key-vals-by-entity-id blob-id]
  (let [partition-size (count b-tree-key-vals-by-entity-id)
        b-tree-node-collector-ch (chan partition-size (bpt-b-node-collector-xf partition-size))
        b-tree-root-ch (chan partition-size (bpt-b-root-xf partition-size))]
    ;build the time index using b-plus-trees
    (doseq [[entity-info vector-of-maps] b-tree-key-vals-by-entity-id]
      ;all key-vals for one entity
      (let [^APersistentVector key-vals (into [] (mapcat (juxt :timestamp :io-map) vector-of-maps))]
        ;write key-vals for each entity in a b-plus-tree, chan receives all the nodes changed
        ;(future)
        (bpt-b/io-assoc (:entity-id entity-info) (:entity-type entity-info) key-vals blob-id b-tree-node-collector-ch b-tree-root-ch)))
    [b-tree-node-collector-ch b-tree-root-ch]))


(defn build-internal-blob-index
  "Returns index map looks like this {^long node-uuid [^Keyword node-type ^long nth-i], etc}"
  [all-node-seqs]
  (let [nth-i (volatile! -1)]
    (persistent!
      (->> all-node-seqs
           (reduce (fn [result [node-type node-seq]]
                     (vreset! nth-i -1)
                     (reduce (fn [-result node]
                               (vswap! nth-i (fn [^long x] (+ x 1)))
                               (assoc! -result (-> node :uuid) [node-type @nth-i])) result node-seq)) (transient {}))))))


;PIPELINE CHANNEL XF
(defn step-6-clean-up-memory [{:keys [blob-id mini-blobs last-timestamp-in-blob] :as params}]
  (let [entity-ids (map :entity-id mini-blobs)]
    (println-m "step-6-clean-up-memory" "no retry, finishing blob-id" blob-id)
    ;clear strong cache
    (doseq [entity-id entity-ids]
      (blob-unpacker/clear-strong-node-cache blob-id entity-id))
    ;clean in memory index
    (->> mini-blobs
         (map :entity-id)
         (map (partial bpt-sm/clean-up-keys-less-than-or-equal-to last-timestamp-in-blob))
         (doall))
    ;garbage collect channels
    (state/garbage-collect-blob-channels blob-id)
    ;garbage collect blob indexes
    (state/garbage-collect-blob-indexes blob-id)))

(defn step-5-delete-command-log [{:keys [blob-id] :as params}]
  (command-log/delete-command-log blob-id)
  params)

(defn step-4-set-global-blob-id [{:keys [blob-id mini-blobs] :as params}]
  (io-utils/blocking-loop to-disk/set-last-saved-blob-id blob-id)
  ;start GC for current blod-id root log
  (bpt-root-log/start-gc blob-id)
  (bpt-root-log-remote/start-gc blob-id)
  params)

(defn step-3-write-root-log-local [{:keys [blob-id b-tree-root-log-entries] :as params}]
  (io-utils/blocking-loop bpt-root-log/write-protobufs-to-log b-tree-root-log-entries (bpt-root-log/get-log-file-path blob-id))
  params)

(defn step-2-write-root-log-remote [{:keys [blob-id b-tree-root-log-entries] :as params}]
  (io-utils/blocking-loop to-disk/save-root-log-remote b-tree-root-log-entries blob-id)
  params)

(defn- async-write-mini-blobs
  [blob-id mini-blobs confirm-ch]
  (doall
    (for [{:keys [entity-id pdm-node-seqs b-tree-node-seqs internal-blob-index]} mini-blobs]
      (let [blob-data (nodes->blob-proto->byte-array internal-blob-index
                                                     (into [] (nth pdm-node-seqs 0))
                                                     (into [] (nth pdm-node-seqs 1))
                                                     (into [] (nth pdm-node-seqs 2))
                                                     (into [] (nth pdm-node-seqs 3))
                                                     (into [] (nth b-tree-node-seqs 0))
                                                     (into [] (nth b-tree-node-seqs 1)))]
        (io-utils/blocking-loop to-disk/save-blob blob-data blob-id entity-id))))
  (>!! confirm-ch true))

(defn step-1-save-blobs [{:keys [blob-id mini-blobs] :as params}]
  (io-utils/blocking-loop async-write-mini-blobs blob-id mini-blobs)
  params)

(defn step-0-wait-command-log-saved [to-pipeline-ch]
  ;wait for the blob-id data to become available
  (let [{:keys [pipeline-command-log-ch] :as params} (<!! to-pipeline-ch)]
    ;wait for the command log to be confirmed
    (<!! pipeline-command-log-ch)
    params))

;pipeline chans
(def step-0-wait-till-command-log-saved-ch (chan 1))
(def step-1-save-blobs-ch (chan 1))
(def step-2-write-root-log-remote-ch (chan 1))
(def step-3-write-root-log-local-ch (chan 1))
(def step-4-set-global-blob-id-ch (chan 1))
(def step-5-delete-command-log-ch (chan 1))
(def step-6-clean-up-memory-ch (chan 1))

(def pipelines-started? (atom false))

(defn start-pipelines []
  (println-m "STARTING PIPELINES")
  (when (= false @pipelines-started?)
    (pipeline-blocking 6 step-1-save-blobs-ch (map step-0-wait-command-log-saved) step-0-wait-till-command-log-saved-ch)
    (pipeline-blocking 6 step-2-write-root-log-remote-ch (map step-1-save-blobs) step-1-save-blobs-ch)
    (pipeline-blocking 6 step-3-write-root-log-local-ch (map step-2-write-root-log-remote) step-2-write-root-log-remote-ch)
    (pipeline-blocking 6 step-4-set-global-blob-id-ch (map step-3-write-root-log-local) step-3-write-root-log-local-ch)
    ;this step should always stay at 1 parallelism, acting like a commit point for a blob-id
    (pipeline-blocking 1 step-5-delete-command-log-ch (map step-4-set-global-blob-id) step-4-set-global-blob-id-ch)
    (pipeline-blocking 6 step-6-clean-up-memory-ch (map step-5-delete-command-log) step-5-delete-command-log-ch)
    (pipeline-blocking 1 (chan (dropping-buffer 0)) (map step-6-clean-up-memory) step-6-clean-up-memory-ch)
    (reset! pipelines-started? true)))


(defn start-blob-collector
  "Starts a loop process that collects incoming nodes;
   When done collecting all nodes for that blob-id, it ships them to durable storage"
  [blob-id]
  (println-m "starting loop for blob-id " blob-id)
  (thread (loop []
            ;try to pull group of nodes from a channel, from a channel
            (let [blob-size constants/blob-size
                  [pdm-node-collector-xf-ch b-tree-leaf-node-xf-ch pipeline-command-log-ch] (get-blob-channels blob-id)
                  ;group-by a data structure looks like {:entity-id "entity123" :entity-type some-long}
                  ^APersistentVector b-tree-key-vals (<!! b-tree-leaf-node-xf-ch)
                  last-timestamp-in-blob (-> b-tree-key-vals (peek) :timestamp)
                  _ (println-m "last-timestamp-in-blob:" last-timestamp-in-blob)
                  b-tree-key-vals-by-entity-id (group-by :entity-info b-tree-key-vals)
                  ;group pdm node-collectors
                  group-pdm-node-collectors (group-by :entity-id (<!! pdm-node-collector-xf-ch))
                  ;build the b-tree index
                  [b-tree-node-collector-ch b-tree-root-ch] (build-b-tree-time-indices b-tree-key-vals-by-entity-id blob-id)
                  _ (println-m "built b-tree time index success for blob-id" blob-id)
                  ;group b-tree node-collectors
                  group-b-tree-node-collector-ch (group-by :entity-id (<!! b-tree-node-collector-ch))
                  ;grab all the root logs
                  b-tree-root-log-entries (<!! b-tree-root-ch)
                  ;safe to allocate next blob after we've received the b-tree nodes
                  to-pipeline-ch (chan 1)
                  ;reserve place in pipeline
                  _ (>!! step-0-wait-till-command-log-saved-ch to-pipeline-ch)
                  ;alocate next blob-id
                  _ (allocate-next-blob-id blob-size)
                  ;group root logs
                  group-b-tree-root-log-entries (group-by :entity b-tree-root-log-entries)
                  mini-blobs (for [[entity-id vector-of-node-collector-maps] group-pdm-node-collectors]
                               (do
                                 (let [pdm-node-seqs (->> vector-of-node-collector-maps
                                                          (map :node-collector)
                                                          (reduce (fn [[b a h v] [-b -a -h -v]]
                                                                    [(concat b -b) (concat a -a) (concat h -h) (concat v -v)])))
                                       b-tree-node-seqs (->> (get group-b-tree-node-collector-ch entity-id)
                                                             (map :node-collector)
                                                             (reduce (fn [[l i] [-l -i]]
                                                                       [(concat l -l) (concat i -i)])))
                                       internal-blob-index (build-internal-blob-index {:pdmbitmapnodes    (nth pdm-node-seqs 0)
                                                                                       :pdmarraynodes     (nth pdm-node-seqs 1)
                                                                                       :pdmcollisionnodes (nth pdm-node-seqs 2)
                                                                                       :pdmvalnodes       (nth pdm-node-seqs 3)
                                                                                       :btreeleafnodes    (nth b-tree-node-seqs 0)
                                                                                       :btreeinnernodes   (nth b-tree-node-seqs 1)})
                                       root-logs (get group-b-tree-root-log-entries entity-id)]
                                   {:entity-id           entity-id
                                    :pdm-node-seqs       pdm-node-seqs
                                    :b-tree-node-seqs    b-tree-node-seqs
                                    :b-tree-root-logs    root-logs
                                    :internal-blob-index internal-blob-index})))]

              ;send to pipeline
              (>!! to-pipeline-ch {:blob-id                 blob-id
                                   :mini-blobs              mini-blobs
                                   :b-tree-root-log-entries b-tree-root-log-entries
                                   :pipeline-command-log-ch pipeline-command-log-ch
                                   :last-timestamp-in-blob  last-timestamp-in-blob})))))

(defn init
  "Initializes blob-id allocation (called once at server start)"
  []
  (allocate-next-blob-id constants/blob-size))

(defn init-pipelines
  "Starts the pipelines"
  []
  (start-pipelines))