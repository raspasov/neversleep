(ns neversleep-db.b-plus-tree-root-cache
  (:require [neversleep-db.b-plus-tree-root-log :as bpt-root-log]
            [neversleep-db.b-plus-tree-root-log-remote :as bpt-root-log-remote]
            [neversleep-db.state :refer [all-maps all-b-plus-trees-on-disk]]
            [neversleep-db.util :as util]
            [neversleep-db.io-utils :as io-utils]
            [neversleep-db.to-disk :as to-disk]
            [clojure.core.async :refer [chan timeout go <! <!! >!! >! go-loop put! thread alts!! alts! dropping-buffer pipeline-blocking]]
            [neversleep-db.println-m :refer [println-m]]))


(defn mutable-collector
  "Returns a mutable collector that collects all types of roots in volatiles"
  []
  [{:log-key-wanted :root :root-collector (volatile! {})}
   {:log-key-wanted :lastentityroot :root-collector (volatile! {})}])

(defn- restore-local-zero-log
  [last-saved-root-log]
  (bpt-root-log/write-zero-log (bpt-root-log-remote/restore-local-zero-log last-saved-root-log)))

(defn- init-from-local
  "Loads roots from local storage (default)"
  [last-saved-blob-id]
  (let [[b-tree-roots map-roots :as from-local] (bpt-root-log/get-roots-less-than-or-equal-to last-saved-blob-id (mutable-collector))]
    from-local))

(defn set-roots! [[b-tree-roots map-roots]]
  ;init maps
  (reset! all-maps @map-roots)
  ;init b-trees
  (reset! all-b-plus-trees-on-disk @b-tree-roots))

(defn init
  "Loads all roots into memory at server start"
  [last-saved-blob-id]
  (let [[b-tree-roots map-roots :as from-local] (init-from-local last-saved-blob-id)]
    ;if roots are empty, try from remote
    (if (= {} @b-tree-roots @map-roots)
      ;try to recover logs from remote
      (do
        (let [confirm-gc-ch (chan 1)]
          ;GC remote logs before restoring
          (bpt-root-log-remote/start-gc last-saved-blob-id confirm-gc-ch)
          (let [gc-result (util/<!!-till-timeout confirm-gc-ch 10000)]
            (if-not (= true gc-result)
              (throw (Exception. "Remote root log GC failed while trying to restore remote root logs, aborting startup")))))
          (restore-local-zero-log (io-utils/blocking-loop to-disk/get-last-saved-root-log))
          (set-roots! (init-from-local last-saved-blob-id)))
      ;else - set the roots from local
      (set-roots! from-local))
    true))


;EXPLORE DATA
;(defn init-print [last-saved-blob-id]
;  (let [[b-tree-roots map-roots] (bpt-root-log/get-roots-less-than-or-equal-to last-saved-blob-id
;                                                                               [{:log-key-wanted :root :root-collector (volatile! {})}
;                                                                                {:log-key-wanted :lastentityroot :root-collector (volatile! {})}])]
;    ;init maps
;    (clojure.pprint/pprint @map-roots)
;    @map-roots))
;
;
;(defn init-print-2 [last-saved-blob-id]
;  (let [[b-tree-roots map-roots] (bpt-root-log/get-roots-less-than-or-equal-to last-saved-blob-id
;                                                                               [{:log-key-wanted :root :root-collector (volatile! {})}
;                                                                                {:log-key-wanted :lastentityroot :root-collector (volatile! {})}])]
;    ;init maps
;    (clojure.pprint/pprint @b-tree-roots)
;    @b-tree-roots))