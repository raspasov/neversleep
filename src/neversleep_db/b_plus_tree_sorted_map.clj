(ns neversleep-db.b-plus-tree-sorted-map
  (:require [clojure.core.async :refer [chan go >! <! <!! >!! go-loop put! thread alts! alts!! timeout pipeline close!]]
            [neversleep-db.println-m :refer [println-m]]
            [neversleep-db.state :as state]))


(defn get-b-plus-tree [entity-id]
  (get @state/all-b-plus-trees-in-mem entity-id))

(defn save-b-plus-tree [id b-plus-tree]
  (alter state/all-b-plus-trees-in-mem (fn [x] (assoc x id b-plus-tree))))
;end

;constructors
(defn new-b-tree []
  (sorted-map))
;end

(def io-assoc-agent (agent nil))

;(defn io-assoc [entity-id k v confirm-ch]
;  (dosync
;    (let [tree (if-let [tree (get-b-plus-tree entity-id)]
;                 tree
;                 (new-b-tree))]
;      (save-b-plus-tree entity-id (assoc tree k v))))
;  (>!! confirm-ch true))

(defn io-assoc [entity-id k v confirm-ch]
  (send io-assoc-agent (fn [_]
                         (dosync
                           (let [tree (if-let [tree (get-b-plus-tree entity-id)]
                                        tree
                                        (new-b-tree))]
                             (save-b-plus-tree entity-id (assoc tree k v))))
                         (>!! confirm-ch true)
                         nil)))


(defn io-find-range
  "Searches the b-plus tree for a range of keys.
   If key-start > key-end, search is DESC-like"
  [entity-id key-start key-end limit responce-ch]
  (if-let [tree (get-b-plus-tree entity-id)]
    (>!! responce-ch
         {:result
          (->> (rsubseq tree >= key-end <= key-start)
               (take limit)
               (into []))})
    ;empty
    (>!! responce-ch
         {:result []})))

(defn clean-up-keys-less-than-or-equal-to [k entity-id]
  (dosync
    (when-let [tree (get-b-plus-tree entity-id)]
      (let [key-vals-to-remove (take-while #(<= ^long (nth % 0) ^long k) tree)
            keys-seq (map first key-vals-to-remove)
            new-tree (apply dissoc tree keys-seq)]
        (save-b-plus-tree entity-id new-tree)))))

