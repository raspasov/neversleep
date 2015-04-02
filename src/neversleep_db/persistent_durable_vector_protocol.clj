(ns neversleep-db.persistent-durable-vector-protocol)

(defprotocol DurableVectorNode

  (get-array [this])
  (get-node-id [this])
  (get-blob-id [this])


  ;ValNode specific
  (get-value [this]))