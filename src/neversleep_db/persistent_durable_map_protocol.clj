(ns neversleep-db.persistent-durable-map-protocol)

(defprotocol DurableNode
  "Write a book about this one"
  (n-assoc [this shift hash key val added-leaf node-collector blob-id entity-id]
           [this edit shift hash key val added-leaf node-collector blob-id entity-id])
  (n-without [this shift hash key node-collector blob-id entity-id]
             [this edit shift hash key node-collector blob-id entity-id])
  (ensure-editable! [this edit blob-id]
                    [this edit cnt array blob-id])
  (edit-and-set! [this edit ^Integer i ^Object a blob-id]
                 [this edit ^Integer i ^Object a ^Integer j ^Object b blob-id])
  ;(edit-and-remove-pair! [this edit bit i blob-id])
  (set-in-array! [this i ^Object o])
  (set-array! [this new-array])
  (bitmap-bit-or! [this new-bit])
  ;(bitmap-bit-xor! [this new-bit])
  (array-copy-on-this! [this src-pos dest-pos how-many])
  (n-find [this shift hash key not-found responce-ch entity-id])
  (get-bitmap [this])
  (get-array [this])
  (get-node-id [this])
  (get-blob-id [this])


  ;ArrayNode specific
  (inc-cnt! [this])
  (get-count [this])
  (pack [this edit idx blob-id])

  ;HashCollisionNode specific
  (find-index [this key])
  (set-cnt! [this new-cnt])
  (get-hash [this])

  ;ValNode specific
  (get-value [this])

  ;Protobuf
  (to-protobuf [this])
  (to-remote-pointer [this])
  (get-node-type [this]))