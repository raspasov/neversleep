(ns neversleep-db.b-plus-tree-protocol)

(defprotocol DurableBPlusTreeNode
  "Write a book about this one"
  (n-assoc [this key val ops-collector])


  (n-find [this key not-found responce-ch])

  (n-find-range-forward [this key-start key-end limit not-found responce-ch results])
  (n-find-range-backward [this key-start key-end limit not-found responce-ch results]
                         [this key-start key-end limit not-found responce-ch results entity-id])

  (get-array [this])

  (get-parent [this])
  (set-parent! [this new-parent])

  (add-to-array! [this key val])

  (set-array! [this new-array])

  (set-right-pointer! [this new-right-pointer])
  (get-left-pointer [this])
  (get-right-pointer [this])

  (n-push-up [this key pointer ops-collector])

  ;bulk
  ;collects the parent-pointers as it goes down the tree
  (n-assoc-bulk [this key-vals parent-pointers node-collector blob-id entity-id])
  ;
  (n-push-up-bulk [this vector-of-pushed-up-keys-and-pointers parent-pointers node-collector blob-id entity-id])

  (path-copy-on-write [this new-rightmost-pointer parent-pointers node-collector blob-id entity-id])

  ;Protobuf

  (get-node-type [this])
  (get-node-id [this])
  (to-protobuf [this])
  (get-blob-id [this])
  (to-remote-pointer [this]))
