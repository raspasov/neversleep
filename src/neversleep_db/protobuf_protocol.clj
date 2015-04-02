(ns neversleep-db.protobuf-protocol)

(defprotocol ProtobufNode
  "Write a book about this one"

  ;Protobuf
  (to-protobuf [this])
  (to-remote-pointer [this])
  (get-node-type [this]))