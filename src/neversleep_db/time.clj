(ns neversleep-db.time
  (:import (jv SystemClock)))

(def one-second 1000000000)

(defn n-minutes-before [t n]
  (- t (* n 60 one-second)))

(defn n-minutes-ago [n]
  (- (SystemClock/getTime) (* n 60 one-second)))

(defn a-minute-ago []
  (n-minutes-ago 1))

(defn a-minute-before [t]
  (n-minutes-before t 1))