(ns neversleep-db.write-dispatch-test
  (:require [clojure.test :refer :all]
            [clojure.test.check :as check]
            [clojure.test.check.generators :as gen]
            [neversleep-db.println-m :refer [println-m]]
            [clojure.test.check.properties :as prop]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.core.async :refer [chan go >! <! <!! >!! go-loop put! thread alts! alts!! timeout pipeline close!]]
            [neversleep-db.write-dispatch :refer :all]))


(defn request-allocate-write-chan-test- []
  (let [random-entity-ids (gen/sample (gen/such-that #(and (not-empty %)) gen/string-alpha-numeric 1000) 10000)]
    (doseq [entity-id random-entity-ids]
      (let [allocation-confirm-ch (chan 1)]
        (request-allocate-write-chan entity-id (fn []) allocation-confirm-ch)
        (<!! allocation-confirm-ch)))
    (comment)
    (let [result (->> random-entity-ids
                      (map get-write-chan)
                      (remove nil?)
                      (into []))]
      (is (= (count result) (count random-entity-ids))))))
(deftest request-allocate-write-chan-test
  (request-allocate-write-chan-test-))

