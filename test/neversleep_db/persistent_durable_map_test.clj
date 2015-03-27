(ns neversleep-db.persistent-durable-map-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [chan go >! <! <!! >!! go-loop put! thread alts! alts!! timeout]]
            [neversleep-db.persistent-durable-map :refer :all]
            [neversleep-db.core :as db-core]
            [neversleep-db.blob-unpacker :as blob-unpacker]
            [neversleep-db.read-dispatch :as read-dispatch]
            [neversleep-db.node-keeper :as node-keeper]
            [neversleep-db.write-dispatch :as write-dispatch]
            [clojure.test.check :as check]
            [neversleep-db.to-disk :as to-disk]
            [neversleep-db.local-disk :as local-disk]
            [clojure.test.check.generators :as gen]
            [neversleep-db.println-m :refer [println-m]]
            [neversleep-db.state :as state]
            [clojure.test.check.properties :as prop]
            [clojure.test.check.clojure-test :refer [defspec]]
            [neversleep-db.persistent-durable-map :as pdm]
            [neversleep-db.util :as util])
  (:import (jv SystemClock)))


;When running tests we start with a clean server:
;1. Delete all data on disk local, remote
;2. Simulate a server restart
;3. Run the test
;4. Repeat steps 1-3 for each test

;helper fns
(defn- random-data [cnt]
  (->> (gen/sample (gen/such-that (fn [x] (not-empty x)) gen/string-alpha-numeric 1000) cnt)
       (map #(.toLowerCase %))
       (distinct)))

(defn- key-val-data [data]
  (->> data
       (map #(vector % (str % "-val")))
       (into [])))
;end helper fns

(defn simulate-server-restart! []
  "simulate-server-restart! from tests"
  (db-core/simulate-server-restart!))

(defn reset-state-full []
  (println-m "RESET FULL")
  ;delete to-disk data
  (to-disk/delete-all-data)
  ;delete local disk data
  (local-disk/delete-all-b-tree-root-logs!)
  (local-disk/delete-all-command-logs!)
  ;reset the local state
  (simulate-server-restart!))

(defn server-start []
  (println-m "server-start from tests")
  (db-core/server-start))

;=====
;TESTS
;=====
(comment)
(defn -persistent-durable-map-test-1 [with-simulate-restart?]
  (reset-state-full)
  (let [num-of-writes 500
        data (into [] (random-data num-of-writes))
        data-cnt (count data)
        _ (println-m "about to enter N distintct items:" (count data))
        _ (Thread/sleep 1000)
        ;do the writes
        _ (loop [responses []
                 response-ch (chan 1)
                 i 0]
            (if (< i data-cnt)
              (do (write-dispatch/io-pdm-write pdm/io-assoc "users" (nth data i) (nth data i) response-ch)
                  (println-m "writing" (nth data i))
                  (recur (conj responses (<!! response-ch)) (chan 1) (+ i 1)))
              responses))
        ;_ (println "done with writes")
        _ (Thread/sleep 3000)
        ;reset state local (clear cache)
        _ (when with-simulate-restart? (simulate-server-restart!))
        timestamp-after-io-assocs (SystemClock/getTime)
        io-finds (loop [responses []
                        i 0]
                   (println-m "doing read " i)
                   (if (< i data-cnt)
                     (do
                       (let [r (<!! (read-dispatch/io-find-as-of! "users" (nth data i) timestamp-after-io-assocs))]
                         (println-m r)
                         (recur (conj responses (-> r :result)) (+ i 1))))
                     responses))]
    (is (= io-finds data))))

(deftest persistent-durable-maps-test-1
  (println "TEST 1")
  ;DESCRIPTION
  ;1. Write N key/vals to an entity;
  ;2. One by one read all the N key/vals via io-find-as-of!
  ;reset state full
  (-persistent-durable-map-test-1 false))

(deftest persistent-durable-maps-test-1-1
  (println "TEST 1 with restart")
  ;DESCRIPTION
  ;1. Write N key/vals to an entity;
  ;2. Simulate restart
  ;3. One by one read all the N key/vals via io-find-as-of!
  ;reset state full
  (-persistent-durable-map-test-1 true))


(defn -persistent-durable-maps-test-2 [with-simulate-restart?]
  (reset-state-full)
  (let [entity-id "users"
        num-of-writes 5000
        data (random-data num-of-writes)
        ;data (keys key-val-data)
        key-val-data (key-val-data data)
        _ (println-m "about to enter N distinct items:" (count data))
        _ (Thread/sleep 1000)
        responces (atom [])
        _ (doseq [[k v] key-val-data]
            (let [responce-ch (chan 1)]
              (write-dispatch/io-pdm-write pdm/io-assoc entity-id k v responce-ch)
              (swap! responces conj (<!! responce-ch))))
        _ (Thread/sleep 1000)
        _ (when with-simulate-restart? (simulate-server-restart!))
        _ (Thread/sleep 1000)
        ;_ (println "TEST 2, going to io-get-entity-as-of!")
        timestamp-after-io-assocs (SystemClock/getTime)
        whole-entity (-> (read-dispatch/io-get-entity-as-of! entity-id timestamp-after-io-assocs)
                         (<!!)
                         :result)]
    (is (= whole-entity (into {} key-val-data)))))

(comment)
(deftest persistent-durable-maps-test-2
  (println "TEST 2")
  ;DESCRIPTION
  ;1. Write N key/vals to an entity;
  ;2. Read entity as a whole via io-get-entity-as-of!
  ;reset state full
  (-persistent-durable-maps-test-2 false))

(deftest persistent-durable-maps-test-2-1
  (println "TEST 2")
  ;DESCRIPTION
  ;1. Write N key/vals to an entity;
  ;2. Simulate restart
  ;3. Read entity as a whole via io-get-entity-as-of!
  ;reset state full
  (-persistent-durable-maps-test-2 true))

(comment)
(deftest persistent-durable-maps-test-3
  (println "TEST 3")
  ;DESCRIPTION
  ;1. Writes one key/val to an entity, record timestamp t-1
  ;2. Read same key/val as-of t-1
  ;3. Repeat N times
  (reset-state-full)
  (let [entity-id "users"
        num-of-writes 10000
        key-val-data (-> num-of-writes (random-data) (key-val-data))
        _ (println-m "about to enter N distintct key/vals:" (count key-val-data))
        _ (Thread/sleep 1000)
        _ (doseq [[k v] key-val-data]
            (let [responce-ch (chan 1)]
              (write-dispatch/io-pdm-write pdm/io-assoc entity-id k v responce-ch)
              ;try to read the write immediately after
              (let [{:keys [timestamp]} (<!! responce-ch)
                    ;wait for responce, save it
                    result (-> (read-dispatch/io-find-as-of! entity-id k (util/parse-timestamp timestamp))
                               (<!!)
                               :result)]
                (is (= result v)))))]))


(defn -test-5-write [entities]
  (let [_ (println-m "about to write N distinct entities:" (count entities))
        _ (Thread/sleep 1000)
        responces (atom [])
        _ (doseq [entity-id entities]
            (println-m "writing" entity-id)
            (let [responce-ch (chan 1)]
              (write-dispatch/io-pdm-write pdm/io-assoc entity-id entity-id entity-id responce-ch)
              ;wait and save the responce
              (swap! responces conj (<!! responce-ch))))]))

(defn -test-5-read [entities]
  (let [t-1 (SystemClock/getTime)]
    (into [] (for [entity-id entities]
               (do
                 (println-m "reading" entity-id)
                 (->> (read-dispatch/io-find-as-of! entity-id entity-id t-1)
                      (<!!)
                      :result))))))

(defn -persistent-durable-maps-test-5 [with-simulate-restart?]
  (reset-state-full)
  (let [entities (random-data 1000)
        _ (-test-5-write entities)
        _ (Thread/sleep 2000)
        _ (println-m "done with writes")
        _ (when with-simulate-restart? (simulate-server-restart!))
        results (-test-5-read entities)]
    (is (= entities results))))

(comment)
(deftest persistent-durable-maps-test-5
  (println "TEST 5")
  ;Description
  ;1. Write N key/vals to N different entities
  ;2. Read all writes
  (-persistent-durable-maps-test-5 false))

(deftest persistent-durable-maps-test-5-1
  (println "TEST 5 with restart")
  ;Description
  ;1. Write N key/vals to N different entities
  ;2. Simulate restart
  ;3. Read all writes
  (-persistent-durable-maps-test-5 true))

(defn -persistent-durable-maps-test-6 [with-simulate-restart?]
  (reset-state-full)
  (let [entity-id "users"
        number-of-writes 1000
        entries (into [] (random-data number-of-writes))
        _ (println-m "about to write N distinct key/vals:" (count entries))
        _ (Thread/sleep 1000)
        responces (atom [])
        ;write
        _ (doseq [entry entries]
            (let [responce-ch (chan 1)]
              (write-dispatch/io-pdm-write pdm/io-assoc entity-id entry entry responce-ch)
              (swap! responces conj (<!! responce-ch))))
        ;_ (println "timestamp at end of writes, TEST 6:" (SystemClock/getTime))
        _ (when with-simulate-restart? (simulate-server-restart!))
        ;"delete"
        _ (doseq [entry entries]
            (let [responce-ch (chan 1)]
              (write-dispatch/io-pdm-write pdm/io-without entity-id entry nil responce-ch)
              (<!! responce-ch)))
        t-1 (SystemClock/getTime)]
    (is (= 0 (-> (read-dispatch/io-get-entity-as-of! entity-id t-1)
                 (<!!)
                 :result
                 (count))))))

(comment)
(deftest persistent-durable-maps-test-6
  (println "TEST 6")
  ;Description
  ;1. Write N key/vals to an entity
  ;2. Delete N keys from an entity
  (-persistent-durable-maps-test-6 false))


(deftest persistent-durable-maps-test-6-1
  (println "TEST 6 with restart")
  ;Description
  ;1. Write N key/vals to an entity
  ;2. Simulate restart
  ;3. Delete N keys from an entity
  (-persistent-durable-maps-test-6 true))



(defn- -persistent-durable-maps-test-7 [with-simulate-restart?]
  (reset-state-full)
  (let [entity-id "many-writes-test"
        a-key "k-1"
        number-of-different-vals 1000
        all-vals (into [] (distinct (random-data number-of-different-vals)))
        _ (doseq [a-val all-vals]
            (let [responce-ch (chan 1)]
              (write-dispatch/io-pdm-write pdm/io-assoc entity-id a-key a-val responce-ch)
              (<!! responce-ch)))
        _ (when with-simulate-restart? (simulate-server-restart!))
        t-1 (SystemClock/getTime)
        results (map (comp second first second) (-> (read-dispatch/io-get-all-versions-between! entity-id t-1 0 number-of-different-vals)
                                                    (<!!)
                                                    (:result)))]
    (is (= (reverse results) all-vals))))

(deftest persistent-durable-maps-test-7
  (println "TEST 7")
  ;Description
  ;1. Write N times to the same key in the same entity
  ;2. Read all version of the entity
  (-persistent-durable-maps-test-7 false))

(deftest persistent-durable-maps-test-7-1
  (println "TEST 7 with restart")
  ;Description
  ;1. Write N times to the same key in the same entity
  ;2. Simulate restart
  ;3. Read all version of the entity
  (-persistent-durable-maps-test-7 true))

(defn -persistent-durable-maps-test-8 [with-simulate-restart?]
  )

(deftest persistent-durable-maps-test-8
  (println "TEST 8"))

