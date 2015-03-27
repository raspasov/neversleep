(ns neversleep-db.write-dispatch
  (:require [clojure.core.async :refer [chan timeout go <! close! <!! >!! >! go-loop put! thread alts!! alts! dropping-buffer pipeline-blocking pipeline-async]]
            [neversleep-db.backpressure :as backpressure]
            [clojure.core.async.impl.protocols :refer [closed?]]
            [neversleep-db.persistent-durable-map :as pdm]
            [neversleep-db.state :as state]
            [neversleep-db.read-dispatch :as read-dispatch]
            [neversleep-db.println-m :refer [println-m]])
  (:import (clojure.lang IFn)
           (jv SystemClock)))


(defn get-write-chan [entity-id]
  (get @state/write-chans entity-id))

(defn init-write-chan
  "Allocates a write chan and starts a loop which processes command-fn's"
  [entity-id command-fn]
  (let [write-ch (chan 42)]
    ;put the first write on the chan
    (>!! write-ch command-fn)
    (swap! state/write-chans (fn [x] (assoc x entity-id write-ch)))
    (go (loop []
          (let [command-fn (let [[value _] (alts! [write-ch (timeout 20)])] value)]
            (if-not (nil? command-fn)
              ;true
              (do (<! (thread (command-fn)))
                  (recur))
              ;else, time to start GC process for this loop
              (if-not (closed? write-ch)
                ;true - close the channel
                (do (close! write-ch)
                    (println-m "CHANNEL CLOSED")
                    ;recur one more time so we process any puts that might have come in on the channel
                    (recur))
                ;else we're done
                (do (println-m "CLEANING GO LOOP FOR" entity-id)
                    ;finalize cleanup by dissoc
                    (swap! state/write-chans dissoc entity-id)))))))
    ;return the newly allocated ch
    write-ch))


(def write-chan-allocator-ch (chan 42))

(defn start-write-chan-allocator-loop []
  (go (loop [allocation-fn-wait nil]
        (if (= nil allocation-fn-wait)
          (let [^IFn allocation-fn (<! write-chan-allocator-ch)]
            (if (< 42 (count @state/write-chans))
              ;going to wait
              (recur allocation-fn)
              ;ok to allocate
              (do (allocation-fn)
                  (recur nil))))
          ;else, we already have a fn waiting
          (if (< 42 (count @state/write-chans))
            ;going to wait, again
            (recur allocation-fn-wait)
            ;finally ok to allocate
            (do (allocation-fn-wait)
                (recur nil)))))))


(defn request-allocate-write-chan
  "All entity-id channel allocations go through here, agent ensures that they happen in order.
   Takes command-fn and puts it onto a newly allocated channel"
  [entity-id command-fn allocation-confirm-ch]
  (>!! write-chan-allocator-ch (fn []
                                 (let [ch (get-write-chan entity-id)]
                                   (if (= nil ch)
                                     ;allocate a write chan and put the write on the chan
                                     (do (init-write-chan entity-id command-fn)
                                         ;send confirmation back
                                         (>!! allocation-confirm-ch true))
                                     ;write chan allocated before, send command-fn directly
                                     (do (>!! ch command-fn)
                                         (>!! allocation-confirm-ch true))))
                                 nil)))


(defn io-pdm-write
  "Main entry point for map io-assoc writes, routes everything
   through a channel per entity that ensures only one write per entity is in progress."
  ([^IFn io-fn entity-id key val confirm-ch]
   (io-pdm-write io-fn entity-id key val confirm-ch identity))
  ([^IFn io-fn entity-id key val confirm-ch ^IFn confirm-ch-modifier-fn]
    (let [ch (get-write-chan entity-id)
          ;if override is true, allow the write in any case, otherwise check with the backpressure ns
          ok-to-write? (backpressure/ok-to-write?)
          ;create an anonymous fn that holds the action
          command-fn (condp = io-fn
                       pdm/io-assoc #(io-fn entity-id key val confirm-ch confirm-ch-modifier-fn)
                       pdm/io-without #(io-fn entity-id key confirm-ch confirm-ch-modifier-fn)
                       ;last item in the key vector is the entity-id in this case - peek/pop juggling
                       pdm/io-assoc-in-json #(io-fn entity-id (peek key) (pop key) (-> (read-dispatch/io-find-as-of! entity-id (peek key) (SystemClock/getTime)) <!! :result) val confirm-ch confirm-ch-modifier-fn)
                       pdm/io-dissoc-in-json #(io-fn entity-id (peek key) (pop key) (-> (read-dispatch/io-find-as-of! entity-id (peek key) (SystemClock/getTime)) <!! :result) confirm-ch confirm-ch-modifier-fn)
                       (throw (Exception. "unsupported pdm operation")))]
      (if-not (= nil ch)
        ;channel ready to go
        (if ok-to-write?
          ;api ok, proceed with write
          (if (= false (>!! ch command-fn))
            ;if >!! returns false the channel has just closed, recur the function so we can retry
            (do (println-m "recur for a write retry")
                (recur io-fn entity-id key val confirm-ch confirm-ch-modifier-fn))
            true)
          ;if api is blocked, send a false on the channel immediately
          (>!! confirm-ch false))
        ;does not exist, request a channel to be allocated for that entity-id
        (let [allocation-confirm-ch (chan 1)]
          (request-allocate-write-chan entity-id command-fn allocation-confirm-ch)
          ;wait for the allocation to be complete before doing next write
          (<!! allocation-confirm-ch))))))


(def map-test-run-count (atom 0))

(defn map-test
  "Run insertion tests"
  []
  (let [run-count (swap! map-test-run-count + 1)
        key-base (str run-count "-user-v32-")]
    (println "testing with key-base:: " key-base)
    (time (dotimes [i 10000]
            (println i)
            (do
              (io-pdm-write pdm/io-assoc "users-2" (str key-base i) (str i) (chan 1))
              (io-pdm-write pdm/io-assoc "users-1" (str key-base i) (str i) (chan 1))
              (io-pdm-write pdm/io-assoc "users-3" (str key-base i) (str i) (chan 1))
              (io-pdm-write pdm/io-assoc "levels" (str key-base i) (str i) (chan 1))
              (io-pdm-write pdm/io-assoc "equipment" (str key-base i) (str i) (chan 1))
              (io-pdm-write pdm/io-assoc "countries" (str key-base i) (str i) (chan 1)))))))

(defn map-test-n []
  (let [run-count (swap! map-test-run-count + 1)
        key-base (str run-count "-user-v35-")]
    (println "testing with key-base:: " key-base)
    (time (dotimes [i 10]
            (println i)
            (dotimes [j 10000]
              (io-pdm-write pdm/io-assoc (str "users-v9-" j) (str key-base i) (str i "-v4") (chan 1)))))))



(defn map-test-long
  "Run insertion tests"
  []
  (let [run-count (swap! map-test-run-count + 1)
        key-base (str run-count "very-long-key-goes-here-very-long-key-goes-here-user-v40-")]
    (println "testing with key-base:: " key-base)
    (time (dotimes [i 2500]
            (println i)
            (do
              (let [confirm-ch (chan 1)]
                (io-pdm-write pdm/io-assoc "users-3-long" (str key-base i) (str i) confirm-ch)
                ;(<!! confirm-ch)
                )

              ;(io-pdm-write pdm/io-assoc "users-1-long" (str key-base i) (str i) (chan 1))
              ;(io-pdm-write pdm/io-assoc "users-3-long" (str key-base i) (str i) (chan 1))
              ;(io-pdm-write pdm/io-assoc "levels-long" (str key-base i) (str i) (chan 1))
              ;(io-pdm-write pdm/io-assoc "equipment-long" (str key-base i) (str i) (chan 1))
              ;(io-pdm-write pdm/io-assoc "countries-long" (str key-base i) (str i) (chan 1))
              )))))

(defn map-test-delete
  "Run insertion tests"
  []
  (let [run-count (swap! map-test-run-count + 1)
        key-base (str run-count "-user-v29-")]
    (println "testing with key-base:: " key-base)
    (time (dotimes [i 10000]
            (println i)
            (do
              (io-pdm-write pdm/io-without "users-2" (str key-base i) (str i) (chan 1))
              (io-pdm-write pdm/io-without "users-1" (str key-base i) (str i) (chan 1))
              (io-pdm-write pdm/io-without "users-3" (str key-base i) (str i) (chan 1))
              (io-pdm-write pdm/io-without "levels" (str key-base i) (str i) (chan 1))
              (io-pdm-write pdm/io-without "equipment" (str key-base i) (str i) (chan 1))
              (io-pdm-write pdm/io-without "countries" (str key-base i) (str i) (chan 1)))))))


(defn map-test-small
  "Run insertion tests"
  []
  (let [run-count (swap! map-test-run-count + 1)
        key-base (str run-count "-user-v32-")]
    (println-m "testing with key-base:: " key-base)
    (time (dotimes [i 1000]
            (println i)
            (do
              (io-pdm-write pdm/io-assoc "users-2-small" (str key-base i) (str key-base i "-V8") (chan 1))
              (io-pdm-write pdm/io-assoc "users-1-small" (str key-base i) (str key-base i "-V8") (chan 1))
              (io-pdm-write pdm/io-assoc "users-3-small" (str key-base i) (str key-base i "-V8") (chan 1))
              (io-pdm-write pdm/io-assoc "levels-small" (str key-base i) (str key-base i "-V8") (chan 1))
              (io-pdm-write pdm/io-assoc "equipment-small" (str key-base i) (str key-base i "-V8") (chan 1))
              (io-pdm-write pdm/io-assoc "countries-small" (str key-base i) (str key-base i "-V8") (chan 1))
              )))))

(defn map-test-small-2
  "Run insertion tests"
  []
  (let [run-count (swap! map-test-run-count + 1)
        key-base (str run-count "-user-v29-")]
    (println-m "testing with key-base:: " key-base)
    (time (dotimes [i 1]
            (println i)
            (do
              (io-pdm-write pdm/io-assoc "users-2-small" (str key-base i) (str key-base i "-V8") (chan 1))
              ;(io-pdm-write-chan pdm/io-assoc "users-1-small" (str key-base i) (str key-base i "-V8") (chan 1))
              ;(io-pdm-write-chan pdm/io-assoc "users-3-small" (str key-base i) (str key-base i "-V8") (chan 1))
              ;(io-pdm-write-chan pdm/io-assoc "levels-small" (str key-base i) (str key-base i "-V8") (chan 1))
              ;(io-pdm-write-chan pdm/io-assoc "equipment-small" (str key-base i) (str key-base i "-V8") (chan 1))
              ;(io-pdm-write-chan pdm/io-assoc "countries-small" (str key-base i) (str key-base i "-V8") (chan 1))
              )))))

(defn map-test-small-delete
  []
  (let [run-count (swap! map-test-run-count + 1)
        key-base (str run-count "-user-v28-")]
    (println-m "testing with key-base:: " key-base)
    (time (dotimes [i 1000]
            (println i)
            (do
              (io-pdm-write pdm/io-without "users-2-small" (str key-base i) nil (chan 1))
              (io-pdm-write pdm/io-without "users-1-small" (str key-base i) nil (chan 1))
              (io-pdm-write pdm/io-without "users-3-small" (str key-base i) nil (chan 1))
              (io-pdm-write pdm/io-without "levels-small" (str key-base i) nil (chan 1))
              (io-pdm-write pdm/io-without "equipment-small" (str key-base i) nil (chan 1))
              (io-pdm-write pdm/io-without "countries-small" (str key-base i) nil (chan 1)))))))

(defn map-test-1
  "Run insertion tests"
  []
  (let [run-count (swap! map-test-run-count + 1)
        key-base (str run-count "-user-v20-")]
    (println-m "testing with key-base:: " key-base)
    (time (dotimes [i 500]
            (do
              (io-pdm-write pdm/io-assoc "users-test" (str key-base i) (str i) (chan 1)))))))

(defn init
  "Called at server start"
  []
  (start-write-chan-allocator-loop))

