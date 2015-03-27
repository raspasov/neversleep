(ns neversleep-db.core
  (:require [neversleep-db.aleph-netty :as aleph-netty]
            [neversleep-db.env :as env]
            [neversleep-db.node-keeper :as node-keeper]
            [neversleep-db.command-log :as command-log]
            [neversleep-db.b-plus-tree-root-cache :as bpt-root-cache]
            [neversleep-db.b-plus-tree-root-log :as bpt-root-log]
            [neversleep-db.b-plus-tree-root-log-remote :as bpt-root-log-remote]
            [neversleep-db.persistent-durable-map :as pdm]
            [neversleep-db.b-plus-tree-batch :as bpt-b]
            [neversleep-db.to-disk :as to-disk]
            [neversleep-db.println-m :refer [println-m]]
            [clojure.tools.nrepl.server :as nrepl :refer [start-server stop-server]]
            [clojure.core.async :refer [chan go >! <! <!! >!! go-loop put! thread alts! alts!! timeout pipeline close!]]
            [neversleep-db.util :as util]
            [neversleep-db.write-dispatch]
            [neversleep-db.read-dispatch]
            [neversleep-db.io-utils :as io-utils]
            [taoensso.timbre :as timbre]
            [neversleep-db.time]
            [neversleep-db.backpressure :as backpressure]
            [neversleep-db.exception-log :as exception-log]
            [neversleep-db.state :as state]
            [neversleep-db.read-dispatch :as read-dispatch]
            [neversleep-db.write-dispatch :as write-dispatch])
  (:gen-class)
  (:import (com.google.protobuf ByteString)
           (clojure.lang APersistentMap)))

;init vars
(pdm/init)
(bpt-b/init)

(declare server-start)

;(def config (environ/env :config))
;logging config
(defn configure-timbre-logging []
  (timbre/set-level! :info)
  (timbre/set-config! [:appenders :spit :enabled?] true)
  (timbre/set-config! [:shared-appender-config :spit-filename] (get-in env/env [:config :error-log])))
;end logging config




(defn -main
  [& args]
  ;always use original config first
  (env/merge-with-env! (read-string (slurp "/etc/neversleep-db/config-original.clj")))
  ;merge user-defined config (if specified)
  (let [config-path (first args)]
    (when (string? config-path)
      (env/merge-with-env! (read-string (slurp config-path)))))
  (println-m "Nvrslp Db 0.1-alpha")
  ;start the exception logger
  (configure-timbre-logging)
  (exception-log/start-exception-logger)
  ;server start
  (server-start)
  (reset! state/tcp-server (aleph-netty/start-server (aleph-netty/tcp-api-handler) 10000))
  (defonce nrepl-server (nrepl/start-server :port 9998 :bind "127.0.0.1"))
  #_(defonce aleph-http-server (aleph-http/start-server handler {:port 8080}))
  #_(def aleph-tcp-server (aleph-tcp/start-server tcp-handler {:port 8081})))


(defn finalize-server-start []
  ;API OK
  (backpressure/start-writes)
  ;start the pipelines
  (node-keeper/start-pipelines))


(defn do-all-commands
  "Returns true if all writes succeeded, false otherwise"
  [log-files]
  (let [commands (command-log/get-all-commands log-files)
        all-writes-ok? (atom true)]
    (doseq [{:keys [entitytype command params]} commands :when (= true @all-writes-ok?)]
      (let [confirm-ch (chan 1)
            params (-> ^ByteString params (.toByteArray) (util/de-serialize))]
        (condp = entitytype
          1 (condp = command
              "io-assoc" (apply pdm/io-assoc-startup (conj params confirm-ch))
              "io-without" (apply pdm/io-without-startup (conj params confirm-ch))
              (>!! exception-log/incoming-exceptions (Exception. (str "unrecognized command " command " for entity type " entitytype))))
          ;else
          (>!! exception-log/incoming-exceptions (Exception. (str "unrecognized entity type " entitytype))))
        (when-not (instance? APersistentMap (util/<!!-till-timeout confirm-ch 20000))
          ;log the exception
          (>!! exception-log/incoming-exceptions (Exception.
                                                   (str "Write failed during command-log recovery")))
          ;fail
          (reset! all-writes-ok? false))))
    @all-writes-ok?))

(defn recover-command-logs
  "Runs at every server start.
   The commit point of the server is determined by last-saved-blob-id (from pipeline).
   The steps are the following:
   1. Delete all b-tree-root-logs that are bigger than last-saved-blob-id
   2. Delete all command-logs that are smaller than or equal to last-saved-blob-id
   3. Run do-all-commands which tries to recover all the writes
   4. Delete command-logs"
  []
  (finalize-server-start)

  (let [last-saved-blob-id (io-utils/blocking-loop to-disk/get-last-saved-blob-id)
        ;step 1 - local root logs
        _ (bpt-root-log/delete-logs-bigger-than last-saved-blob-id)
        ;step 1-1 - remote root logs
        _ (bpt-root-log-remote/delete-logs-bigger-than last-saved-blob-id)
        ;step 2
        _ (command-log/delete-logs-smaller-than-or-equal-to last-saved-blob-id)
        log-files (command-log/get-logs-bigger-than last-saved-blob-id)
        ;step 3
        recovery-result (do-all-commands log-files)]
    ;did the server start ok?
    (if (not= true recovery-result)
      (do (>!! exception-log/incoming-exceptions (Exception. "Server failed to start - unable to recover writes from command log."))
          ;return
          (println-m "RECOVERED FAIL")
          false)
      ;return
      (do
        ;TODO STEPS FOR MORE SAFETY
        ;1. copy logs
        ;2. only then try to delete logs
        ;delete old logs
        (command-log/delete-logs log-files)
        (println-m "RECOVERED OK")
        true))))

(defn server-start []
  (let [last-saved-blob-id (io-utils/blocking-loop to-disk/get-last-saved-blob-id)
        storage-check (io-utils/blocking-loop to-disk/check-blob-id-value-local last-saved-blob-id)]
    ;check the local blob id value
    (when-not storage-check
      (let [e (Exception. "WARNING: last-saved-blob-id > current-blob-id, writer server is likely connected to the wrong storage backend, aborting startup")]
        (>!! exception-log/incoming-exceptions e)
        (throw e)))
    ;start read pipeline
    (read-dispatch/init)
    ;load the root cache
    (bpt-root-cache/init last-saved-blob-id)
    ;node keeper
    (node-keeper/init)
    ;start the command log write loop
    (command-log/init)
    ;start the writer dispatch write loop
    (write-dispatch/init)
    ;recover procedure & start
    (recover-command-logs)))

(defn simulate-server-restart! []
  (state/reset-state!)
  (server-start))

;TODO remove when building uberjar
(-main)


