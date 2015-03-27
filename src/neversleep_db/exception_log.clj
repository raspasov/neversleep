(ns neversleep-db.exception-log
  (:require [clojure.core.async :refer [chan go >! <! <!! >!! go-loop put! thread alts! alts!! timeout pipeline close!]]
            [taoensso.timbre :as timbre]
            [neversleep-db.println-m :refer [println-m]]))

(def incoming-exceptions (chan 1024))


(def loop-started? (atom false))
(defn start-exception-logger []
  (println "STARTING EXCEPTION LOGGER")
  (when (= false @loop-started?)
    (go (loop []
              (let [e (<! incoming-exceptions)]
                ;ensure it's an exception
                (when (instance? Exception e)
                  ;(println-m (.getMessage e))
                  (timbre/error e (.getMessage ^Exception e)))
                (recur))))
    (reset! loop-started? true)))


