(ns neversleep-db.backpressure
  "Allows a form of backpressure by dropping read or write requests if certain conditions occur,
  such as IO exceptions during writing.")

;writes only are allowed to happen when this is = 0
(def write-block-cnt (atom -1))

(defn ok-to-write? []
  (= 0 @write-block-cnt))

(defn block-writes []
  (swap! write-block-cnt inc))

(defn unblock-writes []
  (swap! write-block-cnt dec))

(defn start-writes []
  (reset! write-block-cnt 0))