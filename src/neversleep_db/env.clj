(ns neversleep-db.env
  (:require [environ.core :as environ]))


(defonce env environ/env)

(defn merge-with-env! [a-map]
  (alter-var-root #'env (fn [x] (merge x a-map))))

(defn data-dir []
  (get-in env [:config :data-dir]))
