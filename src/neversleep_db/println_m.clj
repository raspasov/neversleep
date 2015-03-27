(ns neversleep-db.println-m
  (:require [neversleep-db.env :as env]))

(defn println-enabled? [] (-> env/env :config :println-enabled?))

(defmacro println-m [& params]
  `(if (println-enabled?)
     (println ~@params)))