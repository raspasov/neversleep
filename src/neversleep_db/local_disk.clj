(ns neversleep-db.local-disk
  (:require [clojure.java.shell :as shell :refer [sh with-sh-dir]]
            [neversleep-db.env :as env]
            [neversleep-db.println-m :refer [println-m]]))


(defn delete-all-b-tree-root-logs! []
  (with-sh-dir (env/data-dir)
               (sh "rm" "-rf" "b_plus_tree_root_logs")
               (sh "mkdir" "b_plus_tree_root_logs")
               (sh "touch" "b_plus_tree_root_logs/.empty")))

(defn delete-all-command-logs! []
  (with-sh-dir (env/data-dir)
               (sh "rm" "-rf" "command_log")
               (sh "mkdir" "command_log")
               (sh "touch" "command_log/.empty")
               (sh "chmod" "-R" "777" "command_log/")))

;(defn scp-jar! []
;  (with-sh-dir (env/data-dir)
;               (sh "scp /Users/raspasov/Zend/workspaces/DefaultWorkspace/neversleep-db/target/uberjar/neversleep-db-1.0.0-alpha1-standalone.jar raspasov@10.0.1.157:/home/raspasov/debian/usr/bin/neversleep-db/neversleep-db-1.0.0-alpha1-standalone.jar")))