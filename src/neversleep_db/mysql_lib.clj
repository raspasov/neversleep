(ns neversleep-db.mysql-lib
  (:require [clojure.java.jdbc :as db-adapter]
            [neversleep-db.state :as state]
            [neversleep-db.println-m :refer [println-m]]
            [neversleep-db.env :as env])
  (:import com.jolbox.bonecp.BoneCPDataSource))


;MYSQL CONFIG
(defn mysql-config [] (-> env/env :config :mysql))
(defn db-spec []
  {:classname   "com.mysql.jdbc.Driver"
   :subprotocol "mysql"
   :subname     (str "//" ((mysql-config) :host) ":" ((mysql-config) :port) "/" ((mysql-config) :database-name))
   :user        ((mysql-config) :user)
   :password    ((mysql-config) :password)
   })
(defn pool
  "MySQL connection pool"
  [spec]
  (let [partitions 3
        cpds (doto (BoneCPDataSource.)
               (.setJdbcUrl (str "jdbc:" (:subprotocol spec) ":" (:subname spec)))
               (.setUsername (:user spec))
               (.setPassword (:password spec))
               (.setMinConnectionsPerPartition (inc (int (/ 4 partitions)))) ;min pool
               (.setMaxConnectionsPerPartition (inc (int (/ 20 partitions)))) ;max pool
               (.setPartitionCount partitions)
               (.setStatisticsEnabled true)
               ;; test connections every 25 mins (default is 240):
               (.setIdleConnectionTestPeriodInMinutes 25)
               ;; allow connections to be idle for 3 hours (default is 60 minutes):
               (.setIdleMaxAgeInMinutes (* 3 60))
               ;; consult the BoneCP documentation for your database:
               (.setConnectionTestStatement "/* ping *\\/ SELECT 1"))]
    {:datasource cpds}))

;(defonce pooled-db (delay (pool db-spec)))
(defn db-connection []
  (if (= nil @state/mysql-connection-pool)
    (reset! state/mysql-connection-pool (pool (db-spec))))
  @state/mysql-connection-pool)

;(defn- get-last-insert-id [db-connection]
;  (-> (db-adapter/query db-connection
;                        ["SELECT LAST_INSERT_ID() AS last_insert_id"])
;      (first)
;      :last_insert_id))
;
;(defn execute-return-last-insert-id!
;  ([sql-vector]
;   (db-adapter/with-db-transaction [db-connection (db-spec)]
;                                   (db-adapter/execute! db-connection
;                                                        sql-vector)
;                                   (get-last-insert-id db-connection))))


(defn execute!
  "Executes a query to MySQL, usually one that changes the database; uses Timbre to log any errors;
  sql-vector is in the format [query-string-with-?-params param1 param2 etc]
  If the operation fails with an exception, the function calls itself once to retry, might happen if the MySQL connection has timed out"
  ([sql-vector]
   (db-adapter/execute! (db-connection)
                        sql-vector)))

(defn query
  "Executes a query to MySQL, usually one that doesn't change any data such as SELECT; uses Timbre to log any errors;
  sql-vector is in the format [query-string-with-?-params param1 param2 etc];
  If the operation fails with an exception, the function calls itself once to retry, might happen if the MySQL connection has timed out"
  ([sql-vector]
   (db-adapter/query (db-connection)
                     sql-vector)))
