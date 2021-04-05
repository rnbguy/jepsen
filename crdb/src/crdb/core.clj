(ns crdb.core
(:require [clojure.tools.logging :refer :all]
          [clojure.string :as str]
          [slingshot.slingshot :refer [try+]]
          [jepsen [cli :as cli]
                  [control :as c]
                  [client :as client]
                  [db :as db]
                  [core :as jepsen]
                  [generator :as gen]
                  [nemesis :as nemesis]
                  [tests :as tests]]
          [crdb.tpcc-utils :as tpcc]
          [crdb.sqldump :as sqldump]
          [crdb.tpcc-asserts :as asserts]
          [jepsen.control.util :as cu]
          [jepsen.os.debian :as debian])
)

(def mysql-dir "/var/lib/mysql")
(def mysql-stock-dir "/var/lib/mysql-stock")

(def cnf-file "/etc/mysql/mariadb.conf.d/50-server.cnf")
(def cnf-stock-file "/etc/mysql/mariadb.conf.d/50-server.cnf.stock")

(def log-files
  ["/var/log/syslog"
   "/var/log/mysql.log"
   "/var/log/mysql.err"
   "/var/lib/mysql/queries.log"])

(defn eval!
  "Evals a mysql string from the command line."
  [s]
  (c/exec :mysql :-u "root" :-e s))

(defn setup-db!
  "Adds a jepsen database to the cluster."
  [node]
  (eval! (str
  "CREATE DATABASE IF NOT EXISTS jepsen;"
  "CREATE USER  'jepsen'@'%';"
  "GRANT ALL PRIVILEGES ON *.* TO 'jepsen'@'%' WITH GRANT OPTION;"
  "FLUSH PRIVILEGES;"))
  )

(defn db
  "galera DB for a particular version."
  [version]
  (reify db/DB
    (setup! [_ test node]
      (c/su
        (when-not (debian/installed? :mariadb-server)
          (c/exec :apt-get :-y :update)
          (c/exec :apt-get :-y :upgrade)
          (debian/install [:mariadb-server])
        )

        (c/exec :service :mysql :stop)
        (when-not (cu/exists? mysql-stock-dir)
          (c/exec :cp :-rp mysql-dir mysql-stock-dir)
        )
        (when-not (cu/exists? cnf-stock-file)
          (c/exec :cp :-p cnf-file cnf-stock-file)
        )

        (c/exec :echo (str "
[galera]
wsrep_on=ON
wsrep_provider=/usr/lib/galera/libgalera_smm.so
wsrep_cluster_address=gcomm://" (when (not= node (jepsen/primary test)) (jepsen/primary test)) "
binlog_format=row
default_storage_engine=InnoDB
innodb_autoinc_lock_mode=2
innodb_doublewrite=1
query_cache_size=0
bind_address=0.0.0.0
wsrep_cluster_name=\"galera_cluster\"
wsrep_node_address=\"" node "\"
        ") :>> cnf-file)

        (when (= node (jepsen/primary test))
        (c/exec :service :mysql :bootstrap)
        )

        (when (= node (jepsen/primary test)) (setup-db! node) (eval! (str "USE jepsen;" sqldump/sqldump)))

        (jepsen/synchronize test)

        (when (not= node (jepsen/primary test))
        (c/exec :service :mysql :start)
        )

        (jepsen/synchronize test)

        ;; (info node "sleeping")
        ;; (Thread/sleep 30000)
        ;; (info node "woke")
      )
    )

    (teardown! [_ test node]
      (info node "tearing down galera")
      (try
        (when (= node (jepsen/primary test))
          (info node "[ASSERT 01]" (eval! (str "USE jepsen;" asserts/cr01)))
          (info node "[ASSERT 02]" (eval! (str "USE jepsen;" asserts/cr02)))
          (info node "[ASSERT 03]" (eval! (str "USE jepsen;" asserts/cr03)))
          (info node "[ASSERT 04]" (eval! (str "USE jepsen;" asserts/cr04)))
          (info node "[ASSERT 05]" (eval! (str "USE jepsen;" asserts/cr05)))
          (info node "[ASSERT 06]" (eval! (str "USE jepsen;" asserts/cr06)))
          (info node "[ASSERT 07]" (eval! (str "USE jepsen;" asserts/cr07)))
          (info node "[ASSERT 08]" (eval! (str "USE jepsen;" asserts/cr08)))
          (info node "[ASSERT 09]" (eval! (str "USE jepsen;" asserts/cr09)))
          (info node "[ASSERT 10]" (eval! (str "USE jepsen;" asserts/cr10)))
          (info node "[ASSERT 11]" (eval! (str "USE jepsen;" asserts/cr11)))
          (info node "[ASSERT 12]" (eval! (str "USE jepsen;" asserts/cr12)))
        )
        (catch Exception e (info node "[ASSERT] prolly this is beginning of run"))
      )
      (c/exec :service :mysql :stop :|| :echo "prolly not started")
      ;; (jepsen/synchronize test)
      (when (cu/exists? mysql-stock-dir)
        (c/exec :rm :-rf mysql-dir)
        (c/exec :cp :-rp mysql-stock-dir mysql-dir))
      (when (cu/exists? cnf-stock-file)
        (c/exec :rm :-f cnf-file)
        (c/exec :cp :-p cnf-stock-file cnf-file))
      (apply c/exec :truncate :-c :--size 0 log-files)
      )))

(defn no [_ _] {:type :invoke, :f :NO})
(defn pm [_ _] {:type :invoke, :f :PM})
(defn os [_ _] {:type :invoke, :f :OS})
(defn dv [_ _] {:type :invoke, :f :DV})
(defn sl [_ _] {:type :invoke, :f :SL})

(defn parse-long
  "Parses a string to a Long. Passes through `nil`."
  [s]
  (when s (Long/parseLong s)))

(defrecord Client [conn]
  client/Client
  (open! [this test node] (
    assoc this
      :conn
      (java.sql.DriverManager/getConnection (str "jdbc:mysql://" node ":3306/jepsen") "jepsen" "")))


  (setup! [this test]
  ;; (let [stmt (.createStatement (:conn this))] (.executeUpdate stmt "CREATE TABLE IF NOT EXISTS variables") (.close stmt))
  (new tpcc.Utils_tpcc 0))

  (invoke! [this test op]
    (info "invoking" op)
    (case (:f op)
      :NO ((:javaFunc (nth tpcc/operationMap 0)) (:conn this) (tpcc/getNextArgs 1))
      :PM ((:javaFunc (nth tpcc/operationMap 1)) (:conn this) (tpcc/getNextArgs 2))
      :OS ((:javaFunc (nth tpcc/operationMap 2)) (:conn this) (tpcc/getNextArgs 3))
      :DV ((:javaFunc (nth tpcc/operationMap 3)) (:conn this) (tpcc/getNextArgs 4))
      :SL ((:javaFunc (nth tpcc/operationMap 4)) (:conn this) (tpcc/getNextArgs 5))
    )
    (assoc op :type :ok)
  )

  (teardown! [this test])

  (close! [this test] (.close (:conn this))))

(defn galera-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (merge tests/noop-test
         opts
         {:name "galera"
          :os   debian/os
          :db   (db "10.5.9")
          :client (Client. nil)
          :generator (->> (gen/mix [no pm os dv sl])
                    (gen/stagger 1)
                    (gen/nemesis nil)
                    (gen/time-limit 15))
          :pure-generators true}))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn galera-test}) (cli/serve-cmd))
            args))
