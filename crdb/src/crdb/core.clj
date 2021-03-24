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
          [jepsen.control.util :as cu]
          [jepsen.os.debian :as debian])
)

(defn listen-addr
  [node]
  (str node ":" 26257))

(defn http-addr
  [node]
  (str node ":" 8080))

(defn initial-cluster
  "Constructs an initial cluster string for a test, like
  \"foo:2380,bar:2380,...\""
  [test]
  (->> (:nodes test)
       (map listen-addr)
       (str/join ",")))

(def dir     "/usr/local/mysql")
(def binary "bin/mysqld_safe")
(def logfile "/opt/galera.log")
(def pidfile "/opt/galera.pid")
(def mysqlbin "bin/mysql")

(defn cluster-address
  "Connection string for a test."
  [test]
  (str "gcomm://" (str/join "," (map name (:nodes test)))))

(defn eval!
  "Evals a mysql string from the command line."
  [s]
  (c/cd dir (c/exec mysqlbin :-u "root" :-e s)))

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
      (info node "installing galera" version)
      (c/su
        (let [url (str "https://ftp.igh.cnrs.fr/pub/mariadb//mariadb-" version "/bintar-linux-x86_64/mariadb-" version "-linux-x86_64.tar.gz")]
          (cu/install-archive! url "/usr/local/mysql"))

        
        (debian/install [:libaio1 :libtinfo5])

        (c/cd dir (c/exec "scripts/mysql_install_db"))

        (cu/start-daemon!
          {:logfile logfile
           :pidfile pidfile
           :chdir   dir}
          binary :--user=root
          )

        (Thread/sleep 5000)
        (setup-db! node)

        ;; ((Thread/sleep 5000)
        ;; (if (= node "n1") (c/exec binary :init :--insecure :--host (listen-addr "n1")))
        (Thread/sleep 30000)
      )
    )

    (teardown! [_ test node]
      (info node "tearing down galera")
      (when (cu/exists? dir) (c/cd dir (c/exec "bin/mysqladmin" :shutdown)))
      (cu/stop-daemon! binary pidfile)
      ;; (c/exec binary :quit :--insecure :--host (listen-addr node))
      (c/su (c/exec :rm :-rf dir)))))

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
  (let [stmt (.createStatement (:conn this))] (.executeUpdate stmt "CREATE TABLE IF NOT EXISTS variables") (.close stmt))
  (new tpcc.Utils_tpcc 30))

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