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

(def dir     "/opt/cockroach")
(def binary (str dir "/cockroach"))
(def logfile (str dir "/cockroach.log"))
(def pidfile (str dir "/cockroach.pid"))


(defn db
  "cockroach DB for a particular version."
  [version]
  (reify db/DB
    (setup! [_ test node]
      (info node "installing cockroach" version)
      (c/su
        (let [url (str "https://binaries.cockroachdb.com/cockroach-" version ".linux-amd64.tgz")]
          (cu/install-archive! url dir))

        (cu/start-daemon!
          {:logfile logfile
           :pidfile pidfile
           :chdir   dir}
          binary
          :start
          :--insecure
          :--store (name node)
          :--listen-addr (listen-addr node)
          :--http-addr (http-addr node)
          :--join (initial-cluster test)
          ;; :--background
          )

        (when (= node (jepsen/primary test))
        (c/exec binary :init :--insecure :--host (listen-addr node)))

        ;; ((Thread/sleep 5000)
        ;; (if (= node "n1") (c/exec binary :init :--insecure :--host (listen-addr "n1")))
        (Thread/sleep 35000)
      )
    )

    (teardown! [_ test node]
      (info node "tearing down cockroach")
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
      (java.sql.DriverManager/getConnection (str "jdbc:postgresql://" (listen-addr node) "/?sslmode=disable") "root" "")))


  (setup! [this test] (let [stmt (.createStatement (:conn this))] (.executeUpdate stmt "create database if not exists variables") (.close stmt)))

  (invoke! [this test op]
    (info "invoking" op)
    (case (:f op)
      :NO (:javaFunc (nth tpcc/operationMap 0) (:conn this) (tpcc/getNextArgs 0))
      :PM (:javaFunc (nth tpcc/operationMap 1) (:conn this) (tpcc/getNextArgs 1))
      :OS (:javaFunc (nth tpcc/operationMap 2) (:conn this) (tpcc/getNextArgs 2))
      :DV (:javaFunc (nth tpcc/operationMap 3) (:conn this) (tpcc/getNextArgs 3))
      :SL (:javaFunc (nth tpcc/operationMap 4) (:conn this) (tpcc/getNextArgs 4))
    )
    (assoc op :type :ok)
  )

  (teardown! [this test])

  (close! [this test] (.close (:conn this))))

(defn cockroach-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (merge tests/noop-test
         opts
         {:name "cockroach"
          :os   debian/os
          :db   (db "v20.2.5")
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
  (cli/run! (merge (cli/single-test-cmd {:test-fn cockroach-test}) (cli/serve-cmd))
            args))