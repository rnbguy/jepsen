(ns crdb.core
(:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [verschlimmbesserung.core :as v]
            [slingshot.slingshot :refer [try+]]
            [jepsen [cli :as cli]
                    [control :as c]
                    [client :as client]
                    [db :as db]
                    [generator :as gen]
                    [nemesis :as nemesis]
                    [tests :as tests]]
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
          :--background
          )
      )

      ((Thread/sleep 5000)
      (if (= node "n1") (c/exec binary :init :--insecure :--host (listen-addr "n1")))
      (Thread/sleep 30000))
    )

    (teardown! [_ test node]
      (info node "tearing down cockroach")
      (cu/stop-daemon! binary pidfile)
      ;; (c/exec binary :quit :--insecure :--host (listen-addr node))
      (c/su (c/exec :rm :-rf dir)))))

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defn parse-long
  "Parses a string to a Long. Passes through `nil`."
  [s]
  (when s (Long/parseLong s)))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (v/connect (http-addr node)
                                 {:timeout 5000})))

  (setup! [this test])

  (invoke! [this test op]
      (case (:f op)
        :read (assoc op :type :ok, :value (parse-long (v/get conn "foo")))
        :write (do (v/reset! conn "foo" (:value op))
                   (assoc op :type :ok))
        :cas (try+
               (let [[old new] (:value op)]
                 (assoc op :type (if (v/cas! conn "foo" old new)
                                   :ok
                                   :fail)))
               (catch [:errorCode 100] ex
                 (assoc op :type :fail, :error :not-found)))))

  (teardown! [this test])

  (close! [_ test]))

(defn cockroach-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (merge tests/noop-test
         opts
         {:name "cockroach"
          :os   debian/os
          :db   (db "v20.2.5")
          :pure-generators true}))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn cockroach-test}) (cli/serve-cmd))
            args))