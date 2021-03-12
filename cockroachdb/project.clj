(defproject cockroachdb "0.1.0"
  :description "Jepsen testing for CockroachDB"
  :url "http://cockroachlabs.com/"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [jepsen "0.2.4-SNAPSHOT"]
                 [org.clojure/java.jdbc "0.7.12"]
                 [circleci/clj-yaml "0.6.0"]
                 [org.postgresql/postgresql "42.2.19"]]
  :jvm-opts ["-Xmx12g"
             "-XX:+CMSParallelRemarkEnabled"
             "-XX:MaxInlineLevel=32"
             "-XX:MaxRecursiveInlineLevel=2"
             "-server"]
  :main jepsen.cockroach.runner
  :aot [jepsen.cockroach.runner
        clojure.tools.logging.impl])
