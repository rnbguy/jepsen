(defproject crdb "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :main crdb.core
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [jepsen "0.2.4-SNAPSHOT"]
                 [verschlimmbesserung "0.1.3"]]
  :java-source-paths ["/home/ranadeep/phd/projects/jepsen/crdb/Jepsen_Java_Tests/src/main/java"]
  :repl-options {:init-ns crdb.core})
