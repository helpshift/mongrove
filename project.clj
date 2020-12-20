(defproject helpshift/mongrove "0.1.0-SNAPSHOT"
  :description "An idiomatic Clojure wrapper for MongoDB java-driver 4.x"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [org.mongodb/mongodb-driver-sync "4.1.1"]
                 [org.clojure/tools.logging "1.1.0"]]
  :repl-options {:init-ns mongrove.core}
  :jvm-opts ^:replace ["-Duser.timezone=UTC"
                       "-Dloglevel=DEBUG"
                       "-Ddebuglogfile=debug.log"]
  :profiles {:dev {:dependencies [[clj-time "0.15.2"]
                                  [circleci/bond "0.3.1"]
                                  [org.clojure/test.check "1.1.0"]
                                  [com.gfredericks/test.chuck "0.2.10"]]}}
  :plugins [[lein-codox "0.10.7"]])
