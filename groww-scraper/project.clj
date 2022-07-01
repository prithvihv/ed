(defproject groww-scraper "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [clj-http "3.12.3"]
                 [cheshire "5.10.2"]
                 [environ "1.2.0"]
                 [clj-time "0.15.2"]
                 [com.github.seancorfield/next.jdbc "1.2.772"]
                 [org.postgresql/postgresql "42.2.10"]
                 [com.github.seancorfield/honeysql "2.2.868"]
                 [hiccup "1.0.5"]

                 ;; http server set up
                 [ring/ring-core "1.8.2"]
                 [ring/ring-jetty-adapter "1.8.2"]]
  :main ^:skip-aot groww-scraper.main
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all
                       :jvm-opts ["-Dclojure.compiler.direct-linking=true"]}})