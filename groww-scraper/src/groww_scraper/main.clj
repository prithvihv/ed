(ns groww-scraper.main
  (:require [clj-time.coerce :as tc]
            [clj-time.core :as t]
            [groww-scraper.fin :as fin]
            [groww-scraper.http-client :as http-client]
            [groww-scraper.repo :as repo]
            [groww-scraper.http :as handler]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [ring.adapter.jetty :as jetty])
  (:gen-class))

(defn -main
  "I dont do a whole lot ... yet."
  [& _args]
  (do
    (jetty/run-jetty handler/handler {:port 3100
                                      :join? false})))
