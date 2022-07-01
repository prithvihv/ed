(ns groww-scraper.http-client
  (:require [cheshire.core :as json]
            [clj-http.client :as client]
            [clj-time.coerce :as tc]
            [clj-time.core :as t]
            [environ.core :refer [env]]))

(def groww-auth-envs (atom {:auth-token (env :groww-auth-token),
                            :campaign-token (env :groww-campaign-token)}))

(defn set-groww-auth-envs
  "helper to set groww auth envs"
  [new-env]
  (swap! groww-auth-envs (fn [old] (merge old new-env))))

;; http sdk?
(defn get-dashboard-mf
  "get dashboard related data for mutaul funds"
  []
  (let [auth-token (:auth-token @groww-auth-envs)
        act-time (tc/to-long (t/now))]
    (json/decode
     ((client/get "https://groww.in/v1/api/portfolio/v2/dashboard"
                  {:headers {"Authorization" (str "Bearer " auth-token)}
                   :query-params {"actTime" act-time
                                  "list_tracked" (str true)}})
      :body))))

(defn get-txns-for-mf
  "assumes that only 1 page of size is present
   FIXME: add pagination"
  [folio-number scheme-code]
  (let [auth-token (:auth-token @groww-auth-envs)
        capaign-token (:campaign-token @groww-auth-envs)]
    (json/decode
     ((client/get "https://groww.in/v1/api/portfolio/v1/transaction/scheme/all"
                  {:headers {"Authorization" (str "Bearer " auth-token)
                             "X-USER-CAMPAIGN" (str "Bearer " capaign-token)}
                   :query-params {"folio_number" folio-number
                                  "scheme_code" scheme-code
                                  "page" 0
                                  "size" 50}})
      :body))))

(defn get-mf-details
  "get mf details"
  [search-id]
  (-> (client/get (str "https://groww.in/v1/api/data/mf/web/v2/scheme/search/" search-id))
      (:body)
      (json/decode)))

(defn http-get-mf-tick-data
  "get mf details"
  ([scheme-code months]
   (-> (str
        "https://groww.in/v1/api/data/mf/web/v1/scheme/" scheme-code
        "/graph?benchmark=false&months=" months)
       (client/get)
       (:body)
       (json/decode))))