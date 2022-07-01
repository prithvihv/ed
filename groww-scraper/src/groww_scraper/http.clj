(ns groww-scraper.http
  (:require [ring.util.codec :as codec]
            [cheshire.core :as json]
            [groww-scraper.http-client :as http-client]
            [groww-scraper.core :as core])
  (:use hiccup.core))


(defn handler [request]
  (case (:uri request)
  ;; was able to move this part to grafana 🥳
;;     "/" {:status 200
;;          :headers {"Content-Type" "text/html"}
;;          :body (html
;;                 [:header [:script  "
;; async function storeAPIKeys(){
;;   let res = await fetch('/api-keys?' + new URLSearchParams({
;;     'auth-token': document.getElementById('authToken').value,
;;     'campaign-token': document.getElementById('campaignKey').value,
;;   }))
;;    document.getElementById('out').innerText = JSON.stringify(await res.json(),null, '\\t')
;; }
;; "]]
;;                 [:body
;;                  [:input {:type "text", :id "authToken", :placeholder "auth-token"}] [:br]
;;                  [:input {:type "text", :id "campaignKey", :placeholder "campaign-key"}] [:br]
;;                  [:button {:onclick "storeAPIKeys()"} "load"] [:br]
;;                  [:div {:id "out"}] [:br]])}
    "/api-keys" {:status 200
                 :headers {"Content-Type" "application/json"}
                 :body (->> (:query-string request)
                            (codec/form-decode)

                            ;; hacky way to get keyword list
                            (json/encode)
                            (json/decode)

                            (http-client/set-groww-auth-envs)
                            (json/encode))}
    "/reload-data" {:status 200
                    :headers {"Content-Type" "application/json"}
                    :body (json/encode (core/store-inv-mf))}
    {:status 400,
     :headers {"Content-Type" "application/json"}
     :body (json/encode {:data "i dont know this route"})}))