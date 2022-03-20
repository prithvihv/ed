(ns groww-scraper.core
  (require '[clj-http.client :as client]
           '[cheshire.core :as json]
           '[environ.core :refer [env]]
           '[clj-time.core :as t]
           '[clj-time.coerce :as tc]
           '[next.jdbc :as jdbc]
           '[next.jdbc.types :as jdbc.types]
           '[honey.sql :as sql]
           '[honey.sql.helpers :as sqlh])
  (:gen-class))

(def db {:dbtype "postgres" :dbname "ed" :user "postgres" :pass "postgres"})
(def ds (jdbc/get-datasource db))

(defn mock-get-dashboard-mf
  []
  (json/decode (slurp "/home/phv/code/src/github.com/prithvihv/ed/groww-scraper/jsons/dashboard_mf.json")))

(defn mock-get-txn-mf
  []
  (json/decode (slurp "/home/phv/code/src/github.com/prithvihv/ed/groww-scraper/jsons/transaction_mf.json")))


;; http sdk?
(defn get-dashboard-mf
  "get dashboard related data for mutaul funds"
  []
  (let [auth-token (env :groww-auth-token)
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
  (let [auth-token (env :groww-auth-token)
        capaign-token (env :groww-campaign-token)]
    (json/decode
     ((client/get "https://groww.in/v1/api/portfolio/v1/transaction/scheme/all"
                  {:headers {"Authorization" (str "Bearer " auth-token)
                             "X-USER-CAMPAIGN" (str "Bearer " capaign-token)}
                   :query-params {"folio_number" folio-number
                                  "scheme_code" scheme-code
                                  "page" 0
                                  "size" 50}})
      :body))))

(defn get-inv-for-txn-mf
  "get inv obj from transaction for mf"
  [dashboard-detials mf-txn]
  {:schema_code (mf-txn "scheme_code")
   :buy_price (mf-txn "transaction_price")
   :buy_date (tc/from-string (mf-txn "transaction_time"))
   :qty (mf-txn "units")
   :total_amount (mf-txn "transaction_time")})

(defn sql-upsert-asset-mf
  "upsert assets"
  [schemas]
  (-> (sqlh/insert-into :asset_holding_type_mapping [:schema_code :name :type])
      (sqlh/values schemas)
      (sqlh/upsert (-> (sqlh/on-conflict :schema_code)
                       (sqlh/do-update-set :name :type)))
      (sql/format {:pretty true})))


(defn get-asset-details-mf-from-dashboard
  ""
  [d-details]
  (let [type-mapping {"120503" "stocks-elss"}
        scheme-code (d-details "scheme_code")]
    {:schema_code scheme-code
     :name (d-details "scheme_name")
     :type (-> (get type-mapping scheme-code "stocks-mf")
               (name)
               (jdbc.types/as-other))}))

;;
(defn mf-main
  []
  (->> (-> (mock-get-dashboard-mf)
           (get-in ["investments" "portfolio_schemes"]))
       (map get-asset-details-mf-from-dashboard)
       (map vals)
       (sql-upsert-asset-mf)
       (jdbc/execute-one! ds)))

(defn -main
  "I don't do a whole lot ... yet."
  [& args])
