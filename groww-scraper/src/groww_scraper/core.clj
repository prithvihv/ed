(ns groww-scraper.core
  (:require [clj-http.client :as client]
            [cheshire.core :as json]
            [environ.core :refer [env]]
            [clj-time.core :as t]
            [clj-time.coerce :as tc]
            [next.jdbc :as jdbc]
            [next.jdbc.types :as jdbc.types]
            [next.jdbc.date-time :as jdbc.date]
            [honey.sql :as sql]
            [honey.sql.helpers :as sqlh])
  (:gen-class))

(def db {:dbtype "postgres" :dbname "ed" :user "postgres" :pass "postgres"})
(def ds (jdbc/get-datasource db))

(defn mock-get-dashboard-mf
  []
  (json/decode (slurp "/home/phv/code/src/github.com/prithvihv/ed/groww-scraper/jsons/dashboard_mf.json")))

(defn mock-get-txn-mf
  []
  (json/decode (slurp "/home/phv/code/src/github.com/prithvihv/ed/groww-scraper/jsons/transaction_mf.json")))


(defn mock-get-mf-info
  []
  (json/decode (slurp "/home/phv/code/src/github.com/prithvihv/ed/groww-scraper/jsons/mf-info.json")))


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

(defn get-mf-details
  "get mf details"
  [search-id]
  (let [auth-token (env :groww-auth-token)
        capaign-token (env :groww-campaign-token)]
    (json/decode
     ((client/get (str "https://groww.in/v1/api/data/mf/web/v2/scheme/search/" search-id)
                  {:headers {"Authorization" (str "Bearer " auth-token)
                             "X-USER-CAMPAIGN" (str "Bearer " capaign-token)}})
      :body))))

(defn new-inv-for-mf
  "get inv obj from transaction for mf"
  [mf-txn]
  {:schema_code (mf-txn "scheme_code")
   :txn_price (mf-txn "transaction_price")
   :txn_date (-> (mf-txn "transaction_time")
                 (tc/from-string)
                 (tc/to-timestamp))
   :qty (mf-txn "units")
   :total_amount (mf-txn "transaction_amount")
   :txn_id (mf-txn "transaction_id")
   :txn_type (->  (mf-txn "transaction_type")
                  (jdbc.types/as-other))})

(defn new-asset-details-for-mf
  ""
  [mf-info]
  (let [type-mapping {"120503" "stocks-elss"}
        scheme-code (mf-info "scheme_code")]
    {:schema_code scheme-code
     :name (mf-info "scheme_name")
     :type (-> (get type-mapping scheme-code "stocks-mf")
               (name)
               (jdbc.types/as-other))
     :expense_ratio (-> (mf-info "expense_ratio") 
                     (read-string))
     :nav (mf-info "nav")}))


(defn sql-upsert-inv-mf
  ""
  [invs]
  (-> (sqlh/insert-into :investments_log)
      (sqlh/values invs)
      (sqlh/upsert (-> (sqlh/on-conflict :txn_id)
                       (sqlh/do-update-set
                        :txn_price
                        :txn_date
                        :qty
                        :total_amount))) ;; FIXME
      (sql/format)))

(defn sql-upsert-asset-mf
  "upsert assets"
  [schemas]
  ;; FIXME: why do we need to manual add columns here
  (-> (sqlh/insert-into :asset_holding_type_mapping  [:schema_code
                                                      :name
                                                      :type
                                                      :expense_ratio
                                                      :nav])
      (sqlh/values schemas)
      (sqlh/upsert (-> (sqlh/on-conflict :schema_code)
                       (sqlh/do-update-set :name :type :expense_ratio :nav)))
      (sql/format)))


(defn store-inv-from-mf
  "load investmenst from mf details"
  [folio-number scheme-code]
  (->> (-> (get-txns-for-mf folio-number scheme-code)
           (get-in ["data" "transaction_list"]))
       (map new-inv-for-mf)
       (sql-upsert-inv-mf)
       (jdbc/execute-one! ds)))

(defn store-inv-mf
  "everything mf"
  []
  (let [dashboard-mf (get-dashboard-mf)]
    (do
    ;; load asset infomation
      (->> (get-in dashboard-mf ["investments" "portfolio_schemes"])
           (map (fn [n] (n "search_id")))
           (distinct)
           (map get-mf-details)
           (map new-asset-details-for-mf)
           (map vals)
           (sql-upsert-asset-mf)
           (jdbc/execute-one! ds))
   ;; load inv
      (->> (get-in dashboard-mf ["investments" "portfolio_schemes"])
           (map (fn [mf] [(mf "folio_number") (mf "scheme_code")]))
           (map (fn [mf-details] (apply store-inv-from-mf mf-details)))))))

(defn play-ground-threads
  []
  (->> (let [mf-details (-> (mock-get-dashboard-mf)
                            (get-in ["investments" "portfolio_schemes"]))
             scheme-infos (->> (map (fn [n] (n "search_id")) mf-details)
                               (distinct)
                               (map (fn [_] (mock-get-mf-info)))
                              ;;  (map (fn [n] {n (mock-get-mf-info)}))
                               )]
         scheme-infos)
       (map new-asset-details-for-mf)
       (map vals)
       (sql-upsert-asset-mf)
       (jdbc/execute-one! ds))

  ;; insert inv of mf
  (->> (-> (mock-get-txn-mf)
           (get-in ["data" "transaction_list"]))
       (map new-inv-for-mf)
       (sql-upsert-inv-mf)
       (jdbc/execute-one! ds))

  (-> (mock-get-mf-info)
      (get-in ["nav"]))

  ;; new mf thread
  (->> (get-in (mock-get-dashboard-mf) ["investments" "portfolio_schemes"])
       (map (fn [n] (n "search_id")))
       (distinct)
       (map get-mf-details)
       (map new-asset-details-for-mf)
       (map vals)
       (sql-upsert-asset-mf))
  
  ;; store mf
  (store-inv-from-mf "910111080781" "120503"))

(defn -main
  "I dont do a whole lot ... yet."
  [& args]
  (do
    (store-inv-mf)))
