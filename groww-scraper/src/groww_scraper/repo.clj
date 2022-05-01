(ns groww-scraper.repo
  (:require [cheshire.core :as json]
            [clj-time.coerce :as tc]
            [honey.sql :as sql]
            [honey.sql.helpers :as sqlh]
            [next.jdbc :as jdbc]
            [next.jdbc.types :as jdbc.types]))

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

(defn sql-upsert-tick-mf
  "upsert assets"
  [values]
  ;; FIXME: why do we need to manual add columns here
  (-> (sqlh/insert-into :asset_tick_data  [:tick_date
                                           :tick_value
                                           :schema_code])
      (sqlh/values values)
      (sqlh/upsert (-> (sqlh/on-conflict :tick_date
                                         :tick_value
                                         :schema_code)
                       (sqlh/do-nothing)))
      (sql/format)))

(defn sql-select-cagr-data
  [schema-code st-date years-forward]
  [(str "SELECT tick_value from asset_tick_data
WHERE schema_code = ?
    and (tick_date = ?::timestamp
             OR tick_date = ?::timestamp + interval '" years-forward "years')
ORDER BY tick_date asc") schema-code st-date st-date])

(defn sql-select-first-tick-date
  [schema-code]
  (-> (sqlh/select [[:min, :tick_date]])
      (sqlh/from :asset_tick_data)
      (sqlh/where [:= :schema_code schema-code])
      (sql/format)))

;; FIXME: running tc/from-sql-date doesnt work from outside the map
(defn get-min-tick-date-for-scheme
  [scheme-code]
  (->  (jdbc/execute! ds (sql-select-first-tick-date scheme-code))
       (get-in [0 :min])
       (tc/from-sql-date)))
