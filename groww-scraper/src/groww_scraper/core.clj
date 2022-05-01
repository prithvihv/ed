(ns groww-scraper.core
  (:require [clj-time.coerce :as tc]
            [clj-time.core :as t]
            [groww-scraper.fin :as fin]
            [groww-scraper.http-client :as http-client]
            [groww-scraper.repo :as repo]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs])
  (:gen-class))

(defn to-date-map
  [date]
  (take 10 (tc/to-string date)))

(defn get-mf-tick-data
  ([schema-code]
   (get-mf-tick-data schema-code 600))
  ([schema-code months]
   (map (fn [n] (vector (-> (nth n 0)
                            (tc/from-long)
                            (tc/to-timestamp)) (nth n 1)))
        (-> (http-client/http-get-mf-tick-data schema-code months)
            (get-in ["folio" "data"])))))


(defn store-inv-from-mf
  "load investmenst from mf details"
  [folio-number scheme-code]
  (->> (-> (http-client/get-txns-for-mf folio-number scheme-code)
           (get-in ["data" "transaction_list"]))
       (map repo/new-inv-for-mf)
       (repo/sql-upsert-inv-mf)
       (jdbc/execute-one! repo/ds)))

(defn store-inv-mf
  "everything mf"
  []
  (do
    ;; FIXME: can consider making 1 get-dashboard-mf call
    ;; load asset infomation
    (->> (get-in (http-client/get-dashboard-mf) ["investments" "portfolio_schemes"])
         (map (fn [n] (n "search_id")))
         (distinct)
         (map http-client/get-mf-details)
         (map repo/new-asset-details-for-mf)
         (map vals)
         (repo/sql-upsert-asset-mf)
         (jdbc/execute-one! repo/ds))
   ;; load inv
    (->> (get-in (http-client/get-dashboard-mf) ["investments" "portfolio_schemes"])
         (map (fn [mf] [(mf "folio_number") (mf "scheme_code")]))
         (map (fn [mf-details] (apply store-inv-from-mf mf-details))))
   ;; load tick data
    (->> (get-in (http-client/get-dashboard-mf) ["investments" "portfolio_schemes"])
         (map (fn [n] (n "scheme_code")))
         (distinct)
         (map (fn [scheme-name] (for [tick (get-mf-tick-data scheme-name)]
                                  (conj tick scheme-name))))
         (flatten)
         (partition 3)
         (partition-all 1000)
         (map (fn [to-insert] (->> (repo/sql-upsert-tick-mf to-insert)
                                   (jdbc/execute-one! repo/ds)))))))

(defn play-ground-threads
  []
  (->> (let [mf-details (-> (repo/mock-get-dashboard-mf)
                            (get-in ["investments" "portfolio_schemes"]))
             scheme-infos (->> (map (fn [n] (n "search_id")) mf-details)
                               (distinct)
                               (map (fn [_] (repo/mock-get-mf-info))))]
                              ;;  (map (fn [n] {n (mock-get-mf-info)}))

         scheme-infos)
       (map repo/new-asset-details-for-mf)
       (map vals)
       (repo/sql-upsert-asset-mf)
       (jdbc/execute-one! repo/ds))

  ;; insert inv of mf
  (->> (-> (repo/mock-get-txn-mf)
           (get-in ["data" "transaction_list"]))
       (map repo/new-inv-for-mf)
       (repo/sql-upsert-inv-mf)
       (jdbc/execute-one! repo/ds))

  (-> (repo/mock-get-mf-info)
      (get-in ["nav"]))

  ;; new mf asset thread
  (->> (get-in (repo/mock-get-dashboard-mf) ["investments" "portfolio_schemes"])
       (map (fn [n] (n "search_id")))
       (distinct)
       (map http-client/get-mf-details)
       (map repo/new-asset-details-for-mf)
       (map vals)
       (repo/sql-upsert-asset-mf))

  ;; store mf
  (store-inv-from-mf "910111080781" "120503")

  ;; load tick data
  (->> (get-in (repo/mock-get-dashboard-mf) ["investments" "portfolio_schemes"])
       (map (fn [n] (n "scheme_code")))
       (distinct)
       (map (fn [scheme-name] (for [tick (get-mf-tick-data scheme-name)]
                                (conj tick scheme-name))))
       (flatten)
       (partition 3)
       (repo/sql-upsert-tick-mf)
       (jdbc/execute-one! repo/ds))

  ;; FIXME this doesnt work with execte-one
  ;; get-min-tick-date-for-scheme
  (->>  (jdbc/execute! repo/ds (repo/sql-select-first-tick-date "120503") {:builder-fn rs/as-unqualified-lower-maps})
        (map (fn [n] (n :min))))

  ;; cagr-data
  ;; https://cljdoc.org/d/com.github.seancorfield/next.jdbc/1.2.772/doc/getting-started#options--result-set-builders
  (let [min-date (repo/get-min-tick-date-for-scheme "120503")
        a (repo/sql-select-cagr-data "120503" "2019-03-27" 1)
        min-max (->> (jdbc/execute! repo/ds a {:builder-fn rs/as-unqualified-lower-maps})
                     (map (fn [n] (bigdec (n :tick_value)))))]
    ;; (type (nth min-max 0)))
    (if (< (count min-max) 2)
      (println "didnt find min-max for this date")
      (apply fin/cagr (-> (conj min-max 1)
                          (reverse)))))

  (apply str (take 10 (tc/to-string (t/now))))

  (repo/get-min-tick-date-for-scheme "120503")

  (->> (repo/get-min-tick-date-for-scheme "120503")
       (map (fn [n] (t/plus n (-> 1 t/days)))))
  (t/plus (repo/get-min-tick-date-for-scheme "120503") (-> 1 t/days)))



(defn -main
  "I dont do a whole lot ... yet."
  [& args]
  (do
    (store-inv-mf)))
