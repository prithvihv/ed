(ns groww-scraper.fin)

(defn cagr
  "cagr accepts st-price ed-price (decimal) and a time duration in years (int)"
  [st-price ed-price t]
  (-> (with-precision 4 (/ ed-price st-price))
      (Math/pow (/ (bigdec 1) (bigdec t)))
      (- 1)))

(defn playground
  []
  ;; expecting ~58.49
  (cagr (bigdec 100000) (bigdec 1000000) 5)
  (cagr (bigdec 42.4778) (bigdec 46.2936) 1))
