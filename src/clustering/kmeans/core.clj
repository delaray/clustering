(ns clustering.kmeans.core
  (:gen-class)
  (:require [clustering.kmeans.lib :as lib]
            [clustering.lib :as core-lib]
            [clojure.string :as s]
            [taoensso.timbre :as timbre]))

(defn -main [mode k input-dir output-dir params]
  (lib/parse-params params) ;; fail fast on bad params
  (core-lib/run "aws s3 ls" (core-lib/join-path input-dir "inputs.txt")) ;; fail fast if inputs.txt doesnt exist
  (let [ctx (lib/init-spark mode)]
    (try
      (let [inputs (lib/process-file ctx (lib/input-file input-dir))
            model (lib/timeit "done with train-kmeans"
                    (lib/train inputs k (lib/parse-params params)))
            uuid (core-lib/uuid)
            datas (lib/timeit "done with get-data"
                    (lib/get-datas inputs model))
            users (lib/load-users input-dir)
            _ (lib/save-data-json users datas output-dir uuid)
            names (lib/load-names input-dir)
            pages (lib/load-pages input-dir)
            meta-data (lib/timeit "done with get-meta-data"
                        (lib/get-meta-data inputs model names pages uuid))
            _ (lib/save-meta-data-json meta-data output-dir)])
      (finally
        (if (= mode "single")
          (.stop ctx))))
    (core-lib/shutdown)))
