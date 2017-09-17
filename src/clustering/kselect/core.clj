(ns clustering.kselect.core
  (:gen-class)
  (:require [clustering.kmeans.lib :as lib]
            [clustering.kselect.kmeans :as k]
            [clustering.lib :as core-lib]
            [taoensso.timbre :as timbre]))

(defn -main [mode input-dir params]
  (core-lib/setup-logging)
  (let [ctx (lib/init-spark mode)]
    (try
      (let [[_ iterations _] (lib/parse-params params)
            rdd (lib/process-file ctx (lib/input-file input-dir))
            [k _ _ _] (lib/timeit "done with snail speed (omg) K selection"
                        (k/process-kmeans rdd nil iterations))]
        (timbre/info "picked k:" k)
        (print "K = " k))
      (finally
        (if (= mode "single")
          (.stop ctx))))
    (core-lib/shutdown)))
