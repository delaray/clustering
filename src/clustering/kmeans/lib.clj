(ns ae.kmeans.lib
  (:require [clustering.kmeans.meta-data :as meta-data]
            [clustering.lib :as core-lib]
            [cheshire.core :as json]
            [clojure.string :as s]
            [sparkling
             [conf :as conf]
             [core :as spark]
             [destructuring :as tuple]
             [function :as function]]
            [taoensso.timbre :as timbre])
  (:import [org.apache.spark.mllib.clustering BisectingKMeans KMeans]
           org.apache.spark.mllib.linalg.Vectors
           org.apache.spark.api.java.JavaRDD))

(defmacro timeit
  [msg & forms]
  `(let [start# (. System (nanoTime))
         ret# (do ~@forms)]
     (timbre/info (str ~msg ". runtime: " (long (/ (double (- (. System (nanoTime)) start#)) 1000000000.0)) " secs"))
     ret#))

(defn process-file
  [ctx data-file]
  (->> data-file
    (spark/text-file ctx)
    (spark/map (fn [s]
                 (->> (s/split s #" ")
                   (map #(s/split % #":"))
                   (map (fn [[index value]]
                          (spark/tuple
                           (Integer/parseInt index)
                           (Double/parseDouble value)))))))
    .cache))

(defn get-datas
  [inputs model]
  (.zipWithIndex (.predict model inputs)))

(defn tuple->vec
  [x]
  [(tuple/first x) (tuple/second x)])

(defn save-meta-data-json
  [meta-datas result-dir]
  (->> meta-datas
    (map json/generate-string)
    (s/join "\n")
    (core-lib/put-key (core-lib/join-path result-dir "meta-data.json"))))

(defn save-data-json
  [users datas result-dir uuid]
  (->> (.aggregateByKey
        datas
        []
        (function/function2
         (fn [u v]
           (conj u v)))
        (function/function2
         (fn [u1 u2]
           (concat u1 u2))))
    spark/collect
    (map tuple->vec)
    (map (fn [[k v]]
           (->> {:cluster_id (str uuid "-" k)
                 :user_ids (for [row-num v]
                             (doto (nth users row-num)
                               (assert "index out of bound of users lookup")))}
                json/generate-string)))
    (s/join "\n")
    (core-lib/put-key (core-lib/join-path result-dir "data.json"))))

(def init-spark
  (fn [mode]
    (-> (conf/spark-conf)
      (conf/set "spark.serializer" "org.apache.spark.serializer.KryoSerializer")
      (conf/set "spark.kryoserializer.buffer.max" "512m")
      (conf/master (condp = mode
                     "single" "local"
                     "multi" "local[*]"
                     mode))
      (conf/app-name "K-Means")
      spark/spark-context)))

(defn get-meta-data
  ;; TODO this may well be more expensive than kmeans training. much
  ;; more. treat it seperately? make it leverage rdd's instead of
  ;; being a computation on master? persist the kmeans model and push
  ;; this computation elsewhere? other stuff?
  [inputs model names pages uuid]
  (->> (meta-data/generate inputs model names pages uuid)
    (filterv #(pos? (:cluster_member_count %))))) ;; Elimate empty clusters, i.e. member count = 0.

(defn train
  [inputs k [init iterations epsilon]]
  (let [x (-> (KMeans.)
            (.setK k)
            (.setMaxIterations iterations)
            (.setInitializationMode init)
            (.setEpsilon epsilon))]
    (timbre/info "train standard-kmeans"
                 "k:" (.getK x)
                 "iterations:" (.getMaxIterations x)
                 "init:" (.getInitializationMode x)
                 "epsilon:" (.getEpsilon x))
    (.run x (if (= JavaRDD (type inputs))
              (.rdd inputs)
              inputs))))

(defn s3->s3n
  [x]
  (s/replace x #"s3://" "s3n://"))

(defn load-names
  [input-dir]
  (->> "names.txt"
    (core-lib/join-path input-dir)
    core-lib/get-key
    s/split-lines))

(defn load-pages
  [input-dir]
  (->> "pages.txt"
    (core-lib/join-path input-dir)
    core-lib/get-key
    s/split-lines))

(defn load-users
  [input-dir]
  (->> "users.txt"
    (core-lib/join-path input-dir)
    core-lib/get-key
    s/split-lines))

(defn parse-params
  [params]
  (let [params (s/split params #",")
        _ (assert (= 3 (count params)) (str "standard params should be: <kmeans|| or random>,<iterations>,<epsilon>. got: " params))]
    (let [[init iterations epsilon] params
          iterations (doto (read-string iterations)
                       (-> integer? (assert "iterations should be an int")))
          epsilon (doto (read-string epsilon)
                    (-> float? (assert "epsilon should be a float")))]
      (assert (#{"kmeans||" "random"} init) (str "init should be on of \"random\" or \"kmeans||\", not: " init))
      [init iterations epsilon])))
