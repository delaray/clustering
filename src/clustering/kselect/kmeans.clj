(ns clustering.kselect.kmeans
  (:require [clustering.kmeans.meta-data :as md]
            [clustering.kmeans.lib :as lib]
            [clustering.kselect.kselect :as ks]
            [clustering.kselect.math :as m]
            [clojure.string :as str]
            [clojure.string :as s]
            [sparkling.core :as spark]
            [sparkling.destructuring :as tuple]
            [me.raynes.fs :as fs])
  (:import (org.apache.spark.mllib.clustering KMeans)
           (org.apache.spark.mllib.linalg Vectors)))

;;;-------------------------------------------------------------------------------------------------
;;; Diameter computation
;;;-------------------------------------------------------------------------------------------------

(defn avg-cluster-diameter [rdd model]
  (let [d (md/compute-cluster-diameters rdd model)]
    (/ (reduce + (vals d)) (count d))))

(defn model-diameters [k-models]
  (mapv (fn [[k _ d]][k d]) k-models))

;;;-------------------------------------------------------------------------------------------------
;;; COMPUTE-KMEANS
;;;-------------------------------------------------------------------------------------------------

;;; Clojure wrapper for Spark Kmeans. Returns an object of type KMeansModel along with k wssse.

(defn compute-kmeans
  "Runs Sparks MlLib Kmeans implementation on the specified rdd"
  [rdd k iterations]
  (let [rdd1 (.rdd rdd)
        model (KMeans/train rdd1 k iterations)
        avg-d (avg-cluster-diameter rdd model)
        wssse (.computeCost model rdd1)]
    (println "Tried K=" k ", Average diameter =" avg-d)
    [k wssse avg-d model]))

;;;-------------------------------------------------------------------------------------------------
;;; COMPUTE-NEXT-MODEL
;;;-------------------------------------------------------------------------------------------------

;;; Invokes Kmeans with the specified parameters

(defn compute-next-model
  "Returns a KMeans model and it's cost."
  [rdd k iterations]
  (compute-kmeans rdd k iterations))

;;;-------------------------------------------------------------------------------------------------

;;; Invokes Kmeans and then computes the average cluster diameter size. Used as a helper function
;;; for determining the value for K.

(defn compute-next-model-diameters
  "Returns a KMeans model and the average cluster diameter."
  [rdd k iterations]
  (let [[k wssse avg-diameter model] (compute-kmeans rdd k iterations)]
    [avg-diameter model]))

;;;-------------------------------------------------------------------------------------------------
;;; COMPUTE-KMEANS-N-TIMES
;;;-------------------------------------------------------------------------------------------------

;;; Calls a K-Means N times for the different values of K in the supplied vector of K's
;;; and picks the best one.

;;; Heuristic: As K gets smaller, the diameters get bigger. The graph of K values by average cluster
;;; diameter is asymptotic. The best value for K lies somewhere is the elbow of the curve.

(defn compute-kmeans-n-times
  "Run kmeans on each value of k in k-vectors"
  [rdd k-vector iterations]
  (reduce (fn [acc next-k]
            (conj acc (compute-next-model rdd next-k iterations)))
          []
          k-vector))

;;;-------------------------------------------------------------------------------------------------
;;; BINSEARCH-BIC-K
;;;-------------------------------------------------------------------------------------------------

;;;  Bayesian Based K Selector with logarithmic time search

(defn binsearch-bic-k
  "Uses binary search with bayesian information criterion to find best k."
  [rdd [[k1 b1][k2 b2]] iterations]
  (loop [[k1 b1] [k1 b1]
         [k2 b2] [k2 b2]]
    (println "[k1 b1] =  [" k1 " " b1 "]")
    (println "[k2 b2] =  [" k2 " " b2 "]")
    (cond (<= k2 k1)
          [k1 b1]
          (= 1 (- k2 k1))
          (if (< b1 b2)
            [k1 b1]
            [k2 b2])
          :else
          (let [mid-k (m/next-binsearch-value k1 k2)
                [k w d m] (compute-kmeans rdd mid-k iterations)
                b (ks/BIC rdd m)]
            (println "\n k1, mid-k, k2: " k1 mid-k k2)
            (if (> (Math/abs (- b1 b)) (Math/abs (- b b2)))
              (recur [k1 b1][mid-k b])
              (recur [mid-k b][k2 b2]))))))

;;;-------------------------------------------------------------------------------------------------
;;; COMPUTE-BEST-K-4
;;;-------------------------------------------------------------------------------------------------

;;;  Bayesian Based K Selector with linear time search

;;; This version works but call kmeans N/2 times where N=k2-k1.

(defn compute-best-k-4 [rdd [k1 k2] iterations]
  ;; TODO: Find more efficient sampling other than every other number.
  (let [kandidates (conj (vec (range k1 k2 2)) k2)
        results (compute-kmeans-n-times rdd kandidates iterations)
        models (map last results)]
    (first (sort (fn [x y] (> (second x)(second y)))
                 (mapv (fn [m][(.k m) (ks/BIC rdd m)]) models)))))

;;;-------------------------------------------------------------------------------------------------
;;; COMPUTE-KMEANS*
;;;-------------------------------------------------------------------------------------------------

(defn compute-kmeans*
  "Selects the best K based on the range of specified k's."
  [rdd k-vector iterations]
  ;; (print "\nK-vector: " k-vector "\n")
  (let [models (mapv last (compute-kmeans-n-times rdd k-vector iterations))
        [[k1 b1][k2 b2]] (ks/compute-best-k-range-4 rdd models)
        [k d] (binsearch-bic-k rdd [[k1 b1][k2 b2]] iterations)]
    (println "\nBest value for K = " k "\n")
    (compute-kmeans rdd k iterations)))

;;;-------------------------------------------------------------------------------------------------

#_
(defn compute-kmeans*
  "Selects the best K based on the range of specified k's."
  [rdd k-vector iterations]
  ;; (print "\nK-vector: " k-vector "\n")
  (let [models (mapv last (compute-kmeans-n-times rdd k-vector iterations))
        [[k1 b1][k2 b2]] (ks/compute-best-k-range-4 rdd models)
        [k d] (compute-best-k-4 rdd  [k1 k2] iterations)]
    (println "\nBest value for K = " k "\n")
    (compute-kmeans rdd k iterations)))

;;;-------------------------------------------------------------------------------------------------
;;; PROCESS-KMEANS
;;;-------------------------------------------------------------------------------------------------

(defn process-kmeans
  "Run kmeans on rdd and potentially determine best k value if not supplied"
  [rdd k iterations]
  (let [kandidates (m/powers-of-two (spark/count rdd))
        [k wssse avg-d model]
        (cond (number? k)
              (compute-kmeans rdd k iterations)
              (vector? k)
              (compute-kmeans* rdd k iterations)
              :else
              (compute-kmeans* rdd kandidates iterations))]
    (print "\nBest value for K is " k "\n")
    [k wssse avg-d model]))

;;;*************************************************************************************************
;;; OLD CODE: Earlier Attemtps at Selecting K. (Do Not Delete)
;;;*************************************************************************************************

;;;-------------------------------------------------------------------------------------------------
;;; BINSEARCH-K
;;;-------------------------------------------------------------------------------------------------

(defn binsearch-k
  "Uses binary search with average diameter reduction to find best k."
  [rdd [[k1 d1][k2 d2]] iterations]
  (loop [[k1 d1] [k1 d1]
         [k2 d2] [k2 d2]]
    ;;(println "[k1 d1] =  [" k1 " " d1 "]")
    ;;(println "[k2 d2] =  [" k2 " " d2 "]")
    (cond (<= k2 k1)
          [k1 d1]
          (= 1 (- k2 k1))
          [k2 d2]
          :else
          (let [mid-k (m/next-binsearch-value k1 k2)
                [k w d m] (compute-kmeans rdd mid-k iterations)]
            ;;(println "\n k1, mid-k, k2: " k1 mid-k k2)
            (if (> (- d1 d)(- d d2))
              (recur [k1 d1][mid-k d])
              (recur [mid-k d][k2 d2]))))))

;;;-------------------------------------------------------------------------------------------------
;;; End of File
;;;-------------------------------------------------------------------------------------------------
