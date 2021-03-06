(ns clustering.kselect.kselect
  (:require [clustering.kselect.math :as m]
            [clustering.kmeans.meta-data :as md]
            [sparkling
             [core :as spark]
             [destructuring :as tuple]])
   (:import org.apache.spark.mllib.linalg.Vectors))

;;;-------------------------------------------------------------------------------------------------

(defn succesive-pairs
  "Returns a vector of consecutive pairs of values."
  [values]
  (rest (reduce (fn [acc next-val] (conj acc [(second (last acc)) next-val])) [] values)))

;;;-------------------------------------------------------------------------------------------------
;;; COMPUTE-K-DIAMETERS-SLOPES
;;;-------------------------------------------------------------------------------------------------

(defn filter-slopes [slopes]
  slopes
  #__
  (filter (fn [[k s]] (<= s -1)) slopes))

;;;-------------------------------------------------------------------------------------------------

(defn compute-k-diameter-slopes
  "Computes the slopes between successive values of k and the avg cluster diameters"
  [k-diameters]
  ;; Ensure the K's are in icreasing order
  (let [vectors (sort (fn [[k1 _][k2 _]](< k1 k2)) k-diameters)]
    ;; Compute the slopes between succesive ppoints
    (mapv (fn [[[k1 d1][k2 d2]]][k1 (m/slope k1 d1 k2 d2) d1])
          (succesive-pairs vectors))))

;;;-------------------------------------------------------------------------------------------------
;;; COMPUTE-BEST-K
;;;-------------------------------------------------------------------------------------------------

(defn best-k [k-vectors] (first (first k-vectors)))

;;;-------------------------------------------------------------------------------------------------
;;; Gaussian Mixture Mode Approach helper functions
;;;-------------------------------------------------------------------------------------------------

;;; Can probably optimize the logic.

(defn add-squared-distance
  [v1 v2 model centers]
  (cond (and (number? v1) (number? v2))
        (+ v1 v2)
        (and (not (number? v1)) (number? v2))
        (+ (let [d (md/center-distance v1 model centers)](m/square d))
           v2)
        (and (number? v1) (not (number? v2)))
        (+ v1
           (let [d (md/center-distance v2 model centers)](m/square d)))
        :else
        (let [d1 (md/center-distance v1 model centers)
              d2 (md/center-distance v2 model centers)]
          (+ (m/square d1)(m/square d2)))))

;;;-------------------------------------------------------------------------------------------------
;;; Gaussian Mixture Mode Approach
;;;-------------------------------------------------------------------------------------------------

;;; APPENDIX A:INFORMATION CRITERIAFOR K-MEANS

;;; From a statistical point of view,the K-means algorithm is aparticular instance of theClassication
;;; Expectation Maximisation  algorithm for a Gaussian mixture model with equal mixture weights and equal
;;; isotropic variances.

;;; For K-means we take the mean of each Gaussian to be the center of the resulting clusters

;;; The mean, MUk = cluster center.
;;; The variance, sig2 is the overall within cluster variance

(defn compute-variances [rdd model]
  (let [centers (md/compute-cluster-centers model)
        N (spark/count rdd)
        squared-distances
        (->> rdd
             (spark/map-to-pair (fn [x] (spark/tuple (.predict model x) x)))
             (spark/reduce-by-key (fn [x y] (add-squared-distance x y model centers)))
             ;; TODO spark/collect realizes everything in local ram on master. this is bad.
             ;; Rebuttal: Not really. There are K things being realized in memory.
             spark/collect
             md/untuple-vector)
        result (mapv (fn [[cluster-id sd]]
                       [cluster-id (if (number? sd) sd (m/square (md/center-distance sd model centers)))])
                     squared-distances)]
    (/ (reduce + (map second result)) N)))


;;;-------------------------------------------------------------------------------------------------
;;; Single Data point Density
;;;-------------------------------------------------------------------------------------------------

;;; This computes P(uj | M, sigma2) where uj is a datapoint, M contains all the means (i.e. cluster
;;; centers) and sigma squared is the overall variance.

(defn data-point-density [datapoint model centers variance]
  (let [mfactor (/ 1.0 (Math/sqrt (* 2 Math/PI variance)))
        mean (centers (.predict model datapoint))
        uj (vec (.toArray datapoint))
        ;; ukl is the mean of the clusyer that uj belongs to.
        ukj (vec (.toArray mean))
        norm (m/vector-norm (m/vector-diff uj ukj))
        exponent (- (* 0.5 (/ (m/square norm) variance)))
        density (* mfactor (Math/pow Math/E exponent))]
    density))

;;;-------------------------------------------------------------------------------------------------
;;; Entire Data Set Density
;;;-------------------------------------------------------------------------------------------------

;;; Note: This actually returns the log of the data set density.

(defn ln-data-set-density [rdd model]
  (let [centers (md/compute-cluster-centers model)
        variance (compute-variances rdd model)]
    (spark/reduce (fn [acc datapoint]
                    (cond (and (number? acc)(number? datapoint))
                          (+ acc datapoint)
                          (number? acc)
                          (+ acc (Math/log (data-point-density datapoint model centers variance)))
                          (number? datapoint)
                          (+ (Math/log (data-point-density acc model centers variance))
                             datapoint)
                          :else
                          (+ (Math/log (data-point-density acc model centers variance))
                             (Math/log (data-point-density datapoint model centers variance)))))
                  rdd)))
        
;;;-------------------------------------------------------------------------------------------------
;;; Akaike Information Criterion
;;;-------------------------------------------------------------------------------------------------

(defn AIC [rdd model]
  (let [Q (count (vec (.toArray (spark/first rdd))))]
    (- (ln-data-set-density rdd model) (inc (* (.k model) Q)))))
        
;;;-------------------------------------------------------------------------------------------------
;; Bayesian Information Criterion
;;;-------------------------------------------------------------------------------------------------

(defn BIC [rdd model]
  (let [Q (count (vec (.toArray (spark/first rdd))))]
    (- (ln-data-set-density rdd model)
       (* (/ (inc (* (.k model) Q)) 2)
          (Math/log (spark/count rdd))))))

;;;-------------------------------------------------------------------------------------------------

(defn BICs [rdd models]
  (mapv (fn [m][(.k m) (BIC rdd m)]) models))

;;;-------------------------------------------------------------------------------------------------
;;; MAX-BIC-GAP
;;;-------------------------------------------------------------------------------------------------

;;; This chooses largest gap in BICs.
;;; Input: Bics is a vector of vectors of [k bic].
;;; Output: A vector of [k1 b1] [k2 b2] and (- b2 b1).

(defn max-bic-gap [bics]
  (first (sort (fn [x y] (> (last x)(last y)))
               (mapv (fn [[[k1 b1][k2 b2]]] [[k1 b1][k2 b2](Math/abs (- b2 b1))])
                     (succesive-pairs bics)))))

;;;-------------------------------------------------------------------------------------------------
;;; Bayesian Based K Range Selector
;;;-------------------------------------------------------------------------------------------------

;;; Each vector in vectors is of the form [k d w m]

(defn compute-best-k-range-4
  "Returns a K range basaed on the bayesin informtion criterion."
  [rdd models]
  (let [[[k1 b1] [k2 b2] _] (max-bic-gap (BICs rdd models))]
    [[k1 b1] [k2 b2]]))

;;;*************************************************************************************************
;;; OLD CODE: Earlier Attemtps at Selecting K. (Do Not Delete)
;;;*************************************************************************************************

;;;-------------------------------------------------------------------------------------------------
;;; The REAL Elbow method
;;;-------------------------------------------------------------------------------------------------

;;; This method looks at the percentage of variance explained as a
;;; function of the number of clusters: One should choose a number of
;;; clusters so that adding another cluster doesn't give much better
;;; modeling of the data. More precisely, if one plots the percentage of
;;; variance explained by the clusters against the number of clusters,
;;; the first clusters will add much information (explain a lot of
;;; variance), but at some point the marginal gain will drop, giving an
;;; angle in the graph. The number of clusters is chosen at this point,
;;; hence the "elbow criterion".
;;;
;;; This "elbow" cannot always be unambiguously identified. (WE CAN LIVE WITH THIS!)

;;; Percentage of variance explained is the ratio of the between-group
;;; variance to the total variance, also known as an F-test.
;;; A slight variation of this method plots the curvature of the within group variance.

;;; 1. Generation Population Data
;;;
;;; For each run of K we want to generate the individual clusters diameters

;;; Each vector in k-models is of the form [k d w m], where k is the average diameter.

;;;(defn square [n](* n n))

(defn generate-models-observations [rdd k-models]
  (mapv (fn [[k w d m]]
          (let [ds (vec (vals (md/compute-cluster-diameters rdd m)))]
            [k (/ (reduce + ds) k) ds]))
        k-models))

(defn overall-mean [Yis]
  (/ (reduce + (mapv second Yis)) (count Yis)))

(defn between-group-ssd [Yis]
  (let [Y (overall-mean Yis)
        squares (mapv (fn [Yi]
                      (* (count (last Yi)) (m/square (- (second Yi) Y))))
                      Yis)]
    (reduce + squares)))

(defn msb [Yis]
  (/ (overall-mean Yis) (dec (count Yis))))

(defn center-data [Yis]
  (mapv (fn [Yi]
          (let [ds (last Yi)
                avg (/ (reduce + ds) (count ds))]
            [(first Yi)(second Yi)(mapv (fn [d](- d avg)) ds)]))
        Yis))


;;;-------------------------------------------------------------------------------------------------
;;; FIND-ELBOW-POINT
;;;-------------------------------------------------------------------------------------------------

;;; This was a quick and dirty solution from stack overflow but doesn't work in general!
;;; Keep it here for reference until we converg on a solution. See below for real elbow
;;; methon using F-Test.

(defn find-elbow-point [kd-vector]
  (let [mid-point (m/midpoint (first kd-vector)(last kd-vector))]
    (reduce (fn [elbow-kd next-kd]
              (let [d1 (m/cartesian-distance next-kd mid-point)
                    d2 (m/cartesian-distance elbow-kd mid-point)]
              (if (< d1 d2) next-kd elbow-kd)))
            (second kd-vector)
            (rest (reverse (rest kd-vector))))))

;;;-------------------------------------------------------------------------------------------------
;;; COMPUTE-BEST-K-RANGE
;;;-------------------------------------------------------------------------------------------------

;;; Slope based k selection. Note -1 is the slope of the tangent to the elbow.

;;; Each vector in vectors is of the form [k d w m]

(defn compute-best-k-range-1
  "Returns a range for k based on the slope of the elbow curve of k average cluster diameters"
  [vectors]
  ;;(print "\nVectors: " vectors "\n")
  (let [slopes (compute-k-diameter-slopes vectors)]
     (cond (<= (count slopes) 1)
           (let [[k s d] (first slopes)]
             [[k s d][k s d]])
           :else
           (let [pairs (succesive-pairs slopes)
                 k-range (filter (fn [[[k1 s1 _][k2 s2 _]]] (and (<= s1 -1)(>= s2 -1)))
                                 pairs)
                 [[k1 s1 d1][k2 s2 d2]] (first k-range)
                 [[xk1 xs1 xd1][yk2 ys2 yd2]] (first pairs)
                 best-range
                 (if (or (nil? k1)(nil? k2))
                   [[xk1 xd1][yk2 yd2]]
                   [[k1 d1][k2 d2]])]
             (print "\nSlopes: " slopes "\n")
             (println "\nBest k range: " best-range)
             best-range))))

;;;-------------------------------------------------------------------------------------------------

;;; Change in diameter based k-selection. Useed with binary search once initial k range established.

(defn compute-best-k-range-2
  "Returns the two best values of K based on change in average cluster diameters"
  [vectors]
  (let [diffs (mapv (fn [[[k1 d1][k2 d2]]] [k2 (- d1 d2)]) (succesive-pairs vectors))]
    ;;(print "\nDiffs: " diffs "\n")
    (reduce (fn [res val] (if (> (second val)(second res)) val res))
            [0 0]
            diffs)))

;;;-------------------------------------------------------------------------------------------------

(defn model-errors [k-models]
  (mapv (fn [[k w]][k w]) k-models))

;;; Change in diameter based k-selection. Useed with binary search once initial k range established.

;;; Each vector in vectors is of the form [k d w m]

(defn compute-best-k-range-3
  "Returns the two best values of K based on change in average cluster diameters"
  [vectors]
  (let [errors-vectors (model-errors vectors)
        diffs (mapv (fn [[[k1 w1][k2 w2]]] [k2 (/ w1 w2)]) (succesive-pairs errors-vectors))]
    (print "\nDiffs: " diffs "\n")
    (reduce (fn [res val]
              (if (< (second val)(second res)) val res))
            diffs)))

;;;-------------------------------------------------------------------------------------------------
;;; End of File
;;;-------------------------------------------------------------------------------------------------
