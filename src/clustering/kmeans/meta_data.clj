(ns clustering.kmeans.meta-data
  (:require [clustering.kselect.math :as m]
            [clojure.pprint :as pprint]
            [sparkling
             [core :as spark]
             [destructuring :as tuple]]
            [clustering.lib :as core-lib])
  (:import org.apache.spark.mllib.linalg.Vectors))

(defn compute-cluster-centers
  "Returns a map of cluster id and cluster center."
  [model]
  (let [centers (.clusterCenters model)]
    (->> centers
      (map (fn [center]
             (vec (.toArray center))))
      (zipmap (range (count (vec centers)))))))

(defn count-clusters-members
  ;; TODO this is wastefully recalculated a bazillion times. memoize or something.
  [rdd model]
  (let [number-of-clusters (.k model)
        map-counter (zipmap (range number-of-clusters)(repeat number-of-clusters 0))
        ;; TODO spark/collect realizes everything in local ram on master. this is bad.
        coll (vec (spark/collect (.predict model rdd)))]
    (reduce (fn [acc next-index]
              (assoc acc next-index (inc (acc next-index))))
            map-counter
            coll)))

(defn center-distance
  ;; TODO wastefully recalculated a bazillion times. memoize or something
  ;; NOTEE: Actually it is computed once per data point.
  [v model centers]
  (let [cluster-id (.predict model v)
        center (centers cluster-id)
        v1  (if (= (type v) scala.Tuple2) (tuple/second v) v)]
    (m/cartesian-distance (vec (.toArray v1)) center)))

(defn add-distances
  [v1 v2 model centers]
  (let [d1 (if (number? v1) v1 (center-distance v1 model centers))
        d2 (if (number? v2) v2 (center-distance v2 model centers))]
    (+ d1 d2)))

(defn max-distance
  "Returns the max distance of v1 and v2 from center of the cluster that they belong to."
  [v1 v2 model centers]
  (let [d1 (if (number? v1) v1 (center-distance v1 model centers))
        d2 (if (number? v2) v2 (center-distance v2 model centers))]
    (max d1 d2)))

(defn untuple-vector
  [v]
  (map (fn [x]
         [(tuple/first x)
          (tuple/second x)])
       v))

(defn compute-cluster-diameters
  [rdd model]
  (let [centers  (compute-cluster-centers model)
        k (count centers)
        member-count (count-clusters-members rdd model)
        tupled-distances (->> rdd
                           (spark/map-to-pair (fn [x] (spark/tuple (.predict model x) x)))
                           (spark/reduce-by-key (fn [x y] (max-distance x y model centers)))
                           ;; TODO spark/collect realizes everything in local ram on master. this is bad.
                           spark/collect)
        distances (untuple-vector tupled-distances)]
    (reduce (fn [acc [id distance]]
              (let [distance (if (number? distance) distance (center-distance distance model centers))
                    diameter (* 2 distance)]
                (merge acc {id diameter})))
            {}
            distances)))


(defn compute-cluster-fn
  [rdd model func]
  (let [tupled-values (->> rdd
                        (spark/map-to-pair (fn [x] (spark/tuple (.predict model x) x)))
                        (spark/reduce-by-key (fn [x y] (func x y)))
                        ;; TODO spark/collect realizes everything in local ram on master. this is bad.
                        spark/collect)]
    (map (fn [x][(tuple/first x)(tuple/second x)]) tupled-values)))

(defn min-vector
  [vectors]
  (let [vectors (map (fn [v](if  (number? v) (vector v) v)) vectors)
        min-vector-value (first vectors)]
    (loop [vectors (rest vectors)
           min-vector-value min-vector-value]
      (if (empty? vectors)
        min-vector-value
        (recur (rest vectors)
               (vec (map min min-vector-value (first vectors))))))))

(defn min-vectors
  [v1 v2]
  (let [v1 (vec (.toArray (if (= (type v1) scala.Tuple2) (._2 v1) v1)))
        v2 (vec (.toArray (if (= (type v2) scala.Tuple2) (._2 v2) v2)))
        v (min-vector (list v1 v2))]
    (Vectors/dense (double-array v))))

(defn compute-cluster-mins
  "Returns a map of cluster minimum vectors"
  [rdd model]
  (reduce (fn [acc [id minv]](merge acc {id (vec (.toArray minv))}))
          {}
          (compute-cluster-fn rdd model min-vectors)))

(defn max-vector
  [vectors]
  (let  [vectors (map (fn [v](if  (number? v) (vector v) v)) vectors)
         max-vector (first vectors)]
    (loop [vectors (rest vectors)
           max-vector max-vector]
      (if (empty? vectors)
        max-vector
        (recur (rest vectors)
               (vec (map max max-vector (first vectors))))))))

(defn max-vectors
  [v1 v2]
  (let [v1 (vec (.toArray (if (= (type v1) scala.Tuple2) (._2 v1) v1)))
        v2 (vec (.toArray (if (= (type v2) scala.Tuple2) (._2 v2) v2)))
        v (max-vector (list v1 v2))]
    (Vectors/dense (double-array v))))

(defn compute-cluster-maxes
  [rdd model]
  (reduce (fn [acc [id maxv]](merge acc {id (vec (.toArray maxv))}))
          {}
          (compute-cluster-fn rdd model max-vectors)))

(defn add-vectors
  [v1 v2]
  (let [v1 (if (= (type v1) scala.Tuple2) (._2 v1) v1)
        v2 (if (= (type v2) scala.Tuple2) (._2 v2) v2)]
    (Vectors/dense (double-array (mapv (fn [x y](+ x y)) (vec (.toArray v1)) (vec (.toArray v2)))))))

(defn compute-cluster-averages
  [rdd model]
  (let [k (count (compute-cluster-centers model))
        member-count (count-clusters-members rdd model)
        sums (compute-cluster-fn rdd model add-vectors)]
    (reduce (fn [acc [id sumv]]
              (merge acc {id (mapv #(/ % (member-count id)) (vec (.toArray sumv)))}))
            {}
            sums)))

(defn add-users
  [v1 v2]
  (let [v1 (if (= (type v1) scala.Tuple2) (._2 v1) v1)
        v2 (if (= (type v2) scala.Tuple2) (._2 v2) v2)]
    (->> (mapv (fn [x y] (+ x y))
               (vec (.toArray v1))
               (vec (.toArray v2)))
      double-array
      Vectors/dense)))


(defn count-cluster-users
  [rdd model]
  (->> rdd
    (spark/map-to-pair (fn [x]
                         (spark/tuple (.predict model x)
                                      (Vectors/dense
                                       (double-array
                                        (mapv (fn [y] (if (> y 0) 1 0))
                                              (vec (.toArray x))))))))
    (spark/reduce-by-key (fn [x y] (add-users x y)))
    ;; TODO spark/collect realizes everything in local ram on master. this is bad.
    spark/collect
    (map (fn [x]
           [(tuple/first x)
            (tuple/second x)]))
    (reduce (fn [acc [id userv]]
              (merge acc {id (vec (.toArray userv))}))
            {})))

(defn generate
  [rdd model names pages uuid]
  (let [centers (compute-cluster-centers model)
        diameters (compute-cluster-diameters rdd model)
        mins (compute-cluster-mins rdd model)
        maxes (compute-cluster-maxes rdd model)
        averages (compute-cluster-averages rdd model)
        member-counts (count-cluster-users rdd model)
        counts (count-clusters-members rdd model)
        error (.computeCost model (.rdd rdd))]
    (->> (range (count centers))
      (map (fn [id]
             {:cluster_member_count (counts id)
              :private false
              :cluster_id (str uuid "-" id)
              :cluster_diameter (diameters id)
              :cluster_wssse error
              :dimensions
              (->> (map vector
                        pages
                        names
                        (mapv #(Math/round %) (member-counts id))
                        (centers id)
                        (mins id)
                        (maxes id)
                        (averages id))
                (map #(zipmap [:dimension_id
                               :dimension_name
                               :member_count
                               :center
                               :min
                               :max
                               :avg] %)))}))

      doall)))
