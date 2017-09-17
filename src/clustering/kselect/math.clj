(ns clustering.kselect.math)

;;; This magic number is based the AK comment about user-segment size:
;;; "anything lower than 10k is probably not useful"

(defonce max-K 1000)

(defn square [n](* n n))

(defn powers-of-two
  "Generates a vector of powers of 2"
  [maximum & {:keys [limit] :or {limit 100000}}]
  (let [max (if (> maximum limit) max-K maximum)
        min (if (> maximum limit) 16 2)]
    (loop [result []
           k min]
      (if (> k max) result (recur (conj result k) (* k 2))))))

;;;-------------------------------------------------------------------------------------------------

(defn next-binsearch-value [k1 k2]
  (let [res (+ k1 (Math/round (/ (- k2 k1) 2.0)))]
    ;;(println "\nk1, k2, mid-k:" k1 k2 res)
    res))

;;;-------------------------------------------------------------------------------------------------

(defn scale-x-points [points [min-x max-x][min-y max-y]]
  (let [x-factor (* 1.0 (/ max-y max-x))]
    (map (fn [[x y]] [(* x x-factor) y]) points)))

;;;-------------------------------------------------------------------------------------------------

(defn scale-y-points [points [min-x max-x][min-y max-y]]
  (let [y-factor (* 1.0 (/ max-x max-y))]
    (map (fn [[x y]] [x (* y y-factor)]) points)))

;;;-------------------------------------------------------------------------------------------------

(defn scale-points [points]
  (let [min-x (apply min (map first points))
        max-x (apply max (map first points))
        min-y (apply min (map second points))
        max-y (apply max (map second points))]
    (if (> (- max-y min-y)(- max-x min-x))
      (scale-x-points points [min-x max-x][min-y max-y])
      (scale-y-points points [min-x max-x][min-y max-y]))))

;;;---------------------------------------------------------------------
;;; VECTOR MATH
;;;---------------------------------------------------------------------

(defn vector-diff [v1 v2]
  (mapv (fn [x1 x2](- x1 x2)) v1 v2))

;;;---------------------------------------------------------------------

(defn vector-norm
  "Returns the norm of a vector"
  [point1]
  (let [point1 (if (number? point1)(vector point1) point1)]
    (Math/sqrt (reduce + (map (fn [c1](square c1)) point1)))))

;;;-------------------------------------------------------------------------------------------------

(defn cartesian-distance
  [point1 point2]
  (let [point1 (if (number? point1) (vector point1) point1)
        point2 (if (number? point2) (vector point2) point2)]
    (Math/sqrt (reduce + (map (fn [c1 c2] (Math/pow (- c2 c1) 2))
                              point1
                              point2)))))

;;;-------------------------------------------------------------------------------------------------

(defn midpoint [[x1 y1][x2 y2]]
  [(+ x1 (/ (Math/abs (- x2 x1)) 2.0)) (+ y1 (/ (Math/abs (- y2 y1)) 2.0))])

;;;-------------------------------------------------------------------------------------------------

(defn slope
  "Returns the slope of the line between the two points."
  [k1 s1 k2 s2]
  (if (zero? (- k2 k1)) 0 (/ (- s2 s1) (- k2 k1))))

;;;-----------------------------------------------------------------------
;;; POINTS-ON-CIRCLE
;;;------------------------------------------------------------------------;;;

;;; This function returns a list of n (x y) pairs that are uniformely distributed
;;; around acircle with center 'center' and diameter 'diameter' starting at
;;; angle start-angle.

(defn points-on-circle [center radius start-angle n]
  (let [x-center (first center)
        y-center (second center)
        angle-inc (/ (* 2 Math/PI) n)
        angle1 start-angle
	points 
        ;; Move around the circle in increments of 'anc-inc'
        (mapv (fn [i]
                (let [sin-mid-angle (Math/sin (+ angle1 (* i angle-inc)))
                      cos-mid-angle (Math/cos  (+ angle1 (* i angle-inc)))
                      point-x (+ x-center (Math/round (* (+ 1 radius) cos-mid-angle)))
                      point-y (+ y-center (Math/round (* (+ 1 radius) sin-mid-angle)))]
                  [point-x point-y]))
              (range n))]
    points))

;;;-------------------------------------------------------------------------------------------------
;;; End of File
;;;-------------------------------------------------------------------------------------------------
