(ns ae.kmeans.core-test
  (:require [ae.kmeans.core :as core]
            [ae.kselect.core :as kselect]
            [ae.group-by-user.lib :as gbu-lib]
            [clojure.pprint :refer [pprint]]
            [ae.make-clustering-inputs
             [core :as mci]
             [lib :as mci-lib]]
            [clojure.test :refer :all]
            [sparkling.core :as spark]
            [clojure.string :as s]
            [ae.kmeans.lib :as lib]
            [cheshire.core :as json]
            [aws.s3 :as s3]
            [ae.lib :as core-lib]))

(defn kmeans
  []
  (let [root (str "s3://shareablee-hive/testing/" (core-lib/uuid))]
    (try
      (let [category (doto (core-lib/join-path root "category.txt")
                       (core-lib/put-key (json/generate-string {:id 0
                                                                :facebook [{:id "page1" :name "foo"}
                                                                           {:id "page2" :name "bar"}]})))
            group-by-user-dir (core-lib/join-path root "group-by-user/201512")
            make-inputs-dir (core-lib/join-path root "make-clustering-inputs/201512")
            clustering-dir (core-lib/join-path root "clustering/201512")]

        (core-lib/put-key (core-lib/join-path group-by-user-dir "00000")
                          (->> [
                                ;; {"page1" 0} a zero freq like this
                                ;; would normally be filtered out
                                ;; upstream, but is useful for easy to
                                ;; think about testing data. this data
                                ;; is a 10x10 grid, with 3 points in
                                ;; lower left and 3 in upper right.
                                {:user-id "a" :likes {"page1" 0}}
                                {:user-id "b" :likes {"page2" 1}}
                                {:user-id "c" :likes {"page1" 1}}
                                {:user-id "d" :likes {"page1" 9 "page2" 9}}
                                {:user-id "e" :likes {"page1" 8 "page2" 9}}
                                {:user-id "f" :likes {"page1" 9 "page2" 8}}]
                            (map (fn [{:keys [user-id likes]}]
                                   {:user-id user-id
                                    :gender "1"
                                    :page-like likes
                                    :page-comment {}
                                    :post-like {}
                                    :post-comment {}}))
                            (map #(map % gbu-lib/COLUMNS))
                            (map json/generate-string)
                            (s/join "\n")))
        (with-redefs [core-lib/shutdown (fn [])
                      core-lib/setup-logging (fn [])
                      ae.make-clustering-inputs.lib/-get-scaling-factor (constantly 1)] ;; dont scale the number of page actions to 0 < x < 1
          (mci/-main "false" category make-inputs-dir group-by-user-dir))
        (println :inputs  (str "\n" (.trim (core-lib/get-key (core-lib/join-path make-inputs-dir "inputs.txt")))))
        (println :dimensions (str "\n" (.trim (core-lib/get-key (core-lib/join-path make-inputs-dir "dimensions.txt")))))
        (with-redefs [core-lib/shutdown (fn [])
                      core-lib/setup-logging (fn [])]
          ;; TODO so kselect is erroring trying to pick k=2. it
          ;; should probably work on all inputs, even small
          ;; ones? minimizing cluster diameter, conceptually
          ;; there is no problem with these inputs?
          ;; (kselect/-main "single" make-inputs-dir "1" "random,100,0.001")
          (core-lib/put-key (core-lib/join-path make-inputs-dir "k.txt") "2")
          (core/-main "single" make-inputs-dir clustering-dir "1" (str "kmeans||,100,0.001")))
        (let [datas (->> "data.json"
                      (core-lib/join-path clustering-dir)
                      core-lib/get-key
                      s/split-lines
                      (map #(json/parse-string % true)))
              meta-datas (->> "meta-data.json"
                           (core-lib/join-path clustering-dir)
                           core-lib/get-key
                           s/split-lines
                           (map #(json/parse-string % true)))]
          (is (= #{{:user_ids ["d" "e" "f"]}
                   {:user_ids ["a" "b" "c"]}}
                 (set (map #(dissoc % :cluster_id) datas))))
          (is (= #{{:cluster_member_count 3,
                    :private false,
                    :cluster_diameter 1.4907119849998591,
                    :cluster_wssse 2.666666666666591,
                    :dimensions
                    [{:dimension_id "page1",
                      :dimension_name "page1",
                      :member_count 3,
                      :center 8.666666666666666,
                      :min 8.0,
                      :max 9.0,
                      :avg 8.666666666666666}
                     {:dimension_id "page2",
                      :dimension_name "page2",
                      :member_count 3,
                      :center 8.666666666666666,
                      :min 8.0,
                      :max 9.0,
                      :avg 8.666666666666666}]}
                   {:cluster_member_count 3,
                    :private false,
                    :cluster_diameter 1.4907119849998598,
                    :cluster_wssse 2.666666666666591,
                    :dimensions
                    [{:dimension_id "page1",
                      :dimension_name "page1",
                      :member_count 1,
                      :center 0.3333333333333333,
                      :min 0.0,
                      :max 1.0,
                      :avg 0.3333333333333333}
                     {:dimension_id "page2",
                      :dimension_name "page2",
                      :member_count 1,
                      :center 0.3333333333333333,
                      :min 0.0,
                      :max 1.0,
                      :avg 0.3333333333333333}]}}
                 (set (map #(dissoc % :cluster_id) meta-datas))))))
      (finally
        (core-lib/run "aws s3 rm" root "--recursive")))))

(when (System/getenv "TEST_SPARK")
  (deftest test-kmeans
    (kmeans)))
