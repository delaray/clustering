(defproject clustering "0.0.1"
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [amazonica "0.3.52"]
                 [com.climate/claypoole "1.1.2"]
                 [com.taoensso/timbre "4.3.1"]
                 [me.raynes/fs "1.4.6"]
                 [cheshire "5.5.0"]
                 [gorillalabs/sparkling "1.2.3"]
                 [org.clojure/tools.logging "0.2.3"]]

            
  :main nil
  :profiles {:kmeans {:aot [#"clustering.kmeans.*"
                            #"clustering.kselect.*"
                            clojure.tools.logging.impl
                            sparkling.serialization
                            sparkling.destructuring]}
             :uberjar {:aot [#".*"
                             clojure.tools.logging.impl
                             sparkling.serialization
                             sparkling.destructuring]}})
