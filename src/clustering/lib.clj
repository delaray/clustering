(ns clustering.lib
  (:require [cheshire.core :as json]
            [me.raynes.fs :as fs]
            [clojure.java.shell :as sh]
            [clojure.string :as s]
            [taoensso.timbre :as timbre]))

(defnv log-rate
  [String Number [:Any] -> [:Any]]
  [name length xs]
  (let [start (System/nanoTime)]
    (->> xs
      (map vector (map inc (range)))
      (map (fn [[^long i x]]
             (let [now (System/nanoTime)
                   minutes (/ (- now start) 1000.0 1000.0 1000.0 60.0)
                   rate (double (/ i minutes))]
               (timbre/info
                name
                "completed:" i "/" length
                "progress-per-minute:" (format "%.2f" rate)
                "eta-minutes:" (long (/ (- ^long length i) rate))))
             x)))))

(defn temp-file
  []
  (.getAbsolutePath (fs/temp-file "")))

(defn temp-dir
  []
  (.getAbsolutePath (fs/temp-dir "")))

(defn basename
  [path]
  (-> path
    (s/replace #"/$" "")
    (s/replace #".*/(.+)$" "$1")))

(defn consistent-hash
  "Returns a zero-based index into a vector of size <num-slots> to place <x>."
  [^long num-slots ^String x]
  (-> x
    clojure.lang.Murmur3/hashUnencodedChars
    (mod num-slots)))

(defn run
  [& args]
  (let [cmd (apply str (interpose " " args))
        res (sh/sh "bash" "-c" cmd)]
    (timbre/info "run:", cmd)
    (assert (-> res :exit (= 0)) (assoc res :cmd cmd))
    (s/trim (:out res))))

(defn shutdown
  "Clean shutdown for cli entrypoints."
  []
  (Thread/sleep 1000) ; wait for timbre agent loggers
  (shutdown-agents)
  (System/exit 0)
  nil)

(defn setup-logging
  "Usage agents for logging so we dont get interleaved output in the logs.
   Wait a few seconds before shutting down agents on program exit to make
   sure you get all the output."
  []
  (timbre/merge-config! {:appenders {:println {:async? true}}}))

(defmacro with-tempdir
  [_ name & forms]
  `(let [~name (run "mktemp -d")]
     (try
       ~@forms
       (finally
         (run "rm -rf" ~name)))))

(defn retry
  [f [ms-now & ms-rest]]
  (let [[status res] (try
                       [::success (f)]
                       (catch Exception ex
                         [::fail ex]))]
    (condp = status
      ::fail (if-not ms-now
               (throw res)
               (do (Thread/sleep ms-now)
                   (recur f ms-rest)))
      ::success res)))

(defn retries
  []
  (->> (range 21)
    (drop 1)
    (map #(Math/pow % 1.5))
    (map #(* ^double % 1000))
    (map #(* ^double % ^double (rand)))
    (map long)))

(defn thread-name
  []
  (.getName (Thread/currentThread)))

(defn mkdir-spit
  [path content & opts]
  (try
    (apply spit path content opts)
    (catch java.io.FileNotFoundException _
      (fs/mkdirs (fs/parent path))
      (apply spit path content opts))))

(defn ls-dir
  [path & {:keys [abs-path]}]
  (let [res (s/split-lines (run "ls" path))]
    (if abs-path
      (mapv #(join-path path %) res)
      res)))

(defn uuid
  []
  (str (java.util.UUID/randomUUID)))
