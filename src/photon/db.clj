(ns photon.db
  (:require [clojure.tools.logging :as log]
            [clojure.string :as s]
            [clojure.java.classpath :refer :all]))

(defn get-current-iso-8601-date
  "Returns current ISO 8601 compliant date."
  []
  (.format (java.text.SimpleDateFormat. "yyyy-MM-dd'T'HH:mm:ss'Z'")
           (.getTime (java.util.Calendar/getInstance))))

(defn datetime [] (str (get-current-iso-8601-date)))

(defn uuid [] (str (java.util.UUID/randomUUID)))

(defprotocol DB
  (driver-name [this])
  (fetch [this stream-name order-id])
  (delete! [this id])
  (delete-all! [this])
  (put [this data])
  (search [this id])
  (store [this payload])
  (distinct-values [this k])
  (lazy-events [this stream-name date])
  (lazy-events-page [this stream-name date page]))

(defonce set-records (ref #{}))

(defn implementations [] (into [] @set-records))

(defmacro functionize [macro]
  `(fn [& args#] (eval (cons '~macro args#))))

(defn class->record [cn]
  (let [tokens (clojure.string/split (.getName cn) #"\.")
        prefix (clojure.string/join "." (drop-last tokens))
        all (str prefix "/->" (last tokens))]
    (log/trace "Loading" prefix all)
    (require (symbol prefix))
    (eval (read-string all))))

(defmacro defdbplugin [n & args]
  (dosync
    (let [r (apply (functionize defrecord) n args)]
      (alter set-records conj (class->record r))
      r)))

(defdbplugin DBDummy [conf]
  DB
  (driver-name [this] "dummy")
  (fetch [this stream-name id] {})
  (delete! [this id])
  (delete-all! [this])
  (put [this data])
  (search [this id] [])
  (store [this payload])
  (distinct-values [this k] [])
  (lazy-events [this stream-name date] [])
  (lazy-events-page [this stream-name date page] []))

(defn -file->ns [f]
  (let [tokens (s/split f #"\.clj")
        main-part (s/join ".clj" tokens)
        nn (s/replace main-part #"\/" ".")
        nns (s/replace nn #"_" "-")]
    nns))

(def all-loaded (ref false))

(defn -load-db-plugins!
  ([jf]
   (let [files (filenames-in-jar jf)
         matches (filter #(and (.startsWith % "photon/db/")
                               (.endsWith % ".clj"))
                         files)
         codes (map #(.getInputStream jf (.getEntry jf %)) matches)]
     (dorun (map #(log/info "Loading" % "in" (.getName jf) "...") matches))
     (dorun (map #(let [n (-file->ns %)]
                    (log/trace "Requiring" n)
                    (require (symbol n)))
                 matches))))
  ([]
   (when (not @all-loaded)
     (dosync
       (log/info "Finding backend plugin implementations...")
       (let [jarfiles (classpath-jarfiles)]
         (dorun (map -load-db-plugins! jarfiles)))
       (alter all-loaded (fn [_] true))))))

(defn -find-implementation [conf impls n]
  (first (filter #(= n (driver-name (% conf))) impls)))

(defn available-backends []
  (-load-db-plugins!)
  (implementations))

(defn default-db [conf]
  (-load-db-plugins!)
  (let [target (:db.backend conf)
        impls (implementations)
        chosen (-find-implementation conf impls target)]
    (log/info "Backend implementations available:"
              (map #(driver-name (% conf)) impls))
    (if (nil? chosen)
      (do
        (log/error "Backend plugin for" target
                   "not found, falling back to dummy")
        ((-find-implementation conf impls "dummy") conf))
      (do
        (log/info "Loaded backend for" target)
        (chosen conf)))))

