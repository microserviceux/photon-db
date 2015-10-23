(ns photon.db)

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

(def set-records (ref #{}))

(defn implementations [] (into [] @set-records))

(defmacro functionize [macro]
  `(fn [& args#] (eval (cons '~macro args#))))

(defn class->record [cn]
  (let [tokens (clojure.string/split (.getName cn) #"\.")
        prefix (clojure.string/join "." (drop-last tokens))
        all (str prefix "/->" (last tokens))]
    (require (symbol prefix))
    (eval (read-string all))))

(defmacro defdbplugin [n & args]
  (dosync
    (let [r (apply (functionize defrecord) n args)]
      (alter set-records conj (class->record r))
      r)))

(defdbplugin DBDummy []
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

