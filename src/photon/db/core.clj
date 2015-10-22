(ns photon.db.core)

(defn get-current-iso-8601-date
  "Returns current ISO 8601 compliant date."
  []
  (.format (java.text.SimpleDateFormat. "yyyy-MM-dd'T'HH:mm:ss'Z'")
           (.getTime (java.util.Calendar/getInstance))))

(defn datetime [] (str (get-current-iso-8601-date)))

(defn uuid [] (str (java.util.UUID/randomUUID)))

(defprotocol DB
  (fetch [this stream-name order-id])
  (delete! [this id])
  (delete-all! [this])
  (put [this data])
  (search [this id])
  (store [this payload])
  (distinct-values [this k])
  (lazy-events [this stream-name date])
  (lazy-events-page [this stream-name date page]))

(defrecord DBDummy []
  DB
  (fetch [this stream-name id] {})
  (delete! [this id])
  (delete-all! [this])
  (put [this data])
  (search [this id] [])
  (store [this payload])
  (distinct-values [this k] [])
  (lazy-events [this stream-name date] [])
  (lazy-events-page [this stream-name date page] []))

(defn -name->class [n]
  (try
    (let [c (Class/forName n)]
      (if (.isAssignableFrom photon.db.core.DB c) c nil))
    (catch Exception e nil)))

(defn db-implementations
  ([]
   (flatten (pmap db-implementations (all-ns))))
  ([one-ns]
   (let [pkg (str (.getName (.getName one-ns)) ".")
         symbs (map key (ns-map one-ns))
         potential (filter #(.startsWith (.getName %) "map->") symbs)
         class-names (map #(str pkg (subs (.getName %)
                                          5 (count (.getName %))))
                          potential)
         classes (map -name->class class-names)]
     (remove nil? classes))))

