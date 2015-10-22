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

