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
  (search [this id])
  (store [this payload])
  (distinct-values [this k])
  (lazy-events [this stream-name date]))

(defn load-plugin [backend]
  (let [ns-str (symbol (str "photon.db." backend))]
    (require ns-str)
    (let [symbs (ns-publics (find-ns ns-str))
          candidate (first (filter #(.startsWith (name (key %)) "->") symbs))
          f (val candidate)]
      f)))

(defn default-db [conf]
  (let [target (:db.backend conf)]
    (try
      (let [plugin (load-plugin target)]
        ;; TODO: Reconsider exhaustive classpath search
        #_(log/info "Backend implementations available:"
                    (map #(driver-name (% conf)) impls))
        (log/info "Loaded backend for" target)
        plugin)
      (catch Exception e
        (log/error "Backend plugin for" target
                   "not loaded, falling back to dummy")
        (log/error "Cause of error:" (.getMessage e))
        (load-plugin "dummy")))))

