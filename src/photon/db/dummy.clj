(ns photon.db.dummy
  (:require [photon.db :refer :all]))

(defn event []
  {:stream-name "cambio"
   :payload {:test :ok}
   :service-id "muon://dummy"
   :local-id (java.util.UUID/randomUUID)
   :server-timestamp (System/currentTimeMillis)})

(defdbplugin DBDummy [conf]
  DB
  (driver-name [this] "dummy")
  (fetch [this stream-name id] (event))
  (delete! [this id])
  (delete-all! [this])
  (put [this data])
  (search [this id] [(event)])
  (store [this payload])
  (distinct-values [this k] ["cambio"])
  (lazy-events [this stream-name date]
               (repeatedly event))
  (lazy-events-page [this stream-name date page] []))

