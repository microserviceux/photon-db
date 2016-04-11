(ns photon.db.dummy
  (:require [photon.db :as db]))

(defn event []
  {:stream-name "cambio"
   :payload {:test :ok}
   :service-id "muon://dummy"
   :event-type "dummy"})

(defrecord DBDummy [conf]
  db/DB
  (db/driver-name [this] "dummy")
  (db/fetch [this stream-name id] (event))
  (db/delete! [this id])
  (db/delete-all! [this])
  (db/put [this data])
  (db/search [this id] [(event)])
  (db/store [this payload])
  (db/distinct-values [this k] ["cambio"])
  (db/lazy-events [this stream-name date]
                  (repeatedly event))
  (db/lazy-events-page [this stream-name date page] []))

