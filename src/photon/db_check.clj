(ns photon.db-check
  (:require [photon.db :as db]
            [photon.db.dummy :as dummy]))

(defn event [id stream-name]
  {:stream-name stream-name
   :payload {:test :ok}
   :order-id id
   :service-id "muon://dummy"
   :event-type "dummy"})

(defn db-check [impl]
  (let [ev (event 1 "st")]
    (db/store impl (event 1 "st"))
    (assert (= (db/fetch impl "st" 1) ev))
    (db/delete! impl 1)
    (assert (= (db/fetch impl "st" 1) nil))
    (db/store impl (event 1 "st"))
    (db/store impl (event 2 "st"))
    (assert (= (db/fetch impl "st" 2) (event 2 "st")))
    (db/delete-all! impl)
    (assert (= (db/fetch impl "st" 2) nil))
    (db/store impl (event 1 "st"))
    (db/store impl (event 2 "st"))
    (db/store impl (event 3 "st3"))
    (db/store impl (event 4 "st2"))
    (assert (= (count (db/search impl 2)) 1))
    (assert (= (db/distinct-values impl :order-id) #{1 2 3 4}))
    (assert (= (db/distinct-values impl :stream-name) #{"st" "st2" "st3"}))
    (db/delete-all! impl)
    (dorun (map #(db/store impl (event % "st")) (range 1000)))
    (dorun (map #(db/store impl (event (+ 1000 %) "st2")) (range 1000)))
    (dorun (map #(db/store impl (event (+ 2000 %) "st3")) (range 1000)))
    (dorun (map #(db/store impl (event (+ 3000 %) "st/1")) (range 1000)))
    (dorun (map #(db/store impl (event (+ 4000 %) "st/2")) (range 1000)))
    (dorun (map #(db/store impl (event (+ 5000 %) "st/3")) (range 1000)))
    (let [all (db/lazy-events impl "st" 0)]
      (assert (= (count all) 1000)))
    (let [all (db/lazy-events impl "st3" 0)]
      (assert (= (count all) 1000)))
    (let [all (db/lazy-events impl "st/3" 0)]
      (assert (= (count all) 1000)))
    (let [all (db/lazy-events impl "st**" 0)]
      (assert (= (count all) 6000)))
    (let [all (db/lazy-events impl "**" 0)]
      (assert (= (count all) 6000)))
    (db/delete-all! impl)
    (let [all (db/lazy-events impl "**" 0)]
      (assert (= (count all) 0)))
    true))
