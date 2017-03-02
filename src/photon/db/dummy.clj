(ns photon.db.dummy
  (:require [photon.db :as db]))

(def in-mem (ref {}))

(defrecord DBDummy [conf]
  db/DB
  (db/driver-name [this]
    "dummy")
  (db/fetch [this stream-name id]
    (first (filter #(and (= (:order-id %) id) (= (:stream-name %) stream-name))
                   (get @in-mem this))))
  (db/delete! [this id]
    (dosync
     (alter in-mem update this (fn [old]
                                 (into [] (remove #(= (:order-id %) id) old))))))
  (db/delete-all! [this]
    (dosync (alter in-mem dissoc this)))
  (db/search [this id]
    (filter #(= (:order-id %) id) (mapcat val @in-mem)))
  (db/store [this payload]
    (dosync
     (alter in-mem update this (fn [old] (into [] (conj old payload))))))
  (db/distinct-values [this k]
    (into #{} (map #(get % k) (mapcat val @in-mem))))
  (db/lazy-events [this stream-name date]
    (let [pattern (clojure.string/replace stream-name #"\*\*" "(.*)")
          regex (re-pattern pattern)]
      (filter #(re-matches regex (:stream-name %)) (mapcat val @in-mem)))))
