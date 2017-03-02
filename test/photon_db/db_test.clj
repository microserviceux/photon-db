(ns photon-db.db-test
  (:require [photon.db :as db]
            [photon.db.dummy :as dummy]
            [photon.db-check :as check]
            [clojure.test :refer :all]))

(deftest db-check-test
  (let [impl (dummy/->DBDummy {})]
    (is (true? (check/db-check impl)))))
