(in-package #:info.read-eval-print.mongo)

(find-all "test.foo")
;;⇒ ({"_id": ObjectId("50BCA1A0B5359BD32A42F3BA"), "document": "one"}
;;    {"_id": ObjectId("50BCA312B5359BD32A42F3BB"), "bar": 123}
;;    {"_id": ObjectId("50BCA331B5359BD32A42F3BC"), "bar": -8863300455836421197})

(find-one "test.foo")
;;⇒ {"_id": ObjectId("50BCA1A0B5359BD32A42F3BA"), "document": "one"}

(value (command (db *default-connection* "test") :ismaster) :ismaster)
;;⇒ T

;;⇒ {"ismaster": T, "maxBsonObjectSize": 16777216, "localTime": @2013-02-08T20:23:11.000794+09:00, "ok": 1.0d0}


(with-connection (c :port 27078)
  (command (db c "test") :ismaster))
;;⇒ {"setName": "rs0", "ismaster": T, "secondary": NIL, "hosts": ["yarn:27078", "yarn:27080", "yarn:27079"], "primary": "yarn:27078", "me": "yarn:27078", "maxBsonObjectSize": 16777216, "localTime": @2013-02-10T14:26:34.000677+09:00, "ok": 1.0d0}

(with-connection (c :port 27079)
  (command (db c "test") :ismaster))
;;⇒ {"setName": "rs0", "ismaster": NIL, "secondary": T, "hosts": ["yarn:27079", "yarn:27080", "yarn:27078"], "primary": "yarn:27078", "me": "yarn:27079", "maxBsonObjectSize": 16777216, "localTime": @2013-02-10T14:26:50.000771+09:00, "ok": 1.0d0}

(with-connection (c :port 27080)
  (command (db c "test") :ismaster))
;;⇒ {"setName": "rs0", "ismaster": NIL, "secondary": T, "hosts": ["yarn:27080", "yarn:27079", "yarn:27078"], "primary": "yarn:27078", "me": "yarn:27080", "maxBsonObjectSize": 16777216, "localTime": @2013-02-10T14:43:58.000381+09:00, "ok": 1.0d0}

(describe (connect-replica-set "yarn:27078" "yarn:27079" "yarn:27080"))
;;→ #<REPLICA-SET {1015BDF7F3}>
;;     [standard-object]
;;   
;;   Slots with :INSTANCE allocation:
;;     HOST        = #<unbound slot>
;;     PORT        = #<unbound slot>
;;     REQUEST-ID  = 0
;;     STREAM      = #<unbound slot>
;;     HOSTS       = ("yarn:27078" "yarn:27079" "yarn:27080")
;;     PRIMARY     = #<CONNECTION {1015BDF8C3}>
;;     SLAVES      = (#<CONNECTION {1015BEE7A3}> #<CONNECTION {1015BEB133}>)
;;   
;;⇒ 


(let ((c (connect-replica-set "yarn:27079")))
  (close c)
  (describe c))
;;→ #<REPLICA-SET {100377F293}>
;;     [standard-object]
;;   
;;   Slots with :INSTANCE allocation:
;;     HOST        = #<unbound slot>
;;     PORT        = #<unbound slot>
;;     REQUEST-ID  = 0
;;     STREAM      = #<unbound slot>
;;     HOSTS       = ("yarn:27079")
;;     PRIMARY     = #<CONNECTION {1003782A53}>
;;     SLAVES      = (#<CONNECTION {10037860B3}> #<CONNECTION {100377F363}>)
;;   
;;⇒ 


(with-replica-set (c "yarn:27078" "yarn:27079" "yarn:27080")
  (let* ((db (db c "test"))
         (cl (collection db "nya1")))
    (find-all cl)))
;;⇒ ({"_id": ObjectId("5117952A96B30D90C32EE11E"), "foo": "bar"})



(with-connection (c)
  (let* ((db (db c "outing"))
         (col (collection db "logs.app")))
    (series:collect (scan-mongo col (bson ($gte :time (datetime 2012 12 26 13 55 51)))
                                :limit 1))))
;;⇒ ({"_id": ObjectId("50DA83E3732C156605000009"), "messages": ["Started GET \"/facilities\" for 127.0.0.1 at 2012-12-26 13:55:51 +0900", "Processing by FacilitiesController#index as HTML", "2012/12/26 13:55:51 \"\" \"fe03df132cde54e2120d3ad4433329e34fc855b6\" \"Mozilla/5.0 (X11; Linux x86_64; rv:10.0.11) Gecko/20100101 Firefox/10.0.11 Iceweasel/10.0.11\" GET /facilities", "Completed 200 OK in 372ms (Views: 328.7ms | ActiveRecord: 23.4ms | Solr: 6.7ms)"], "level": "INFO", "time": @2012-12-26T13:55:51.000000+09:00})



(with-connection (c)
  (let* ((db (db c "test"))
         (col (collection db "foo")))
    (insert col (bson :a (datetime 2013 1 2 3 4 5 6 7 8)))))
;;⇒ 



(with-connection (c '("yarn:27077" "yarn:27078" "yarn:27079"))
  (describe c))