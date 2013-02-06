(in-package #:info.read-eval-print.mongo)

(find-all "test.foo")
;;⇒ ({"_id": ObjectId("50BCA1A0B5359BD32A42F3BA"), "document": "one"}
;;    {"_id": ObjectId("50BCA312B5359BD32A42F3BB"), "bar": 123}
;;    {"_id": ObjectId("50BCA331B5359BD32A42F3BC"), "bar": -8863300455836421197})

(find-one "test.foo")
;;⇒ {"_id": ObjectId("50BCA1A0B5359BD32A42F3BA"), "document": "one"}
