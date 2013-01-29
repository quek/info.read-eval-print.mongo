(ql:quickload :info.read-eval-print.mongo)

(defpackage :info.read-eval-print.mongo.test
  (:use :cl :info.read-eval-print.mongo :info.read-eval-print.bson)
  (:shadowing-import-from :info.read-eval-print.mongo #:find))

(in-package :info.read-eval-print.mongo.test)

(with-connection (connection)
  (let* ((db (db connection "test"))
         (collection (collection db "test")))
    (insert collection (bson "ぬぬも" "ぬもも"))))

(with-connection (connection)
  (let* ((db (db connection "test"))
         (collection (collection db "test")))
    (let ((cursor (find collection (bson "ぬぬも" "ぬもも"))))
      (loop while (next-p cursor)
            collect (next cursor)))))
;;⇒ ({"_id": ObjectId("51052B6BEC509C381A7FD3CB"), "ぬぬも": "ぬもも"}
;;    {"_id": ObjectId("510658EE3A7EEDC3F3F0F94F"), "foo": "bar", "ぬぬも": "ぬもも"})



(with-connection (connection)
  (let* ((db (db connection "test"))
         (collection (collection db "test")))
    #+nil
    (loop for i from 1 to 30000
          do (insert collection (bson "foo" i)))
    (with-open-stream (cursor (find collection))
      (loop while (next-p cursor)
            collect (next cursor)))))
