(eval-when (:compile-toplevel :load-toplevel :execute)
  (ql:quickload :info.read-eval-print.mongo)
  (ql:quickload :fiveam))

(defpackage :info.read-eval-print.mongo.test
  (:use :cl :info.read-eval-print.mongo :info.read-eval-print.bson :fiveam)
  (:shadowing-import-from :info.read-eval-print.mongo #:delete #:find)
  (:import-from :fiveam #:def-suite*))

(in-package :info.read-eval-print.mongo.test)

(defparameter *test-mongod* "/usr/bin/mongod")
(defparameter *test-db-dir* "/tmp/info.read-eval-print.mongo.test")
(defparameter *test-port* 27077)

(defun run-test-mongod ()
  (asdf:run-shell-command "rm -r ~a" *test-db-dir*)
  (ensure-directories-exist (concatenate 'string *test-db-dir* "/"))
  (let ((asdf::*verbose-out* *terminal-io*))
    (asdf:run-shell-command
     "~a --dbpath ~a --port ~a --pidfilepath ~a/mongod.pid ~
      --fork --logpath ~a/mongod.log --nojournal --noprealloc --smallfiles"
     *test-mongod* *test-db-dir* *test-port* *test-db-dir* *test-db-dir*)))

(defun kill-test-mongod ()
  (asdf:run-shell-command "kill `cat ~a/mongod.pid`" *test-db-dir*))

(defmacro with-test-mongod (&body body)
  `(unwind-protect
        (progn
          (run-test-mongod)
          ,@body)
     (kill-test-mongod)))


(defmacro with-test-collection ((collection &optional (collection-name "cons")) &body body)
  (let ((connection (gensym "connection"))
        (db (gensym "db")))
    `(with-test-mongod
       (with-connection (,connection :port *test-port*)
         (let* ((,db (db ,connection "test"))
                (,collection (collection ,db ,collection-name)))
           ,@body)))))

(def-suite all)

(def-suite* mongo :in all)

(test test-basic-operations
  (with-test-collection (collection)
    (insert collection (bson "foo" "bar"))
    (update collection (bson "foo" "bar") (bson ($set "ba" "po")))
    (let ((docs (loop with cursor = (find collection)
                      while (next-p cursor)
                      collect (next cursor))))
      (print docs)
      (is (= 1 (length docs)))
      (is (string= (value (car docs) "foo") "bar"))
      (is (string= (value (car docs) "ba") "po")))
    (delete collection (bson "foo" "bar"))
    (let ((docs (loop with cursor = (find collection)
                      while (next-p cursor)
                      collect (next cursor))))
      (is (null docs)))))

(debug!)

#+nil
(with-connection (connection)
  (let* ((db (db connection "test"))
         (collection (collection db "test")))
    (insert collection (bson "ぬぬも" "ぬもも"))))

#+nil
(with-connection (connection)
  (let* ((db (db connection "test"))
         (collection (collection db "test")))
    (let ((cursor (find collection (bson "ぬぬも" "ぬもも"))))
      (loop while (next-p cursor)
            collect (next cursor)))))
;;⇒ ({"_id": ObjectId("51052B6BEC509C381A7FD3CB"), "ぬぬも": "ぬもも"}
;;    {"_id": ObjectId("510658EE3A7EEDC3F3F0F94F"), "foo": "bar", "ぬぬも": "ぬもも"})


#+nil
(with-connection (connection)
  (let* ((db (db connection "test"))
         (collection (collection db "test")))
    (loop for i from 1 to 10000
          do (insert collection (bson "foo" i)))))
#+nil
(with-connection (connection)
  (let* ((db (db connection "test"))
         (collection (collection db "test")))
    (delete collection)))

#+nil
(with-connection (connection)
  (let* ((db (db connection "test"))
         (collection (collection db "test")))
    (with-open-stream (cursor (find collection nil))
      (length
       (loop while (next-p cursor)
             collect (next cursor))))))
