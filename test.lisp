(eval-when (:compile-toplevel :load-toplevel :execute)
  (ql:quickload :info.read-eval-print.mongo)
  (ql:quickload :fiveam))

(defpackage :info.read-eval-print.mongo.test
  (:use :cl :info.read-eval-print.mongo :info.read-eval-print.bson :fiveam :series)
  (:shadowing-import-from :info.read-eval-print.mongo #:delete #:find)
  (:import-from :fiveam #:def-suite*))

(in-package :info.read-eval-print.mongo.test)

(defparameter *test-mongod* "/usr/bin/mongod")
(defparameter *test-db-dir* "/tmp/info.read-eval-print.mongo.test")
(defparameter *test-port* 27077)

(defun run-test-mongod ()
  (asdf:run-shell-command "rm -rf ~a" *test-db-dir*)
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
    (let ((doc (find-one collection (bson "ba" "po"))))
      (is (string= (value doc "ba") "po")))
    (delete collection (bson "foo" "bar"))
    (let ((docs (loop with cursor = (find collection)
                      while (next-p cursor)
                      collect (next cursor))))
      (is (null docs)))

    (loop for i from 1 to 10
          do (insert collection (bson "test" "sort" "value" i)))
    (let ((xs (collect (scan-mongo collection (bson "test" "sort") :sort "value"))))
      (is (= 1 (value (car xs) "value")))
      (is (= 10 (value (car (last xs)) "value"))))
    (let ((xs (collect (scan-mongo collection (bson "test" "sort") :sort '("value" :desc)))))
      (is (= 10 (value (car xs) "value")))
      (is (= 1 (value (car (last xs)) "value"))))))

(test test-command
  (with-test-collection (collection)
    (insert collection (bson "あいう" "まみむ"))
    (let ((stats (print (stats (db collection)))))
      (is (string= "test" (value stats :db)))
      (is (value stats :collections))
      (is (value stats :objects)))))

(debug!)
