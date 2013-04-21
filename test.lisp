(eval-when (:compile-toplevel :load-toplevel :execute)
  (ql:quickload :info.read-eval-print.mongo)
  (ql:quickload :fiveam)
  (ql:quickload :info.read-eval-print.double-quote))

(defpackage :info.read-eval-print.mongo.test
  (:use :cl :info.read-eval-print.mongo :info.read-eval-print.bson :fiveam :series)
  (:shadowing-import-from :info.read-eval-print.mongo #:count #:delete #:find)
  (:import-from :fiveam #:def-suite*)
  (:import-from :parenscript #:@ #:chain)
  (:import-from :info.read-eval-print.bson #:regex))

(in-package :info.read-eval-print.mongo.test)

(named-readtables:in-readtable info.read-eval-print.double-quote:|#"|)

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
  (asdf:run-shell-command "kill `pgrep -f '~a .* --port ~a'`" *test-mongod* *test-port*))

(defmacro with-test-mongod (&body body)
  `(progn
     (run-test-mongod)
     (unwind-protect
          (progn ,@body)
       (kill-test-mongod))))


(defun run-test-replica-set ()
  (let ((asdf::*verbose-out* *terminal-io*))
    (loop for i from 1 to 3
          for db-dir = #"""#,*test-db-dir*,.#,i"""
          do (asdf:run-shell-command #"""rm -rf #,db-dir""")
             (ensure-directories-exist #"""#,db-dir,/""")
             (asdf:run-shell-command
              #"""#,*test-mongod* --dbpath #,db-dir --port #,(+ i *test-port*) ~
--pidfilepath #,db-dir,/mongod.pid ~
--fork --logpath #,db-dir,/mongod.log --nojournal --noprealloc --smallfiles ~
--replSet rs0"""))
    (let ((hostname (isys:gethostname)))
      (asdf:run-shell-command #"""mongo --port #,(1+ *test-port*) --eval '~
rs.initiate(); ~
while(!rs.isMaster().ismaster) sleep(100); ~
rs.add("#,hostname,:#,(+ 2 *test-port*)"); ~
rs.add("#,hostname,:#,(+ 3 *test-port*)");'""")
      (loop for i from 2 to 3
            do (asdf:run-shell-command #"""mongo --port #,(+ i *test-port*) --eval '~
while(!rs.isMaster().secondary) sleep(100);'""")))))

(defun kill-test-replica-set ()
  (let ((asdf::*verbose-out* *terminal-io*))
    (loop for i from 1 to 3
          do (asdf:run-shell-command
              #"""kill `pgrep -f '#,*test-mongod* .* --port #,(+ i *test-port*)'`"""))))

(defmacro with-test-replica-set (&body body)
  `(progn
     (run-test-replica-set)
     (unwind-protect
          (progn ,@body)
       (kill-test-replica-set))))


(defparameter *test-db* nil)

(defmacro with-test-db (&body body)
  (let ((connection (gensym "connection")))
    `(with-test-mongod
       (sleep 0.2)
       (with-connection (,connection #"""localhost:#,*test-port*""")
         (setf *test-db* (db ,connection "test"))
         ,@body))))

(defmacro with-test-collection ((collection &optional (collection-name "cons")) &body body)
  `(let ((,collection (collection *test-db* ,collection-name)))
     (delete ,collection nil)
     ,@body))

(def-suite all)

(def-suite* mongo :in all)

(test test-basic-operations
  (with-test-collection (collection)
    (insert collection (bson "foo" "bar"))
    (update collection (bson "foo" "bar") (bson ($set "ba" "po")))
    (let ((docs (loop with cursor = (find collection nil)
                      while (next-p cursor)
                      collect (next cursor))))
      (is (= 1 (length docs)))
      (is (string= (value (car docs) "foo") "bar"))
      (is (string= (value (car docs) "ba") "po")))
    (let ((doc (find-one collection (bson "ba" "po"))))
      (is (string= (value doc "ba") "po")))
    (delete collection (bson "foo" "bar"))
    (let ((docs (loop with cursor = (find collection nil)
                      while (next-p cursor)
                      collect (next cursor))))
      (is (null docs)))))

(test projection
  (with-test-collection (c)
    (insert c (bson :a 1 :b 2 :c 3))
    (let ((x (find-one c nil '(:b :c (:_id)))))
      (is (bson= (bson :b 2 :c 3) x)))
    (let ((x (find-one c nil '((:_id) (:b)))))
      (is (bson= (bson :a 1 :c 3) x)))))

(test sort
  "find :sort"
  (with-test-collection (collection)
    (is (null (collect (scan-mongo collection nil))))
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
    (let ((stats (stats (db collection))))
      (is (string= "test" (value stats :db)))
      (is (value stats :collections))
      (is (value stats :objects)))))

(test test-count
  (with-test-collection (col)
    (collect-ignore (insert col (bson :foo (scan-range :length 12))))
    (is (= 12 (count col nil)))
    (is (= 3 (count col (bson ($lt :foo 3)))))
    (is (= 10 (count col (bson ($gt :foo 0)) :skip 1)))
    (is (= 2 (count col nil :limit 2)))))

(test find-and-modify
  (with-test-collection (c)
    (insert c (bson :a 1 :b 2))
    (insert c (bson :a 11 :b 22))
    (let ((x (find-and-modify c (bson :a 11) :update (bson ($set :b 222)))))
      (is (= 22 (value x :b)))
      (is (= 222 (value (find-one c (bson :a 11)) :b))))
    (let ((x (find-and-modify c (bson :a 11) :update (bson ($set :b 2222)) :new t)))
      (is (= 2222 (value x :b))))
    (let ((x (find-and-modify c (bson :a 11 ($exists :c nil)) :update (bson ($set :c 7)) :new t)))
      (is (= 7 (value x :c))))
    (let ((x (find-and-modify c (bson :a 11 ($exists :c nil)) :update (bson ($set :c 77)) :new t)))
      (is (eq nil x)))
    (let ((x (find-and-modify c (bson :a 11 ($exists :c t)) :update (bson ($unset :c)) :new t)))
      (multiple-value-bind (value ok?) (value x :c)
        (is (eq nil value))
        (is (eq nil ok?))))
    (let ((x (find-all c (bson :a 1 :b 2))))
      (is (= 1 (length x)))
      (let ((x (car x)))
        (is (= 1 (value x :a)))
        (is (= 2 (value x :b)))))))

(test tailable-cursor
  (let* ((capped-collection (make-collection *test-db* "capped" :capped t :size 3)))
    (insert capped-collection (bson :a 1))
    (let ((cursor (find capped-collection nil :tailable t)))
      (is (next cursor))
      (is (eq :no-data (next cursor :error-value :no-data)))
      (insert capped-collection (bson :a 1))
      (is (= 1 (value (next cursor) :a))))))

(test map-reduce
  (with-test-collection (c)
    (loop for cust-id from 1 to 10
          do (loop for price from 10 to 20 by 10
                   do (insert c (bson "custId" cust-id "price" (* price cust-id)))))
    (map-reduce c
                '(lambda ()
                  (emit (@ this cust-id) (@ this price)))
                '(lambda (cust-id prices)
                  (chain *array (sum prices)))
                :out "hoge")
    (let ((hoge (collection *test-db* "hoge")))
      (is (= 10 (length (find-all hoge nil))))
      (loop for cust-id from 1 to 10
            for price = (* 30 cust-id)
            do (is (= price
                      (value (find-one hoge (bson :_id cust-id)) :value)))))))

(test operators
  (is (bson= (bson "$addToSet" (bson "field" "value"))
             (bson ($add-to-set "field" "value"))))
  (is (bson= (bson "tags" (bson "$all" '("a" "b")))
             (bson ($all :tags "a" "b"))))
  (is (bson= (bson "$and" (list (bson "a" "b") (bson "c" "d")))
             (bson ($and (bson "a" "b") (bson "c" "d")))))
  (is (bson= (bson "$bit" (bson "field" (bson "and" 5)))
             (bson ($bit "field" :and 5))))
  (is (bson= (bson "loc" (bson "$within" (bson "$box" '((1 2) (101 102)))))
             (bson ($within :loc ($box 1 2 101 102)))))
  (is (bson= (bson "$center" '((1 2) 10))
             ($center 1 2 10)))
  (is (bson= (bson "$centerSphere" '((1 2) 10))
             ($center-sphere 1 2 10)))
  (is (bson= (bson "$comment" "foo")
             (bson ($comment "foo"))))
  (is (bson= (bson "$addToSet" (bson "field" (bson "$each" '(1 2 3))))
             (bson ($add-to-set "field" ($each 1 2 3)))))
  (is (bson= (bson "array" (bson "$elemMatch" (bson "value1" 1 "value2" (bson "$gt" 1))))
             (bson ($elem-match "array" "value1" 1 ($gt "value2" 1)))))
  (is (bson= (bson "field" (bson "$exists" +bson-true+))
             (bson ($exists "field" t))))
  (is (bson= (bson "field" (bson "$exists" +bson-false+))
             (bson ($exists :field nil))))
  (is (bson= (bson "field" (bson "$gt" 1))
             (bson ($gt :field 1))))
  (is (bson= (bson "field" (bson "$gte" 1))
             (bson ($gte :field 1))))
  (is (bson= (bson "field" (bson "$in" '(1 2)))
             (bson ($in :field 1 2))))
  (is (bson= (bson "$inc" (bson "field" 1))
             (bson ($inc :field 1))))
  (is (bson= (bson "field1" 1 "$isolated" 1)
             (bson "field1" 1 ($isolated))))
  (is (bson= (bson "field" (bson "$lt" 1))
             (bson ($lt :field 1))))
  (is (bson= (bson "field" (bson "$lte" 1))
             (bson ($lte :field 1))))
  (is (bson= (bson "qty" (bson "$mod" '(4 0)))
             (bson ($mod :qty 4 0))))
  (is (bson= (bson "field" (bson "$ne" 1))
             (bson ($ne :field 1))))
  (is (bson= (bson "location" (bson "$near" (list 10 100)))
             (bson ($near :location 10 100))))
  (is (bson= (bson "location" (bson "$near" (list 10 100) "$maxDistance" 50))
             (bson ($near :location 10 100 50))))
  (is (bson= (bson "location" (bson "$nearSphere" (list 10 100)))
             (bson ($near-sphere :location 10 100))))
  (is (bson= (bson "location" (bson "$nearSphere" (list 10 100) "$maxDistance" 50))
             (bson ($near-sphere :location 10 100 50))))
  (is (bson= (bson "qty" (bson "$nin" '(5 15)))
             (bson ($nin :qty 5 15))))
  (is (bson= (bson "$nor" (list (bson "a" "b") (bson "c" "d")))
             (bson ($nor (bson "a" "b") (bson "c" "d")))))
  (is (bson= (bson "item" (bson "$not" (regex "^p.*")))
             (bson ($not :item (regex "^p.*")))))
  (is (bson= (bson "price" (bson "$not" (bson "$gt" 1.99)))
             (bson ($not ($gt :price 1.99)))))
  (is (bson= (bson "$or" (list (bson "a" "b") (bson "c" "d")))
             (bson ($or (bson "a" "b") (bson "c" "d")))))
  (is (bson= (bson "loc" (bson "$within" (bson "$polygon" '((0 0) (3 6) (6 0)))))
             (bson ($within :loc ($polygon 0 0 3 6 6 0)))))
  (is (bson= (bson "$pop" (bson "field" 1))
             (bson ($pop :field :last))))
  (is (bson= (bson "$pop" (bson "field" -1))
             (bson ($pop :field :first))))
  (is (bson= (bson "$pull" (bson "field" "value"))
             (bson ($pull :field "value"))))
  (is (bson= (bson "$pullAll" (bson "field" '(1 2 3)))
             (bson ($pull-all :field 1 2 3))))
  (is (bson= (bson "$push" (bson "field" "value"))
             (bson ($push :field "value"))))
  (is (bson= (bson "$pushAll" (bson "field" '(1 2 3)))
             (bson ($push-all :field 1 2 3))))
  (is (bson= (bson "$rename" (bson "nickname" "alias" "mobile" "cell"))
             (bson ($rename "nickname" "alias" "mobile" "cell"))))
  (is (bson= (bson "field" (bson "$size" 2))
             (bson ($size :field 2))))
  (is (bson= (bson "price" (bson "$type" 1))
             (bson ($type :price +type-double+))))
  (is (bson= (bson "$unset" (bson "field" ""))
             (bson ($unset :field))))
  (is (bson= (bson "$where" "this.credits == this.debits")
             (bson ($where "this.credits == this.debits"))))
  (is (bson= (bson "$where" (make-instance 'javascript-code :code "function () {
    return this.credits === this.debits;
}"))
             (bson ($where '(lambda () (= (chain this credits) (chain this debits))))))))

#+replica-setのテストをする?
(test replica-set
  (with-test-replica-set
    (let ((connection (connect
                       (loop for i from 1 to 3
                             collect #"""#,(isys:gethostname),:#,(+ i *test-port*)"""))))
      (unwind-protect
           (progn
             (is (slot-value connection 'info.read-eval-print.mongo::primary))
             (is (= 2 (length (slot-value connection 'info.read-eval-print.mongo::slaves))))
             (let* ((db (db connection "test"))
                    (collection (collection db "nya1")))
               (insert collection (bson :foo "bar"))
               (is (string= "bar" (value (find-one collection nil) :foo)))))
        (close connection)))
    (let ((connection (connect
                       (loop for i from 3 to 4
                             collect #"""#,(isys:gethostname),:#,(+ i *test-port*)"""))))
      (unwind-protect
           (progn
             (is (slot-value connection 'info.read-eval-print.mongo::primary))
             (is (= 2 (length (slot-value connection 'info.read-eval-print.mongo::slaves))))
             (let* ((db (db connection "test"))
                    (collection (collection db "nya2")))
               (insert collection (bson :foo "bar"))
               (is (string= "bar" (value (find-one collection nil) :foo)))
               (let ((asdf::*verbose-out* *terminal-io*))
                 (asdf:run-shell-command
                  #"""kill `pgrep -f '#,*test-mongod* .* --port #,(+ 1 *test-port*)'`"""))
               (is (string= "bar" (value (find-one collection nil) :foo)))))
        (close connection)))))

(with-test-db
  (debug!))

