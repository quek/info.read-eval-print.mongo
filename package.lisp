;;;; package.lisp

(defpackage #:info.read-eval-print.mongo
  (:use #:cl #:info.read-eval-print.bson)
  (:shadow #:delete #:find)
  (:export #:connect
           #:db
           #:collection
           #:insert
           #:with-connection
           #:find
           #:next
           #:next-p))

