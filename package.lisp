;;;; package.lisp

(defpackage #:info.read-eval-print.mongo
  (:use #:cl #:info.read-eval-print.bson)
  (:shadow #:delete #:find)
  (:export #:connect
           #:db
           #:collection
           #:insert
           #:update
           #:with-connection
           #:find
           #:find-one
           #:find-all
           #:next
           #:next-p

           #:command
           #:stats

           #:$set

           #:map-reduce

           #:scan-mongo))

