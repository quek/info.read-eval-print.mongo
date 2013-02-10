;;;; package.lisp

(defpackage #:info.read-eval-print.mongo
  (:use #:cl #:info.read-eval-print.bson)
  (:shadow #:delete #:find)
  (:export #:connect
           #:connect-replica-set
           #:with-connection
           #:with-replica-set
           #:db
           #:collection
           #:insert
           #:update
           #:find
           #:find-one
           #:find-all
           #:next
           #:next-p

           #:command
           #:stats

           #:$add-to-set
           #:$all
           #:$and
           #:$bit
           #:$box
           #:$center
           #:$center-sphere
           #:$comment
           #:$each
           #:$elem-match
           #:$exists
           #:$explain
           #:$gt
           #:$gte
           #:$hint
           #:$in
           #:$inc
           #:$isolated
           #:$lt
           #:$lte
           #:$max
           #:$max-distance
           #:$max-scan
           #:$min
           #:$mod
           #:$natural
           #:$ne
           #:$near
           #:$near-sphere
           #:$nin
           #:$nor
           #:$not
           #:$or
           #:$orderby
           #:$polygon
           #:$pop
           #:$
           #:$pull
           #:$pull-all
           #:$push
           #:$push-all
           #:$query
           #:$regex
           #:$rename
           #:$return-key
           #:$set
           #:$show-disk-loc
           #:$size
           #:$snapshot
           #:$type
           #:$unique-docs
           #:$unset
           #:$where
           #:$within

           #:map-reduce

           #:*default-connection*

           #:scan-mongo))
