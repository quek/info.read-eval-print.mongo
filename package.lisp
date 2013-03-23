;;;; package.lisp

(defpackage #:info.read-eval-print.mongo
  (:use #:cl #:info.read-eval-print.bson)
  (:shadow #:count #:delete #:find)
  (:export #:connect
           #:connect-replica-set
           #:with-connection
           #:with-replica-set
           #:db
           #:collection
           #:insert
           #:update
           #:delete
           #:find
           #:find-one
           #:find-all
           #:next
           #:next-p
           #:count

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
           #:$gt  #:$>
           #:$gte #:$>=
           #:$hint
           #:$in
           #:$inc
           #:$isolated
           #:$lt  #:$<
           #:$lte #:$<=
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
           #:find-and-modify

           #:*default-connection*

           #:scan-mongo

           #:mongo-error
           #:operation-failure))
