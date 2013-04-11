(in-package #:info.read-eval-print.mongo)

(series::defS scan-mongo (collection query &key (skip 0) (limit 0) sort projection)
  "scan mongoDB collection."
  (series::fragl
   ;; args
   ((collection) (query) (skip) (limit) (sort) (projection))
   ;; rets
   ((bson t))
   ;; aux
   ((bson t) (cursor t))
   ;; alt
   ()
   ;; prolog
   ((setq cursor (find collection query
                       :skip skip
                       :limit limit
                       :sort sort
                       :projection projection)))
   ;; body
   ((unless (next-p cursor)
      (go series::end))
    (setq bson (next cursor)))
   ;; epilog
   ((close cursor))
   ;; wraprs
   ()
   ;; impure
   nil))
