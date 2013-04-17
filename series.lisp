(in-package #:info.read-eval-print.mongo)

(series::defS scan-mongo (collection query &key (skip 0) (limit 0) sort projection
                                     tailable slave-ok)
  "scan mongoDB collection."
  (series::fragl
   ;; args
   ((collection) (query) (skip) (limit) (sort) (projection) (tailable) (slave-ok))
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
                       :projection projection
                       :tailable tailable
                       :slave-ok slave-ok)))
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
