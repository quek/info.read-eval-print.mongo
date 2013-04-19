(in-package #:info.read-eval-print.mongo)

(series::defS scan-mongo (collection query &key (skip 0) (limit 0) sort projection
                                     tailable query-update-function
                                     slave-ok)
  "scan mongoDB collection."
  (series::fragl
   ;; args
   ((collection) (query) (skip) (limit) (sort) (projection) (tailable)
                 (query-update-function funcall)
                 (slave-ok))
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
                       :await-data tailable
                       :slave-ok slave-ok)))
   ;; body
   (start
    (unless (next-p cursor)
      (when tailable
        (when (dead-p cursor)
          (close cursor)
          (setq cursor (find collection
                             (funcall query-update-function query bson)
                             :skip skip
                             :limit limit
                             :sort sort
                             :projection projection
                             :tailable tailable
                             :await-data tailable
                             :slave-ok slave-ok)))
        (go start))
      (go series::end))
    (setq bson (next cursor)))
   ;; epilog
   ((close cursor))
   ;; wraprs
   ()
   ;; impure
   nil))
