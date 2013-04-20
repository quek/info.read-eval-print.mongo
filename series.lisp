(in-package #:info.read-eval-print.mongo)

(series::defS scan-mongo (collection
                          query
                          &key
                          (skip 0)
                          (limit 0)
                          sort
                          projection
                          tailable
                          (query-update-function (lambda (query last-value)
                                                   (declare (ignore last-value))
                                                   query))
                          slave-ok)
  "scan mongoDB collection."
  (series::fragl
   ;; args
   ((collection) (query) (skip) (limit) (sort) (projection) (tailable)
                 (query-update-function funcall)
                 (slave-ok))
   ;; rets
   ((result t))
   ;; aux
   ((result t) (cursor t) (%query t (or query (bson))))
   ;; alt
   ()
   ;; prolog
   (#1=(setq cursor (find collection %query
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
          (setq %query (funcall query-update-function %query result))
          #1#)
        (go start))
      (go series::end))
    (setq result (next cursor)))
   ;; epilog
   ((close cursor))
   ;; wraprs
   ()
   ;; impure
   nil))
