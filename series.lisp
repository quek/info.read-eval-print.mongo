(in-package #:info.read-eval-print.mongo)

(series::defS scan-mongo (collection query &key (skip 0) (limit 0) sort)
  "scan mongoDB collection."
  (series::fragl
   ;; args
   ((collection) (query) (skip) (limit) (sort))
   ;; rets
   ((bson t))
   ;; aux
   ((bson t) (cursor t))
   ;; alt
   ()
   ;; prolog
   ((setq cursor (find collection query :skip skip :limit limit :sort sort)))
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