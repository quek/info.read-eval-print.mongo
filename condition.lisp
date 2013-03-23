(in-package :info.read-eval-print.mongo)

(define-condition mongo-error (error)
  ())

(define-condition operation-failure (mongo-error)
  ((doc :initarg :doc :reader operation-failure-doc))
  (:report (lambda (condition stream)
             (format stream "MongoDb operation falied: ~a"
                     (operation-failure-doc condition)))))
