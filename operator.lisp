(in-package #:info.read-eval-print.mongo)

(defun $set (key value)
  (cons "$set" (bson key value)))

(defun $push (key value)
  (cons "$push" (bson key value)))


