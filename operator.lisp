(in-package #:info.read-eval-print.mongo)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Query and update operators

(defun $add-to-set (field value)
  (cons "$addToSet" (bson field value)))

(defun $all (field &rest values)
  (cons field (bson "$all" values)))

(defun $and (&rest expressions)
  (cons "$and" expressions))

(defun $bit (field op bit)
  "op is :and, :or, ..."
  (cons "$bit" (bson field (bson op bit))))

(defun $box (x1 y1 x2 y2)
  (bson "$box" (list (list x1 y1) (list x2 y2))))

(defun $center (x y radius)
  (bson "$center" (list (list x y) radius)))

(defun $center-sphere (x y radius)
  (bson "$centerSphere" (list (list x y) radius)))

(defun $comment (comment)
  (cons "$comment" comment))

(defun $each (&rest values)
  (bson "$each" values))

(defun $elem-match (array-field &rest matches)
  (cons array-field (bson "$elemMatch" (apply #'bson matches))))

(defun $exists (field boolean)
  (cons field (bson "$exists" (if boolean +bson-true+ +bson-false+))))

(defun $gt (field value)
  (cons field (bson "$gt" value)))

(defun $gte (field value)
  (cons field (bson "$gte" value)))

(defun $in (field &rest values)
  (cons field (bson "$in" values)))

(defun $inc (field &optional (amount 1))
  (cons "$inc" (bson field amount)))

(defun $isolated ()
  (cons "$isolated" 1))

(defun $lt (field value)
  (cons field (bson "$lt" value)))

(defun $lte (field value)
  (cons field (bson "$lte" value)))


;; $max-distance
;; $near 以外でも $max-distance は使われる?

(defun $mod (field divisor remainder)
  (cons field (bson "$mod" (list divisor remainder))))

(alexandria:define-constant $natural "$natural" :test 'string=)

(defun $ne (field value)
  "{ field: { $ne: value } }"
  (cons field (bson "$ne" value)))

(defun $near (field x y &optional max-distance)
  (cons field (apply #'bson "$near" (list x y)
                     (when max-distance
                       (list "$maxDistance" max-distance)))))

(defun $near-sphere (field x y &optional max-distance)
  (cons field (apply #'bson "$nearSphere" (list x y)
                     (when max-distance
                       (list "$maxDistance" max-distance)))))

(defun $nin (field &rest values)
  (cons field (bson "$nin" values)))

(defun $nor (&rest expressions)
  (cons "$nor" expressions))

(defun $not (field-or-expression &optional regex)
  (if regex
      (cons field-or-expression (bson "$not" regex))
      (cons (car field-or-expression)
            (bson "$not" (cdr field-or-expression)))))

(defun $or (&rest expressions)
  (cons "$or" expressions))

(defun $polygon (x1 y1 x2 y2 x3 y3)
  (bson "$polygon" (list (list x1 y1) (list x2 y2) (list x3 y3))))

(defun $pop (field first-or-last)
  (cons "$pop" (bson field (if (eq first-or-last :first) -1 1))))

(defun $pull (field value)
  (cons "$pull" (bson field value)))

(defun $pull-all (field &rest values)
  (cons "$pullAll" (bson field values)))

(defun $push (key value)
  (cons "$push" (bson key value)))

(defun $push-all (field &rest values)
  (cons "$pushAll" (bson field values)))

(defun $rename (&rest old-new-list)
  (cons "$rename" (apply #'bson old-new-list)))

(defun $set (key value)
  (cons "$set" (bson key value)))

(defun $size (field size)
  (cons field (bson "$size" size)))

(defun $type (field type)
  (cons field (bson "$type" type)))

(defun $unset (field)
  (cons "$unset" (bson field "")))

(defgeneric $where (js)
  (:method ((js string))
    (cons "$where" js))
  (:method ((js list))
    (cons "$where" (js js))))

(defun $within (field value &key (unique-docs t))
  (let ((bson (bson "$within" value)))
    (unless unique-docs
      (setf (value bson "$uniqueDocs") +bson-false+))
   (cons field bson)))


#|
$explain
$hint
$max
$max-scan
$min
$orderby
$query
$return-key
$show-disk-loc
$snapshot
|#