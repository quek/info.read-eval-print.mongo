(in-package :info.read-eval-print.mongo)

(defvar *api-table* (make-hash-table))

(defmacro api-defun (name lambda-list &body body)
  (setf (gethash name *api-table*) (cons 'defun lambda-list))
  `(defun ,name ,lambda-list
     ,@body))

(defvar *scan-lambda-list* nil)

(defmacro api-defS (name lambda-list &body body)
  (setf *scan-lambda-list* lambda-list)
  `(series::defS ,name ,lambda-list
     ,@body))


(defmacro def-api (db-name collection-name
                   &key (prefix collection-name)
                     (package *package*))
  (labels ((args (xs)
             (let (args)
               (loop with key = nil
                     for x in (cdr xs)
                     do (cond ((eq x '&key)
                               (setf key t))
                              ((not (member x '(&rest &optional &allow-other-keys)))
                               (if (consp x)
                                   (progn
                                     (when key (push (intern (symbol-name (car x)) :keyword) args))
                                     (push (car x) args))
                                   (progn
                                     (when key (push (intern (symbol-name x) :keyword) args))
                                     (push x args))))))
               (nreverse args))))
    `(let* ((db (make-instance 'db :name ,db-name))
            (collection (collection db ,collection-name)))
       ,@(collect (mapping (((name def.lambda-list)  (scan-hash *api-table*)))
                    `(,(car def.lambda-list)
                      ,(alexandria:format-symbol
                        package "~:@(~a.~a~)" prefix name) ,(cddr def.lambda-list)
                      (,name collection ,@(args (cdr def.lambda-list))))))
       (defun ,(alexandria:format-symbol package "~:@(~a.scan~)" prefix) ,(cdr *scan-lambda-list*)
         (scan-mongo collection ,@(args *scan-lambda-list*))))))
