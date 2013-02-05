(in-package #:info.read-eval-print.mongo)

(defgeneric next (cursor))
(defgeneric next-p (cursor))

(defconstant +op-reply+        1    "Reply to a client request. responseTo is set")
(defconstant +op-msg+          1000 "generic msg command followed by a string")
(defconstant +op-update+       2001 "update document")
(defconstant +op-insert+       2002 "insert new document")
;; (defconstant RESERVED        2003 "formerly used for OP_GET_BY_OID")
(defconstant +op-query+        2004 "query a collection")
(defconstant +op-get-more+     2005 "Get more data from a query. See Cursors")
(defconstant +op-delete+       2006 "Delete documents")
(defconstant +op-kill-cursors+ 2007 "Tell database client is done with a cursor")

(defconstant +msg-header-size+ (* 4 4))

(defstruct msg-header
  message-length                        ;int32
  request-id                            ;int32
  response-to                           ;int32
  op-code)                              ;int32

(defclass connection ()
  ((host :initarg :host)
   (port :initarg :port)
   (request-id :initform 0)
   (stream)))

(defclass db ()
  ((name :initarg :name)
   (connection :initarg :connection :reader connection)))

(defclass collection ()
  ((name :initarg :name)
   (db :initarg :db)))

(defclass cursor ()
  ((collection            :initarg  :collection)
   (query                 :initarg  :query)
   (skip                  :initarg  :skip                  :initform 0)
   (limit                 :initarg  :limit                 :initform 0)
   (sort                  :initarg  :sort                  :initform nil)
   (return-field-selector :initarg  :return-field-selector :initform nil)
   (tailable-cursor       :initarg  :tailable-cursor       :initform nil)
   (slave-ok              :initarg  :slave-ok              :initform nil)
   (no-cursor-timeout     :initarg  :no-cursor-timeout     :initform nil)
   (await-data            :initarg  :await-data            :initform nil)
   (exhaust               :initarg  :exhaust               :initform nil)
   (partial               :initarg  :partial               :initform nil)
   (cursor-id             :initform 0)
   (documents             :initform nil)
   (documents-count       :initform 0)
   (query-run-p           :initform nil)))

(defmethod connection ((cursor cursor))
  (with-slots (collection) cursor
    (connection collection)))


(defun connect (&key (host "localhost") (port 27017))
  (make-instance 'connection :host host :port port))

(defmethod initialize-instance :after ((connection connection) &key)
  (with-slots (host port stream) connection
    (setf stream (iolib.sockets:make-socket :remote-host host :remote-port port))))

(defmethod send ((connection connection) op size function)
  (with-slots (stream) connection
    (let ((request-id (next-request-id connection)))
      (write-sequence (fast-io:with-fast-output (out)
                        (fast-io:write32-le (+ +msg-header-size+ size) out)
                        (fast-io:write32-le request-id out)
                        (fast-io:write32-le 0 out)
                        (fast-io:write32-le op out)
                        (funcall function out))
                      stream)
      (force-output stream)
      request-id)))

(defmethod next-request-id ((connection connection))
  (incf (slot-value connection 'request-id)))

(defmethod close ((connection connection) &key abort)
  (with-slots (stream) connection
    (close stream :abort abort)))

(defmacro with-connection ((var &key (host "localhost") (port 27017)) &body body)
  `(let ((,var (connect :host ,host :port ,port)))
     (unwind-protect (progn ,@body)
       (close ,var))))


(defgeneric db (thing &optional name))

(defmethod db ((connection connection) &optional name)
  (make-instance 'db :name name :connection connection))

(defmethod db ((collection collection) &optional name)
  (declare (ignore name))
  (slot-value collection 'db))

(defmethod collection ((db db) name)
  (make-instance 'collection :name name :db db))

(defmethod connection ((collection collection))
  (connection (slot-value collection 'db)))

(defun write-utf-8 (string out &key null-terminate-p)
  (fast-io:fast-write-sequence
           (babel:string-to-octets string :encoding :utf-8)
           out)
  (when null-terminate-p
    (fast-io:fast-write-byte 0 out)))

(defmethod full-collection-name (collection)
  (fast-io:with-fast-output (out)
    (write-utf-8 (slot-value (slot-value collection 'db) 'name) out)
    (fast-io:fast-write-byte #.(char-code #\.) out)
    (write-utf-8 (slot-value collection 'name) out :null-terminate-p t)))

(defmethod insert ((collection collection) (document bson))
  (let ((flag 0)
        (full-collection-name (full-collection-name collection))
        (data (encode document)))
    (send (connection collection)
          +op-insert+
          (+ 4 (length full-collection-name) (length data))
          (lambda (out)
            (fast-io:write32-le flag out)
            (fast-io:fast-write-sequence full-collection-name out)
            (fast-io:fast-write-sequence data out)))
    (values)))

(defmethod update ((collection collection) (selector bson) (update bson)
                   &key upsert multi-update)
  (let ((flags (logior (if upsert #b1 0)
                       (if multi-update #b10 0)))
        (full-collection-name (full-collection-name collection))
        (selector (encode selector))
        (update (encode update)))
    (send (connection collection)
          +op-update+
          (+ 4 (length full-collection-name) 4 (length selector) (length update))
          (lambda (out)
            (fast-io:write32-le 0 out)
            (fast-io:fast-write-sequence full-collection-name out)
            (fast-io:write32-le flags out)
            (fast-io:fast-write-sequence selector out)
            (fast-io:fast-write-sequence update out)))
    (values)))

(defmethod delete ((collection collection) &optional selector &key single-remove)
  (declare #+sbcl(sb-ext:muffle-conditions sb-kernel:redefinition-warning))
  (let ((flag (logior (if single-remove #b1 0)))
        (full-collection-name (full-collection-name collection))
        (selector (encode (if selector selector (bson)))))
    (send (connection collection)
          +op-delete+
          (+ 4 (length full-collection-name) 4 (length selector))
          (lambda (out)
            (fast-io:write32-le 0 out)
            (fast-io:fast-write-sequence full-collection-name out)
            (fast-io:write32-le flag out)
            (fast-io:fast-write-sequence selector out)))
    (values)))

(defmethod next ((cursor cursor))
  (with-slots (documents) cursor
    (if documents
        (pop documents)
        (progn
          (refresh cursor)
          (if documents
              (pop documents)
              (error "No more documents."))))))

(defmethod next-p ((cursor cursor))
  (with-slots (documents) cursor
    (unless documents
      (refresh cursor))
    (if documents
        t
        (progn
          (close cursor)
          nil))))

(defmethod refresh ((cursor cursor))
  (with-slots (query-run-p cursor-id documents-count limit) cursor
    (if query-run-p
        (if (and (not (zerop cursor-id))
                 (or (zerop limit)
                     (< documents-count limit)))
            (send-get-more cursor))
        (send-inital-query cursor))))

(defmethod send-inital-query ((cursor cursor))
  (with-slots (tailable-cursor slave-ok no-cursor-timeout await-data exhaust partial
               collection skip limit
               cache query-run-p) cursor
    (let ((flag (logior (if tailable-cursor #b10 0)
                        (if slave-ok #b100 0)
                        (if no-cursor-timeout #b10000 0)
                        (if await-data #b100000 0)
                        (if exhaust #b1000000 0)
                        (if partial #b10000000 0)))
          (full-collection-name (full-collection-name collection))
          (query-data (make-query-data cursor))
          (return-field-selector-data (make-return-field-selector-data cursor)))
      (let ((request-id
              (send (connection cursor)
                    +op-query+
                    (+ 4 (length full-collection-name) 4 4
                       (length query-data)
                       (length return-field-selector-data))
                    (lambda (out)
                      (fast-io:write32-le flag out)
                      (fast-io:fast-write-sequence full-collection-name out)
                      (fast-io:write32-le skip out)
                      (fast-io:write32-le limit out)
                      (fast-io:fast-write-sequence query-data out)
                      (when return-field-selector-data
                       (fast-io:fast-write-sequence return-field-selector-data out))))))
        (receive cursor request-id)))
    (setf query-run-p t)))

(defmethod send-get-more ((cursor cursor))
  (with-slots (documents-count cursor-id limit collection) cursor
    (let ((full-collection-name (full-collection-name collection))
          (number-to-return (if (zerop limit) 0 (- limit documents-count))))
      (let ((request-id (send (connection cursor)
                              +op-get-more+
                              (+ 4 (length full-collection-name) 4 8)
                              (lambda (out)
                                (fast-io:write32-le 0 out)
                                (fast-io:fast-write-sequence full-collection-name out)
                                (fast-io:write32-le number-to-return out)
                                (fast-io:write64-le cursor-id out)))))
        (receive cursor request-id)))))

(defmethod receive ((cursor cursor) expected-request-id)
  (with-slots (documents documents-count cursor-id) cursor
    (fast-io:with-fast-input (in nil (slot-value (connection cursor) 'stream))
      (let ((message-length (fast-io:read32-le in))
            (request-id (fast-io:read32-le in))
            (response-to (fast-io:read32-le in))
            (op-code (fast-io:read32-le in)))
        (declare (ignore message-length request-id))
        (unless (= op-code +op-reply+)
          (error "Unexpected OP_CODE ~a" op-code))
        (unless (= response-to expected-request-id)
          (error "Unexpected ResponseTo ~a" response-to))
        (let (number-returned)
          (fast-io:read32-le in)        ;response flags
          (setf cursor-id (fast-io:read64-le in))
          (fast-io:read32-le in)        ;starting-from
          (setf number-returned (fast-io:read32-le in)) ;number returned
          (setf documents (loop repeat number-returned
                                collect (decode in)))
          (incf documents-count number-returned))))))

(defmethod make-query-data ((cursor cursor))
  (with-slots (query sort) cursor
    (encode
     (if (and query (loop for key in '("$query" "$explain" "$hint" "$snapshot"
                                       "$showDiskLoc" "$maxScan" "$returnKey" "$comment")
                          thereis (value query key)))
         query
         (let ((bson (bson "$query" query)))
           (when sort
             (setf (value bson "$orderby") (sort-bson sort)))
           bson)))))

(defun sort-bson (sort)
  "sort is x or (x y) or (x :desc y z :desc)"
  (let ((bson (bson)))
    (labels ((f (arg)
               (cond ((null arg)
                      bson)
                     ((atom arg)
                      (setf (value bson arg) 1)
                      bson)
                     ((eq :desc (cadr arg))
                      (setf (value bson (car arg)) -1)
                      (f (cddr arg)))
                     (t
                      (setf (value bson (car arg)) 1)
                      (f (cdr arg))))))
      (f sort))))

(defmethod make-return-field-selector-data ((cursor cursor))
  (with-slots (return-field-selector) cursor
    (if return-field-selector
        (encode (apply #'bson (mapcan (lambda (field) (list field 1)) return-field-selector))))))

(defmethod find ((collection collection) &optional (query (bson))
                 &key (skip 0) (limit 0) sort return-field-selector
                   tailable-cursor
                   slave-ok
                   no-cursor-timeout
                   await-data
                   exhaust
                   partial)
  (or query (setf query (bson)))
  (make-instance 'cursor
                 :collection collection
                 :query query
                 :skip skip
                 :limit limit
                 :sort sort
                 :return-field-selector return-field-selector
                 :tailable-cursor tailable-cursor
                 :slave-ok slave-ok
                 :no-cursor-timeout no-cursor-timeout
                 :await-data await-data
                 :exhaust exhaust
                 :partial partial))

(defmethod find-one ((collection collection) &optional (query (bson)))
  (let ((cursor (make-instance 'cursor
                               :collection collection
                               :query query
                               :limit -1)))
    (if (next-p cursor)
        (next cursor))))

(defmethod close ((cursor cursor) &key abort)
  (declare (ignore abort))
  (with-slots (cursor-id collectio) cursor
    (unless (zerop cursor-id)
      (send (connection cursor)
            +op-kill-cursors+
            (+ 4 4 4)
            (lambda (out)
              (fast-io:write32-le 0 out)
              (fast-io:write32-le 1 out)
              (fast-io:write64-le cursor-id out)))
      (setf cursor-id 0))))



(defmethod stats ((db db))
  (command db (bson :dbstats 1)))

(defmethod command ((db db) selector &key)
  (find-one (collection db "$cmd") selector))

#+nil
(with-connection (connection)
  (stats (db connection "test")))
;;⇒ {"db": "test", "collections": 4, "objects": 10, "avgObjSize": 38.8d0, "dataSize": 388, "storageSize": 11268096, "numExtents": 10, "indexes": 2, "indexSize": 16352, "fileSize": 201326592, "nsSizeMB": 16, "ok": 1.0d0}

