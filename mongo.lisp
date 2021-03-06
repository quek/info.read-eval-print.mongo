(in-package #:info.read-eval-print.mongo)

(defgeneric next (cursor &key error-p error-value))
(defgeneric next-p (cursor))

(defvar *default-connection* nil)

(defconstant +op-reply+        1    "Reply to a client request. responseTo is set")
(defconstant +op-msg+          1000 "generic msg command followed by a string")
(defconstant +op-update+       2001 "update document")
(defconstant +op-insert+       2002 "insert new document")
(defconstant +op-reserved+     2003 "formerly used for OP_GET_BY_OID")
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

(defgeneric stream-of (connection &key &allow-other-keys))

(defclass connection ()
  ((node       :initarg  :node)
   (request-id :initform 0)
   (stream)
   (open-p     :initform nil   :reader open-p)))

(defclass replica-set (connection)
  ((nodes   :initarg  :nodes)
   (primary :initform nil)
   (slaves  :initform nil)))

(defmethod stream-of ((self connection) &key)
  (slot-value self 'stream))

(defmethod stream-of ((self replica-set) &key)
  (stream-of (slot-value self 'primary)))

(defclass db ()
  ((name :initarg :name :reader name)
   (connection :initarg :connection)))

(defmethod connection ((db db))
  (cond ((and (slot-boundp db 'connection)
              (slot-value db 'connection)))
        (*default-connection*
         (unless (open-p *default-connection*)
           (establish-connection *default-connection*))
         *default-connection*)
        (t
         (connect))))

(defclass collection ()
  ((name :initarg :name :reader name)
   (db :initarg :db)))

(defclass cursor ()
  ((collection        :initarg  :collection)
   (query             :initarg  :query)
   (skip              :initarg  :skip              :initform 0)
   (limit             :initarg  :limit             :initform 0)
   (sort              :initarg  :sort              :initform nil)
   (projection        :initarg  :projection        :initform nil)
   (tailable          :initarg  :tailable          :initform nil)
   (slave-ok          :initarg  :slave-ok          :initform nil)
   (no-cursor-timeout :initarg  :no-cursor-timeout :initform nil)
   (await-data        :initarg  :await-data        :initform nil)
   (exhaust           :initarg  :exhaust           :initform nil)
   (partial           :initarg  :partial           :initform nil)
   (cursor-id         :initform 0)
   (documents         :initform nil)
   (documents-count   :initform 0)
   (query-run-p       :initform nil)))

(defmethod initialize-instance :after ((cursor cursor) &key)
  (with-slots (query) cursor
    (unless query
      (setf query (bson)))))


(defmethod connection ((cursor cursor))
  (with-slots (collection) cursor
    (connection collection)))


(defun connect (&optional (node-or-node-list "localhost:27017")
                  (make-default t))
  (aprog1
      (etypecase node-or-node-list
        (string
         (make-instance 'connection :node node-or-node-list))
        (list
         (make-instance 'replica-set :nodes node-or-node-list)))
    (when make-default
      (when *default-connection*
        (close *default-connection*))
      (setf *default-connection* it))))

(defmethod initialize-instance :after ((self connection) &key)
  (establish-connection self))

(defun split-host-port (node)
  (if node
      (aif (position #\: node)
           (values (subseq node 0 it)
                   (parse-integer node :start (1+ it)))
           (values node 27017))
      (values "localhost" 27017)))

(defmethod establish-connection :after ((connection connection))
  (with-slots (open-p) connection
    (setf open-p t)))

(defmethod establish-connection ((self connection))
  (with-slots (node stream) self
    (multiple-value-bind (host port) (split-host-port node)
      (setf stream (iolib.sockets:make-socket :remote-host host :remote-port port)))))

(defmethod establish-connection ((self replica-set))
  (with-slots (primary slaves) self
    (prog* ((nodes (slot-value self 'nodes))
            (all-nodes nodes))
     go
       (loop with new-nodes = ()
             for node in nodes
             for connection = (ignore-errors (connect node))
             if connection
               do (let ((res (command (db connection "test") "ismaster")))
                    (if (value res "ismaster")
                        (setf primary connection)
                        (push connection slaves))
                    (loop for node in (value res "hosts")
                          unless (member node all-nodes :test #'string=)
                            do (pushnew node new-nodes :test #'string=)
                               (pushnew node all-nodes :test #'string=)))
             finally (when new-nodes
                       (setf nodes new-nodes)
                       (go go))))
    (unless primary
      (error "primary is not found."))))


(defmethod send ((connection connection) op size function)
  (let ((stream (stream-of connection)))
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

(defmethod send-and-receive ((connection connection) (cursor cursor) op size function)
  (let ((expected-request-id (send connection op size function)))
    (with-slots (documents documents-count cursor-id) cursor
      (fast-io:with-fast-input (in nil (stream-of connection))
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
            (incf documents-count number-returned)))))))

(defmacro with-reconnect-around (connection &key (max-retry-count 10) (retry-sleep 3))
  (let ((retry-count (gensym "retry-count")))
    `(loop for ,retry-count from 0
           do (handler-case (progn
                              (unless (zerop ,retry-count)
                                (establish-connection ,connection))
                              (return (call-next-method)))
                (error (e)
                  (when (< ,max-retry-count ,retry-count)
                    (signal e))
                  (warn "reconnecting... ~a" e)
                  (sleep ,retry-sleep)
                  (close ,connection))))))

(defmethod send :around ((self replica-set) op size function)
  (with-reconnect-around self))

(defmethod send-and-receive :around ((self replica-set) (cursor cursor) op size function)
  (with-reconnect-around self))


(defmethod next-request-id ((connection connection))
  (incf (slot-value connection 'request-id)))

(defmethod close :after ((connection connection) &key abort)
  (declare (ignore abort))
  (with-slots (open-p) connection
    (setf open-p nil)))

(defmethod close ((connection connection) &key abort)
  (with-slots (stream) connection
    (close stream :abort abort)))

(defmethod close ((self replica-set) &key abort)
  (with-slots (primary slaves) self
    (dolist (slave slaves)
      (and slave (close slave :abort abort)))
    (and primary (close primary :abort abort))
    (setf slaves nil
          primary nil)))

(defmacro with-connection ((var &optional (node-or-node-list "localhost:27017")
                                  (make-default t)) &body body)
  "If node-or-node-list is list, connect to replica set."
  `(let ((,var (connect ,node-or-node-list, make-default)))
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

(defmethod make-collection ((db db) name &key capped size)
  (aprog1 (make-instance 'collection :name name :db db)
    (let ((command (bson :create name)))
      (when capped
        (setf (value command :capped) t))
      (when size
        (setf (value command :size) size))
      (command db command :error-p t))))

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

(api-defun insert (collection document)
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

(api-defun update (collection selector update
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

(api-defun delete (collection selector &key single-remove)
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

(defmethod dead-p ((cursor cursor))
  (with-slots (cursor-id) cursor
    (zerop cursor-id)))

(defmethod alive-p ((cursor cursor))
  (not (dead-p cursor)))

(defmethod next-p ((cursor cursor))
  (with-slots (documents tailable) cursor
    (unless documents
      (refresh cursor))
    (if documents
        t
        (progn
          (unless tailable
            (close cursor))
          nil))))

(defmethod next ((cursor cursor) &key error-p error-value)
  (with-slots (documents) cursor
    (if documents
        (pop documents)
        (progn
          (refresh cursor)
          (if documents
              (pop documents)
              (if error-p
                  (error "No more documents.")
                  error-value))))))

(defmethod refresh ((cursor cursor))
  (with-slots (query-run-p cursor-id documents-count limit) cursor
    (if query-run-p
        (if (and (not (zerop cursor-id))
                 (or (zerop limit)
                     (< documents-count limit)))
            (send-get-more cursor))
        (send-inital-query cursor))))

(defmethod send-inital-query ((cursor cursor))
  (with-slots (tailable slave-ok no-cursor-timeout await-data exhaust partial
               collection skip limit projection
               cache query-run-p) cursor
    (let ((flag (logior (if tailable #b10 0)
                        (if slave-ok #b100 0)
                        (if no-cursor-timeout #b10000 0)
                        (if await-data #b100000 0)
                        (if exhaust #b1000000 0)
                        (if partial #b10000000 0)))
          (full-collection-name (full-collection-name collection))
          (query-data (make-query-data cursor))
          (projection-data (make-projection-bson projection)))
      (send-and-receive (connection cursor)
                        cursor
                        +op-query+
                        (+ 4 (length full-collection-name) 4 4
                           (length query-data)
                           (length projection-data))
                        (lambda (out)
                          (fast-io:write32-le flag out)
                          (fast-io:fast-write-sequence full-collection-name out)
                          (fast-io:write32-le skip out)
                          (fast-io:write32-le limit out)
                          (fast-io:fast-write-sequence query-data out)
                          (when projection-data
                            (fast-io:fast-write-sequence projection-data out)))))
    (setf query-run-p t)))

(defmethod send-get-more ((cursor cursor))
  (with-slots (documents-count cursor-id limit collection) cursor
    (let ((full-collection-name (full-collection-name collection))
          (number-to-return (if (zerop limit) 0 (- limit documents-count))))
      (send-and-receive (connection cursor)
                        cursor
                        +op-get-more+
                        (+ 4 (length full-collection-name) 4 8)
                        (lambda (out)
                          (fast-io:write32-le 0 out)
                          (fast-io:fast-write-sequence full-collection-name out)
                          (fast-io:write32-le number-to-return out)
                          (fast-io:write64-le cursor-id out))))))


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

(defun make-projection-bson (projection)
  (if projection
      (encode (apply #'bson (mapcan (lambda (field)
                                      (if (atom field)
                                          (list field 1)
                                          (list (car field) 0)))
                                    projection)))))


(api-defun find (collection
                 query
                 &key (skip 0) (limit 0) sort projection
                 tailable
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
                 :projection projection
                 :tailable tailable
                 :slave-ok slave-ok
                 :no-cursor-timeout no-cursor-timeout
                 :await-data await-data
                 :exhaust exhaust
                 :partial partial))

(api-defun find-one (collection query &optional projection)
  (let ((cursor (find collection query :projection projection :limit -1)))
    (if (next-p cursor)
        (next cursor))))

(api-defun find-all (collection
                     query
                     &key (skip 0) (limit 0) sort projection
                     slave-ok)
  (loop with cursor = (find collection query :skip skip :limit limit :sort sort
                                             :projection projection :slave-ok slave-ok)
        while (next-p cursor)
        collect (next cursor)))

(defmethod close ((cursor cursor) &key abort)
  (declare (ignore abort))
  (with-slots (cursor-id collectio) cursor
    (unless (zerop cursor-id)
      (send (connection cursor)
            +op-kill-cursors+
            (+ 4 4 8)
            (lambda (out)
              (fast-io:write32-le 0 out)
              (fast-io:write32-le 1 out)
              (fast-io:write64-le cursor-id out)))
      (setf cursor-id 0))))



(defmethod stats ((db db))
  (command db (bson :dbstats 1)))

(defmethod command ((db db) command &key)
  (command db (bson command 1)))

(defmethod command ((db db) (selector bson) &key error-p)
  (let ((doc (find-one (collection db "$cmd") selector)))
    (if error-p
        (if (= 1.0 (value doc "ok"))
            doc
            (error 'operation-failure :doc doc))
        doc)))

(defmethod command ((collection collection) selector &rest args &key)
  (apply #'command (db collection) selector args))

(api-defun count (collection query &key skip limit)
  (value (command collection (apply #'bson :count (name collection)
                                    (append (if query
                                                `(:query ,query))
                                            (if skip
                                                `(:skip ,skip))
                                            (if limit
                                                `(:limit ,limit)))))
         :n))


(defmethod map-reduce ((collection collection) (map javascript-code) (reduce javascript-code)
                       &key (out (bson :inline 1)) query sort limit finalize scope js-mode verbose)
  (let ((bson (bson "mapreduce" (name collection)
                    "map" map
                    "reduce" reduce)))
    (when out
      (setf (value bson "out") out))
    (when query
      (setf (value bson "query") query))
    (when sort
      (setf (value bson "sort") (sort-bson sort)))
    (when limit
      (setf (value bson "limit") limit))
    (when finalize
      (setf (value bson "finalize") finalize))
    (when scope
      (setf (value bson "scope") scope))
    (when js-mode
      (setf (value bson "js-mode") js-mode))
    (when verbose
      (setf (value bson "verbose") verbose))
    (command (db collection) bson)))

(defun js (js)
  (make-instance 'javascript-code
                 :code (string-trim '(#\( #\) #\;) (parenscript:ps* js))))

(defmethod map-reduce (collection (map list) reduce
                       &rest args &key &allow-other-keys)
  (apply #'map-reduce collection (js map) reduce args))

(defmethod map-reduce (collection map (reduce list)
                       &rest args &key &allow-other-keys)
  (apply #'map-reduce collection map (js reduce) args))


(api-defun find-and-modify (collection query &key update remove new upsert sort fields)
  (let ((bson (bson "findandmodify" (name collection)
                    "query" query)))
    (when update
      (setf (value bson "update") update))
    (when remove
      (setf (value bson "remove") t))
    (when new
      (setf (value bson "new") t))
    (when upsert
      (setf (value bson "upsert") t))
    (when sort
      (setf (value bson "sort") (sort-bson sort)))
    (when fields
      (setf (value bson "fields") fields))
    (value (command (db collection) bson :error-p t) "value")))
