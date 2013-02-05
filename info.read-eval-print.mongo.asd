;;;; info.read-eval-print.mongo.asd

(asdf:defsystem #:info.read-eval-print.mongo
  :serial t
  :description "Describe info.read-eval-print.mongo here"
  :author "TAHARA Yoshinori <read.eval.print@gmail.com"
  :license "BSD Licence"
  :components ((:file "package")
               (:file "operator")
               (:file "mongo")
               (:file "series"))
  :depends-on (:info.read-eval-print.bson :iolib :series))

