MongoDb driver.

```
(with-connection (connection "localhost:27017")
  (let* ((db (db connection "test"))
         (collection (collection db "foo")))
    (insert collection (bson :name "hoge" :bar "baha"))
    (find-one collection (bson :name "hoge"))))
;;⇒ {"_id": ObjectId("514DC175781C2C99885751FF"), "name": "hoge", "bar": "baha"}
```
