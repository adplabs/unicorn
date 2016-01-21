#!/bin/bash

curl -X PUT -H "Content-Type: application/json" -d '{"_id":"dude","username":"xyz","password":"xyz"}' http://localhost:8080/unicorn_rhino_test

curl -X GET http://localhost:8080/unicorn_rhino_test/dude

curl -X POST -H "Content-Type: application/json" -d '{"_id":"dude","username":"dude","password":"xyz"}' http://localhost:8080/unicorn_rhino_test

curl -X GET http://localhost:8080/unicorn_rhino_test/dude

curl -X PATCH -H "Content-Type: application/json" -d '{"_id":"dude","$set":{"password":"abc"}}' http://localhost:8080/unicorn_rhino_test

curl -X GET http://localhost:8080/unicorn_rhino_test/dude

curl -X DELETE http://localhost:8080/unicorn_rhino_test/dude

curl -X GET http://localhost:8080/unicorn_rhino_test/dude

