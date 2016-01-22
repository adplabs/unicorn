#!/bin/bash

echo -e "PUT"
curl -X PUT -H "Content-Type: application/json" -d '{"_id":"dude","username":"xyz","password":"xyz"}' http://localhost:8080/unicorn_rhino_test

echo -e "\nGET"
curl -X GET http://localhost:8080/unicorn_rhino_test/dude

echo -e "\nPOST"
curl -X POST -H "Content-Type: application/json" -d '{"_id":"dude","username":"dude","password":"xyz"}' http://localhost:8080/unicorn_rhino_test

echo -e "\GET"
curl -X GET http://localhost:8080/unicorn_rhino_test/dude

echo -e "\nPATCH"
curl -X PATCH -H "Content-Type: application/json" -d '{"_id":"dude","$set":{"password":"abc"}}' http://localhost:8080/unicorn_rhino_test

echo -e "\nGET"
curl -X GET http://localhost:8080/unicorn_rhino_test/dude

echo -e "\nDELETE"
curl -X DELETE http://localhost:8080/unicorn_rhino_test/dude

echo -e "\nGET"
curl -X GET http://localhost:8080/unicorn_rhino_test/dude

echo -e "\nPUT IBM"
curl -X PUT -H "Content-Type: application/json" --header 'tenant: "IBM"' -d '{"_id":"dude","username":"xyz","password":"xyz"}' http://localhost:8080/unicorn_rhino_test

echo -e "\nGET IBM"
curl -X GET --header 'tenant: "IBM"' http://localhost:8080/unicorn_rhino_test/dude

echo -e "\nGET MSFT"
curl -X GET --header 'tenant: "MSFT"' http://localhost:8080/unicorn_rhino_test/dude

echo -e "\nGET NONE"
curl -X GET http://localhost:8080/unicorn_rhino_test/dude

echo -e "\nDELETE"
curl -X DELETE --header 'tenant: "IBM"' http://localhost:8080/unicorn_rhino_test/dude

echo -e "\nGET"
curl -X GET --header 'tenant: "IBM"' http://localhost:8080/unicorn_rhino_test/dude

