#!/usr/bin/env bash

echo "wait on cassandra"

while ! nc -z localhost 9042; do
  sleep 5
done

echo "cassandra started"
