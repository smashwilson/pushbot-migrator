#!/bin/bash

exec docker run -it \
  --link redis:redis \
  --rm \
  redis:3.2.8 redis-cli -h redis
