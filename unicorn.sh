#!/bin/bash

sbt stage
shell/target/universal/stage/bin/unicorn -v
