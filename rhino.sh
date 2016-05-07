#!/bin/bash

sbt stage
rhino/target/universal/stage/bin/rhino -v
