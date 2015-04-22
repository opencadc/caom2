#!/bin/bash

## note: you have to be in the dir with the build.xml to run this

cmd="ant -Dprod=true remote-int-test"

echo $cmd
eval $cmd

