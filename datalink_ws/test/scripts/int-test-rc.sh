#!/bin/bash

## note: you have to be in the dir with the build.xml to run this

cmd="ant -DtargetHost=rc.cadc-ccda.hia-iha.nrc-cnrc.gc.ca datalink-int-test"

echo $cmd
eval $cmd




