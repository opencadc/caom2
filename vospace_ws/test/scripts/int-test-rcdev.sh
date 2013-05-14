#!/bin/bash

## note: you have to be in the dir with the build.xml to run this

cmd="ant -DtargetHost=rcdev.cadc-ccda.hia-iha.nrc-cnrc.gc.ca remote-int-test"

echo $cmd
eval $cmd




