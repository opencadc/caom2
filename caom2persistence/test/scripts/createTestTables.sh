#!/bin/bash

##
## This script copies the source SQL DDLs from sql/<db arch> into build/tmp,
## applies modifications (if necessary) for creation in the cadctest database,
## drops all existing caom2 tables, and then creates new ones using credentials
## from $HOME/.dbrc
##
## required .dbrc setup:
##
## CAOM2_SYB_TEST cadctest <username> <password> net.sourceforge.jtds.jdbc.Driver jdbc:jtds:sybase://devsybase:4200/cadctest
## CAOM2_PG_TEST  cadctest <username> <password> org.postgresql.Driver jdbc:postgresql://cvodb0/cadctest
##
## current modifications:
##
## sybase: none (uses cadctest and default schema == username)
## postgresql: replaces standard caom2 schema name with username (from .dbrc)
##             removes tablespace declarations and uses default for cadctest db
##

doitSYB()
{
    RUNCMD=$1

    SQLDIR=sql/sybase
    TMPSQL=build/tmp/sybase
    \rm -rf $TMPSQL
    mkdir -p $TMPSQL
    \cp -f $SQLDIR/*.sql $TMPSQL

    $RUNCMD -i $TMPSQL/caom2.drop_all.sql
    $RUNCMD -i $TMPSQL/caom2.Observation.sql
    $RUNCMD -i $TMPSQL/caom2.Plane.sql
    $RUNCMD -i $TMPSQL/caom2.Artifact.sql
    $RUNCMD -i $TMPSQL/caom2.Part.sql
    $RUNCMD -i $TMPSQL/caom2.Chunk.sql
    $RUNCMD -i $TMPSQL/caom2.access.sql
    $RUNCMD -i $TMPSQL/caom2.deleted.sql
#    $RUNCMD -i $TMPSQL/caom2.permissions.sql
    $RUNCMD -i $TMPSQL/caom2.HarvestState.sql
}

doitPG()
{
    RUNCMD=$1
    DBUSER=$2

    SQLDIR=sql/postgresql
    TMPSQL=build/tmp/postgresql
    \rm -rf $TMPSQL
    mkdir -p $TMPSQL
    \cp -f $SQLDIR/*.sql $TMPSQL

    for sqlFile in $TMPSQL/*.sql; do
        echo "modifying: $sqlFile"
        sed -i 's/tablespace caom_data//g' $sqlFile
        sed -i 's/using index tablespace caom_index//g' $sqlFile
        sed -i 's/tablespace caom_index//g' $sqlFile
        sed -i 's/caom2./'"${DBUSER}".'/g' $sqlFile
    done
    

    $RUNCMD < $TMPSQL/caom2.drop_all.sql
    $RUNCMD < $TMPSQL/caom2.Observation.sql
    $RUNCMD < $TMPSQL/caom2.Plane.sql
    $RUNCMD < $TMPSQL/caom2.Artifact.sql
    $RUNCMD < $TMPSQL/caom2.Part.sql
    $RUNCMD < $TMPSQL/caom2.Chunk.sql
    $RUNCMD < $TMPSQL/caom2.access.sql
    $RUNCMD < $TMPSQL/caom2.deleted.sql
#    $RUNCMD < $TMPSQL/caom2.permissions.sql
    $RUNCMD < $TMPSQL/caom2.HarvestState.sql
    $RUNCMD < $TMPSQL/caom2.HarvestSkip.sql
}

## Sybase test setup
echo
if [ $(grep cadctest ~/.dbrc | grep -c '^CAOM2_SYB_TEST ') = 1 ]; then
    echo "found: CAOM2_SYB_TEST cadctest ... creating Sybase tables"
else
    echo "not found: CAOM2_SYB_TEST cadctest ..."
    exit 1
fi
echo
CRED=$(dbrc_get CAOM2_SYB_TEST cadctest)
DBUSER=$(echo $CRED | awk '{print $1}')
DBPW=$(echo $CRED | awk '{print $2}')

doitSYB "sqsh -S DEVSYBASE -D cadctest -U $DBUSER -P $DBPW"


## PostgreSQL test setup
echo
if [ $(grep cadctest ~/.dbrc | grep -c '^CAOM2_PG_TEST ') == 1 ]; then
    echo "found: CAOM2_PG_TEST cadctest ... creating PostgreSQL tables"
else
    echo "not found: CAOM2_PG_TEST cadctest ..."
    exit 1
fi
echo
CRED=$(dbrc_get CAOM2_PG_TEST cadctest)
DBUSER=$(echo $CRED | awk '{print $1}')
DBPW=$(echo $CRED | awk '{print $2}')

doitPG "psql -h cvodb0 -d cadctest -U $DBUSER -w" $DBUSER
