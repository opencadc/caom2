#!/bin/bash
#
#***********************************************************************
#******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
#*************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2016.                            (c) 2016.
#  Government of Canada                 Gouvernement du Canada
#  National Research Council            Conseil national de recherches
#  Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
#  All rights reserved                  Tous droits réservés
#                                       
#  NRC disclaims any warranties,        Le CNRC dénie toute garantie
#  expressed, implied, or               énoncée, implicite ou légale,
#  statutory, of any kind with          de quelque nature que ce
#  respect to the software,             soit, concernant le logiciel,
#  including without limitation         y compris sans restriction
#  any warranty of merchantability      toute garantie de valeur
#  or fitness for a particular          marchande ou de pertinence
#  purpose. NRC shall not be            pour un usage particulier.
#  liable in any event for any          Le CNRC ne pourra en aucun cas
#  damages, whether direct or           être tenu responsable de tout
#  indirect, special or general,        dommage, direct ou indirect,
#  consequential or incidental,         particulier ou général,
#  arising from the use of the          accessoire ou fortuit, résultant
#  software.  Neither the name          de l'utilisation du logiciel. Ni
#  of the National Research             le nom du Conseil National de
#  Council of Canada nor the            Recherches du Canada ni les noms
#  names of its contributors may        de ses  participants ne peuvent
#  be used to endorse or promote        être utilisés pour approuver ou
#  products derived from this           promouvoir les produits dérivés
#  software without specific prior      de ce logiciel sans autorisation
#  written permission.                  préalable et particulière
#                                       par écrit.
#                                       
#  This file is part of the             Ce fichier fait partie du projet
#  OpenCADC project.                    OpenCADC.
#                                       
#  OpenCADC is free software:           OpenCADC est un logiciel libre ;
#  you can redistribute it and/or       vous pouvez le redistribuer ou le
#  modify it under the terms of         modifier suivant les termes de
#  the GNU Affero General Public        la “GNU Affero General Public
#  License as published by the          License” telle que publiée
#  Free Software Foundation,            par la Free Software Foundation
#  either version 3 of the              : soit la version 3 de cette
#  License, or (at your option)         licence, soit (à votre gré)
#  any later version.                   toute version ultérieure.
#                                       
#  OpenCADC is distributed in the       OpenCADC est distribué
#  hope that it will be useful,         dans l’espoir qu’il vous
#  but WITHOUT ANY WARRANTY;            sera utile, mais SANS AUCUNE
#  without even the implied             GARANTIE : sans même la garantie
#  warranty of MERCHANTABILITY          implicite de COMMERCIALISABILITÉ
#  or FITNESS FOR A PARTICULAR          ni d’ADÉQUATION À UN OBJECTIF
#  PURPOSE.  See the GNU Affero         PARTICULIER. Consultez la Licence
#  General Public License for           Générale Publique GNU Affero
#  more details.                        pour plus de détails.
#                                       
#  You should have received             Vous devriez avoir reçu une
#  a copy of the GNU Affero             copie de la Licence Générale
#  General Public License along         Publique GNU Affero avec
#  with OpenCADC.  If not, see          OpenCADC ; si ce n’est
#  <http://www.gnu.org/licenses/>.      pas le cas, consultez :
#                                       <http://www.gnu.org/licenses/>.
#
#  $Revision: 5 $
#
#***********************************************************************
#
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

    SQLDIR=src/main/resources/sybase
    TMPSQL=build/tmp/sybase
    \rm -rf $TMPSQL
    mkdir -p $TMPSQL
    \cp -f $SQLDIR/*.sql $TMPSQL

echo "drop all"
    $RUNCMD -i $TMPSQL/caom2.drop_all.sql
echo "create caom2 tables"
    $RUNCMD -i $TMPSQL/caom2.Observation.sql
    $RUNCMD -i $TMPSQL/caom2.Plane.sql
    $RUNCMD -i $TMPSQL/caom2.Artifact.sql
    $RUNCMD -i $TMPSQL/caom2.Part.sql
    $RUNCMD -i $TMPSQL/caom2.Chunk.sql
echo "create read access tables"
    $RUNCMD -i $TMPSQL/caom2.access.sql
echo "create deleted tables"
    $RUNCMD -i $TMPSQL/caom2.deleted.sql
echo "create harvest tables"
    $RUNCMD -i $TMPSQL/caom2.HarvestState.sql
    $RUNCMD -i $TMPSQL/caom2.HarvestSkip.sql

## must be dbo to run textptr_fix
#    $RUNCMD -i $TMPSQL/caom2.textptr_fix.sql
## no grants needed for library tests
#    $RUNCMD -i $TMPSQL/caom2.permissions.sql
}

doitPG()
{
    RUNCMD=$1
    DBUSER=$2

    SQLDIR=src/main/resources/postgresql
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
    
echo "drop all"
    $RUNCMD < $TMPSQL/caom2.drop_all.sql
echo "create caom2 tables"
    $RUNCMD < $TMPSQL/caom2.Observation.sql
    $RUNCMD < $TMPSQL/caom2.Plane.sql
    $RUNCMD < $TMPSQL/caom2.Artifact.sql
    $RUNCMD < $TMPSQL/caom2.Part.sql
    $RUNCMD < $TMPSQL/caom2.Chunk.sql
echo "create read access tables"
    $RUNCMD < $TMPSQL/caom2.access.sql
echo "create deleted tables"
    $RUNCMD < $TMPSQL/caom2.deleted.sql
echo "create harvest tables"
    $RUNCMD < $TMPSQL/caom2.HarvestState.sql
    $RUNCMD < $TMPSQL/caom2.HarvestSkip.sql
echo "create extra indices"
    $RUNCMD < $TMPSQL/caom2.extra_indices.sql
#    $RUNCMD < $TMPSQL/caom2.permissions.sql
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

doitPG "psql --quiet -h cvodbdev -d cadctest -U $DBUSER -w" $DBUSER
