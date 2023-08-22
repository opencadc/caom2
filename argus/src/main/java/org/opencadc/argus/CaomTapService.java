/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2023.                            (c) 2023.
 *  Government of Canada                 Gouvernement du Canada
 *  National Research Council            Conseil national de recherches
 *  Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
 *  All rights reserved                  Tous droits réservés
 *
 *  NRC disclaims any warranties,        Le CNRC dénie toute garantie
 *  expressed, implied, or               énoncée, implicite ou légale,
 *  statutory, of any kind with          de quelque nature que ce
 *  respect to the software,             soit, concernant le logiciel,
 *  including without limitation         y compris sans restriction
 *  any warranty of merchantability      toute garantie de valeur
 *  or fitness for a particular          marchande ou de pertinence
 *  purpose. NRC shall not be            pour un usage particulier.
 *  liable in any event for any          Le CNRC ne pourra en aucun cas
 *  damages, whether direct or           être tenu responsable de tout
 *  indirect, special or general,        dommage, direct ou indirect,
 *  consequential or incidental,         particulier ou général,
 *  arising from the use of the          accessoire ou fortuit, résultant
 *  software.  Neither the name          de l'utilisation du logiciel. Ni
 *  of the National Research             le nom du Conseil National de
 *  Council of Canada nor the            Recherches du Canada ni les noms
 *  names of its contributors may        de ses  participants ne peuvent
 *  be used to endorse or promote        être utilisés pour approuver ou
 *  products derived from this           promouvoir les produits dérivés
 *  software without specific prior      de ce logiciel sans autorisation
 *  written permission.                  préalable et particulière
 *                                       par écrit.
 *
 *  This file is part of the             Ce fichier fait partie du projet
 *  OpenCADC project.                    OpenCADC.
 *
 *  OpenCADC is free software:           OpenCADC est un logiciel libre ;
 *  you can redistribute it and/or       vous pouvez le redistribuer ou le
 *  modify it under the terms of         modifier suivant les termes de
 *  the GNU Affero General Public        la “GNU Affero General Public
 *  License as published by the          License” telle que publiée
 *  Free Software Foundation,            par la Free Software Foundation
 *  either version 3 of the              : soit la version 3 de cette
 *  License, or (at your option)         licence, soit (à votre gré)
 *  any later version.                   toute version ultérieure.
 *
 *  OpenCADC is distributed in the       OpenCADC est distribué
 *  hope that it will be useful,         dans l’espoir qu’il vous
 *  but WITHOUT ANY WARRANTY;            sera utile, mais SANS AUCUNE
 *  without even the implied             GARANTIE : sans même la garantie
 *  warranty of MERCHANTABILITY          implicite de COMMERCIALISABILITÉ
 *  or FITNESS FOR A PARTICULAR          ni d’ADÉQUATION À UN OBJECTIF
 *  PURPOSE.  See the GNU Affero         PARTICULIER. Consultez la Licence
 *  General Public License for           Générale Publique GNU Affero
 *  more details.                        pour plus de détails.
 *
 *  You should have received             Vous devriez avoir reçu une
 *  a copy of the GNU Affero             copie de la Licence Générale
 *  General Public License along         Publique GNU Affero avec
 *  with OpenCADC.  If not, see          OpenCADC ; si ce n’est
 *  <http://www.gnu.org/licenses/>.      pas le cas, consultez :
 *                                       <http://www.gnu.org/licenses/>.
 *
 *  $Revision: 4 $
 *
 ************************************************************************
 */

package org.opencadc.argus;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.LocalAuthority;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.uws.server.RandomStringGenerator;
import ca.nrc.cadc.vosi.Availability;
import ca.nrc.cadc.vosi.AvailabilityPlugin;
import ca.nrc.cadc.vosi.avail.CheckDataSource;
import ca.nrc.cadc.vosi.avail.CheckException;
import ca.nrc.cadc.vosi.avail.CheckResource;
import ca.nrc.cadc.vosi.avail.CheckWebService;
import java.net.URI;
import java.net.URL;
import java.util.NoSuchElementException;
import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
public class CaomTapService implements AvailabilityPlugin {

    private static final Logger log = Logger.getLogger(CaomTapService.class);

    private static final String TAP_SCHEMA_TEST = "select schema_name from tap_schema.schemas11 where schema_name='caom2'";
    private static final String CAOM2_TEST = "select collection,observationID from caom2.Observation limit 1";
    private static final String UWSDS_TEST = "select jobID from uws.Job limit 1";
    private static final String UWSDS_WRITE_SQL = "insert into uws.JobAvailability (value) values ('T')";

    public static final String TAPUSER_POOL_NAME = "jdbc/tapuser";
    public static final String UWS_POOL_NAME = "jdbc/uws";

    public CaomTapService() {
    }

    @Override
    public void setAppName(String string) {
        //no-op
    }

    @Override
    public boolean heartbeat() {
        return true;
    }

    public Availability getStatus() {
        boolean isGood = true;
        String note = "service is accepting queries";
        try {
            CheckResource cr;

            cr = new CheckDataSource(UWS_POOL_NAME, UWSDS_TEST);
            cr.check();

            cr = new CheckDataSource(UWS_POOL_NAME, UWSDS_WRITE_SQL, true, true);
            cr.check();

            cr = new CheckDataSource(TAPUSER_POOL_NAME, TAP_SCHEMA_TEST);
            cr.check();

            cr = new CheckDataSource(TAPUSER_POOL_NAME, CAOM2_TEST);
            cr.check();

            // tap_upload test: create and drop a table
            String[] uploadTest = getTapUploadTest();
            // create table
            cr = new CheckDataSource(TAPUSER_POOL_NAME, uploadTest[0], false);
            cr.check();
            // drop table
            cr = new CheckDataSource(TAPUSER_POOL_NAME, uploadTest[1], false);
            cr.check();

            // check other services we depend on
            RegistryClient reg = new RegistryClient();
            URL url;
            CheckResource checkResource;

            LocalAuthority localAuthority = new LocalAuthority();

            String stdID = Standards.CRED_PROXY_10.toString();
            try {

                URI credURI = localAuthority.getServiceURI(stdID);
                url = reg.getServiceURL(credURI, Standards.VOSI_AVAILABILITY, AuthMethod.ANON);
                if (url != null) {
                    checkResource = new CheckWebService(url);
                    checkResource.check();
                } else {
                    log.debug("check skipped: " + credURI + " does not provide " + Standards.VOSI_AVAILABILITY);
                }
            } catch (NoSuchElementException ex) {
                log.debug("not found: " + stdID);
            }

            stdID = Standards.GMS_SEARCH_10.toString();
            URI groupsURI = null;
            try {
                groupsURI = localAuthority.getServiceURI(stdID);
                url = reg.getServiceURL(groupsURI, Standards.VOSI_AVAILABILITY, AuthMethod.ANON);
                if (url != null) {
                    checkResource = new CheckWebService(url);
                    checkResource.check();
                } else {
                    log.debug("check skipped: " + groupsURI + " does not provide " + Standards.VOSI_AVAILABILITY);
                }
            } catch (NoSuchElementException ex) {
                log.debug("not found: " + stdID);
            }

            stdID = Standards.UMS_USERS_01.toString();
            try {
                URI usersURI = localAuthority.getServiceURI(stdID);
                if (!usersURI.equals(groupsURI)) {
                    url = reg.getServiceURL(usersURI, Standards.VOSI_AVAILABILITY, AuthMethod.ANON);
                    if (url != null) {
                        checkResource = new CheckWebService(url);
                        checkResource.check();
                    } else {
                        log.debug("check skipped: " + usersURI + " does not provide " + Standards.VOSI_AVAILABILITY);
                    }
                } else {
                    log.debug("already checked: " + stdID + " = " + usersURI);
                }
            } catch (NoSuchElementException ex) {
                log.debug("not found: " + stdID);
            }

            // TODO: check that cadc-tap-tmp is working
        } catch (CheckException ce) {
            // tests determined that the resource is not working
            isGood = false;
            note = ce.getMessage();
        } catch (Throwable t) {
            // the test itself failed
            log.error("test failed", t);
            isGood = false;
            note = "test failed, reason: " + t;
        }
        return new Availability(isGood, note);
    }

    private String[] getTapUploadTest() {
        String id = new RandomStringGenerator(16).getID();
        String name = "tap_upload.avail_" + id;
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(name).append(" (");
        sb.append("c char(1), i integer, d double precision");
        sb.append(")");
        String drop = getTapUploadCleanup(name);
        return new String[]{sb.toString(), drop};
    }

    private String getTapUploadCleanup(String name) {
        StringBuilder sb = new StringBuilder();
        sb.append("DROP TABLE ").append(name);
        return sb.toString();
    }

    public void setState(String string) {
        //no-op
    }

}
