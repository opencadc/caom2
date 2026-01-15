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
*  $Revision: 5 $
*
************************************************************************
 */

package org.opencadc.torkeep;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.LocalAuthority;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.rest.RestAction;
import ca.nrc.cadc.vosi.Availability;
import ca.nrc.cadc.vosi.AvailabilityPlugin;
import ca.nrc.cadc.vosi.avail.CheckCertificate;
import ca.nrc.cadc.vosi.avail.CheckException;
import ca.nrc.cadc.vosi.avail.CheckResource;
import ca.nrc.cadc.vosi.avail.CheckWebService;
import ca.nrc.cadc.wcs.VerifyWCS;
import java.io.File;
import java.net.URI;
import java.net.URL;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.log4j.Logger;
import org.opencadc.caom2.db.ObservationDAO;
import org.opencadc.erfa.ERFALib;

/**
 * VOSI-Availability plugin.
 * 
 * @author pdowler
 */
public class ServiceAvailabilityImpl implements AvailabilityPlugin {

    private static final Logger log = Logger.getLogger(ServiceAvailabilityImpl.class);
    private static final File AAI_PEM_FILE = new File(System.getProperty("user.home") + "/.ssl/cadcproxy.pem");

    private String appName;

    public ServiceAvailabilityImpl() {
    }

    @Override
    public void setAppName(String appName) {
        this.appName = appName;
    }

    @Override
    public boolean heartbeat() {
        return true;
    }

    public Availability getStatus() {
        boolean isGood = true;
        String note = "service is accepting queries";

        try {
            String state = getState();
            if (RestAction.STATE_OFFLINE.equals(state)) {
                return new Availability(false, RestAction.STATE_OFFLINE_MSG);
            }
            if (RestAction.STATE_READ_ONLY.equals(state)) {
                return new Availability(false, RestAction.STATE_READ_ONLY_MSG);
            }

            TorkeepConfig torkeepConfig = TorkeepInitAction.getTorkeepConfig();
            if (torkeepConfig.getConfigs().isEmpty()) {
                throw new IllegalStateException("CONFIG: no configured collections found in - "
                        + TorkeepConfig.TORKEEP_PROPERTIES);
            }

            Map<String,Object> daoConfig = TorkeepInitAction.getDAOConfig();
            ObservationDAO dao = new ObservationDAO(true);
            dao.setConfig(daoConfig); // connectivity tested

            // check the WCS JNI library
            try {
                VerifyWCS ver = new VerifyWCS();
                ver.run();
            } catch (Throwable t) {
                throw new CheckException("wcslib not available", t);
            }

            // check the ERFA JNI library
            try {
                ERFALib.tai2tt(2453750.5, 0.892482639);
            } catch (Throwable t) {
                throw new CheckException("erfalib not available", t);
            }

            // check other services we depend on
            RegistryClient reg = new RegistryClient();
            LocalAuthority localAuthority = new LocalAuthority();
            
            URI openidURI = localAuthority.getResourceID(Standards.SECURITY_METHOD_OPENID);
            if (openidURI != null) {
                // TODO: cadc-vosi CheckOpenIDProvider(URI)
            }

            URI credURI = localAuthority.getResourceID(Standards.CRED_PROXY_10);
            if (credURI != null) {
                CheckResource cws = new CheckWebService(credURI);
                cws.check();
            }

            URI usersURI = localAuthority.getResourceID(Standards.UMS_USERS_01);
            if (usersURI != null) {
                CheckResource cws = new CheckWebService(usersURI);
                cws.check();
            }

            URI groupsURI = localAuthority.getResourceID(Standards.GMS_SEARCH_10);
            if (groupsURI != null && !groupsURI.equals(usersURI)) {
                CheckResource cws = new CheckWebService(groupsURI);
                cws.check();
            }

            if (credURI != null || usersURI != null) {
                if (AAI_PEM_FILE.exists() && AAI_PEM_FILE.canRead()) {
                    // check for a certificate needed to perform network A&A ops
                    CheckCertificate checkCert = new CheckCertificate(AAI_PEM_FILE);
                    checkCert.check();
                } else {
                    log.debug("AAI cert not found or unreadable");
                }
            }

        } catch (CheckException ce) {
            // tests determined that the resource is not working
            isGood = false;
            note = ce.getMessage();
        } catch (Throwable t) {
            // the test itself failed
            isGood = false;
            note = "test failed, reason: " + t;
        }
        return new Availability(isGood, note);
    }

    /**
     * Sets the state of the service.
     */
    @Override
    public void setState(String state) {
        String key = appName + RestAction.STATE_MODE_KEY;
        if (RestAction.STATE_OFFLINE.equalsIgnoreCase(state)) {
            System.setProperty(key, RestAction.STATE_OFFLINE);
        } else if (RestAction.STATE_READ_ONLY.equalsIgnoreCase(state)) {
            System.setProperty(key, RestAction.STATE_READ_ONLY);
        } else if (RestAction.STATE_READ_WRITE.equalsIgnoreCase(state)) {
            System.setProperty(key, RestAction.STATE_READ_WRITE);
        } else {
            throw new IllegalArgumentException("invalid state: " + state
                    + " expected: " + RestAction.STATE_READ_WRITE + "|"
                    + RestAction.STATE_OFFLINE + "|" + RestAction.STATE_READ_ONLY);
        }
        log.info("WebService state changed: " + key + "=" + state + " [OK]");
    }

    private String getState() {
        String key = appName + RestAction.STATE_MODE_KEY;
        String ret = System.getProperty(key);
        if (ret == null) {
            return RestAction.STATE_READ_WRITE;
        }
        return ret;
    }

}
