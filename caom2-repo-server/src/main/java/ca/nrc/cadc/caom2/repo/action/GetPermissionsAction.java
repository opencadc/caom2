/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2020.                            (c) 2020.
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
 *  : 5 $
 *
 ************************************************************************
 */

package ca.nrc.cadc.caom2.repo.action;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.caom2.access.AccessUtil;
import ca.nrc.cadc.caom2.access.ArtifactAccess;
import ca.nrc.cadc.caom2.persistence.ReadAccessDAO;
import ca.nrc.cadc.caom2.repo.CaomRepoConfig;
import ca.nrc.cadc.caom2.repo.PropertyAuthorizer;
import ca.nrc.cadc.io.ByteCountOutputStream;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.rest.InlineContentHandler;
import ca.nrc.cadc.rest.RestAction;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import org.apache.log4j.Logger;
import org.opencadc.gms.GroupURI;
import org.opencadc.permissions.ReadGrant;
import org.opencadc.permissions.xml.GrantWriter;

public class GetPermissionsAction extends RestAction {

    private static final Logger log = Logger.getLogger(GetPermissionsAction.class);

    // hours until the grant expires
    private static final int HOURS_UNTIL_EXPIRES = 24;

    private static final String PARAM_OP = "OP";
    private static final String PARAM_ID = "ID";

    public enum Operation {
        read
    }

    @Override
    protected InlineContentHandler getInlineContentHandler() {
        return null;
    }

    @Override
    public void doAction() throws Exception {
        log.debug("GET ACTION");

        String op = syncInput.getParameter(PARAM_OP);
        String id = syncInput.getParameter(PARAM_ID);
        log.debug(PARAM_OP + ": " + op);
        log.debug(PARAM_ID + ": " + id);

        if (op == null) {
            throw new IllegalArgumentException("missing required parameter, " + PARAM_OP + "=" + Operation.read);
        }
        try {
            Operation.valueOf(op);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("invalid " + PARAM_OP + " parameter, must be "
                                                   + PARAM_OP + "=" + Operation.read);
        }
        if (id == null) {
            throw new IllegalArgumentException("missing required parameter, " + PARAM_ID);
        }

        URI assetID;
        try {
            assetID = new URI(id);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("invalid " + PARAM_ID + " parameter, not a valid URI: " + id);
        }

        String context = syncInput.getContextPath();
        context = context.startsWith("/") ? context.substring(1) : context;
        String propertiesFilename = context + "-perms.properties";

        PropertyAuthorizer propertiesAuthorization = new PropertyAuthorizer(propertiesFilename);
        propertiesAuthorization.authorize();

        doGetPermissions(assetID);
    }

    protected void doGetPermissions(URI artifactURI) throws Exception {
        log.debug("START: " + artifactURI);

        ReadAccessDAO readAccessDAO = getReadAccessDAO(artifactURI);
        ReadAccessDAO.RawArtifactAccess raa = readAccessDAO.getArtifactAccess(artifactURI);

        if (raa == null) {
            throw new ResourceNotFoundException("not found: " + artifactURI);
        }

        ArtifactAccess artifactAccess =
            AccessUtil.getArtifactAccess(raa.artifact, raa.metaRelease, raa.metaReadAccessGroups,
                                         raa.dataRelease, raa.dataReadAccessGroups);

        ReadGrant readGrant = new ReadGrant(artifactURI, getExpiryDate(), artifactAccess.isPublic);
        for (URI uri : artifactAccess.getReadGroups()) {
            readGrant.getGroups().add(new GroupURI(uri));
        }

        syncOutput.setHeader("Content-Type", "text/xml");
        syncOutput.setCode(200);
        OutputStream os = syncOutput.getOutputStream();
        ByteCountOutputStream bc = new ByteCountOutputStream(os);

        GrantWriter writer = new GrantWriter();
        writer.write(readGrant, bc);
        logInfo.setBytes(bc.getByteCount());

        log.debug("DONE: " + artifactURI);
    }

    // The date the grant expires
    private Date getExpiryDate() {
        Calendar now = Calendar.getInstance();
        now.add(Calendar.HOUR_OF_DAY, HOURS_UNTIL_EXPIRES);
        return now.getTime();
    }

    private ReadAccessDAO getReadAccessDAO(URI artifactURI) throws ResourceNotFoundException {
        String serviceName = syncInput.getContextPath();
        File config = new File(System.getProperty("user.home") + "/config" ,serviceName + ".properties");
        try {
            CaomRepoConfig caomRepoConfig = new CaomRepoConfig(config);
            Iterator<CaomRepoConfig.Item> iterator = caomRepoConfig.iterator();
            while (iterator.hasNext()) {
                CaomRepoConfig.Item item = iterator.next();
                if (archiveMatch(artifactURI, item.getArtifactPattern())) {
                    ReadAccessDAO readAccessDAO = new ReadAccessDAO();
                    readAccessDAO.setConfig(caomRepoConfig.getDAOConfig(item.getCollection()));
                    return readAccessDAO;
                }
            }
            throw new ResourceNotFoundException("not found: " + artifactURI);
        } catch (IOException ex) {
            throw new RuntimeException("CONFIG: failed to read config from " + config.getAbsolutePath());
        }
    }
    
    private boolean archiveMatch(URI uri, String pattern) {
        if (pattern == null) {
            return false;
        }
        return uri.toASCIIString().contains(pattern);
    }

}
