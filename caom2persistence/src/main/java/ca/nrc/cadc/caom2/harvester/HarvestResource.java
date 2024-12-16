/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2017.                            (c) 2017.
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
 ************************************************************************
 */


package ca.nrc.cadc.caom2.harvester;

import java.net.URI;
import java.net.URL;

import org.apache.log4j.Logger;

/**
 * Encapsulate the information about a source or destination for harvesting
 * instances.
 *
 * @author pdowler
 */
public class HarvestResource {

    private static final Logger log = Logger.getLogger(HarvestResource.class);

    private String databaseServer;
    private String database;
    private String schema;

    private URI resourceID;
    private URL capabilitiesURL;

    private String collection;

    private int resourceType;

    public static final int SOURCE_DB = 0;
    public static final int SOURCE_URI = 1;
    public static final int SOURCE_CAP_URL = 2;
    public static final int SOURCE_UNKNOWN = -1;

    /**
     * Create a HarvestResource for a database. This is suitable for a
     * destination and when intending to harvest access control tuples.
     *
     * @param databaseServer
     *            server name in $HOME/.dbrc
     * @param database
     *            database name in $HOME/.dbrc and query generation
     * @param schema
     *            schema name for query generation
     * @param collection
     *            name of collection to harvest
     */
    public HarvestResource(String databaseServer, String database, String schema, String collection) {
        if (databaseServer == null || database == null || schema == null || collection == null) {
            throw new IllegalArgumentException("args cannot be null");
        }
        this.databaseServer = databaseServer;
        this.database = database;
        this.schema = schema;
        this.collection = collection;
        this.resourceType = SOURCE_DB;
    }

    public HarvestResource(URI resourceID, String collection) {
        if (resourceID == null || collection == null) {
            throw new IllegalArgumentException("resourceID and collection args cannot be null");
        }
        this.resourceID = resourceID;
        this.collection = collection;
        this.resourceType = SOURCE_URI;
    }

    public HarvestResource(URL resourceCapabilitiesURL, String collection) {
        if (resourceCapabilitiesURL == null || collection == null) {
            throw new IllegalArgumentException("resourceCapabilitiesURL and collection args cannot be null");
        }
        this.capabilitiesURL = resourceCapabilitiesURL;
        this.collection = collection;
        this.resourceType = SOURCE_CAP_URL;
    }

    public String getIdentifier() {
        if (resourceID != null) {
            return resourceID.toASCIIString() + "?" + collection;
        } else if (capabilitiesURL != null) {
            return createProperUrlId(capabilitiesURL.toString(), collection);
        }
        return databaseServer + "." + database + "." + schema + "?" + collection;
    }

    public String getDatabaseServer() {
        return databaseServer;
    }

    public String getDatabase() {
        return database;
    }

    public String getSchema() {
        return schema;
    }

    public URI getResourceID() {
        return resourceID;
    }

    public String getCollection() {
        return collection;
    }

    /**
     * @return the capabilitiesURL
     */
    public URL getCapabilitiesURL() {
        return capabilitiesURL;
    }

    /**
     * @return the resourceType
     */
    public int getResourceType() {
        return resourceType;
    }

    @Override
    public String toString() {
        if (resourceType == SOURCE_URI) {
            return this.resourceID.toASCIIString();
        } else if (resourceType == SOURCE_DB) {
            return this.databaseServer;
        } else if (resourceType == SOURCE_CAP_URL) {
            return this.capabilitiesURL.toString();
        } else {
            return "UNKNOWN";
        }
    }

    private String createProperUrlId(String urlBase, String collection) {
        String properId;
        if (urlBase.indexOf("?") < 0) {
            properId = urlBase + "?" + collection;
        } else if (urlBase.endsWith("?") || urlBase.endsWith("&")) {
            properId = urlBase + collection;
        } else {
            properId = urlBase + "&" + collection;
        }

        return properId;
    }

}
