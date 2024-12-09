/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2022.                            (c) 2022.
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

package org.opencadc.bifrost;

import ca.nrc.cadc.util.InvalidConfigException;
import ca.nrc.cadc.util.MultiValuedProperties;
import ca.nrc.cadc.util.PropertiesReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
public class BifrostConfig {
    private static final Logger log = Logger.getLogger(BifrostConfig.class);

    private static final String CONFIG = "bifrost.properties";
    
    private static final String BASE_KEY = "org.opencadc.bifrost";
    private static final String QUERY_KEY = BASE_KEY + ".queryService";
    private static final String LOCATOR_KEY = BASE_KEY + ".locatorService";
    private static final String READ_KEY = BASE_KEY + ".readGrantProvider";
    
    private final URI queryService;
    private final URI locatorService;
    private final List<URI> readGrantProviders = new ArrayList<>();
    
    public BifrostConfig() {
        StringBuilder sb = new StringBuilder();
        try {
            PropertiesReader r = new PropertiesReader(CONFIG);
            MultiValuedProperties props = r.getAllProperties();
            
            String qs = props.getFirstPropertyValue(QUERY_KEY);
            URI qsURI = null;
            sb.append("\n\t").append(QUERY_KEY).append(" - ");
            if (qs == null) {
                sb.append("MISSING");
            } else {
                try {
                    qsURI = new URI(qs);
                    sb.append("OK");
                } catch (URISyntaxException ex) {
                    sb.append("ERROR invalid URI: " + qs);
                }
            }
            
            String loc = props.getFirstPropertyValue(LOCATOR_KEY);
            URI locURI = null;
            sb.append("\n\t").append(LOCATOR_KEY).append(" - ");
            if (loc == null) {
                sb.append("MISSING");
            } else {
                try {
                    locURI = new URI(loc);
                    sb.append("OK");
                } catch (URISyntaxException ex) {
                    sb.append("ERROR invalid URI: " + loc);
                }
            }
            
            // optional
            List<String> srgp = props.getProperty(READ_KEY);
            sb.append("\n\t").append(READ_KEY).append(" - ");
            if (srgp == null || srgp.isEmpty()) {
                sb.append("NONE");
            } else {
                for (String s : srgp) {
                    try {
                        URI u = new URI(s);
                        readGrantProviders.add(u);
                        sb.append(" ").append(s);
                    } catch (URISyntaxException ex) {
                        sb.append("ERROR invalid URI: " + s);
                    }
                }
            }
            
            if (qsURI == null && locURI == null) {
                throw new InvalidConfigException("invalid config: " + sb.toString());
            }
            this.queryService = qsURI;
            this.locatorService = locURI;
            // log would be OK if this method was called once in an init action, but is is per request
            //log.info(sb.toString());
        } catch (InvalidConfigException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new InvalidConfigException("invalid config: " + ex.getMessage(), ex);
        }
    }
    
    public URI getQueryService() {
        return queryService;
    }

    public URI getLocatorService() {
        return locatorService;
    }

    public List<URI> getReadGrantProviders() {
        return readGrantProviders;
    }
}
