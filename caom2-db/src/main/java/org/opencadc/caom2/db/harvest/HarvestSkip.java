/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2025.                            (c) 2025.
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

package org.opencadc.caom2.db.harvest;

import java.net.URI;
import java.text.DateFormat;
import java.util.Date;
import java.util.UUID;
import org.opencadc.caom2.util.CaomUtil;
import org.opencadc.caom2.util.CaomValidator;

/**
 * Track failed harvest attempts by URI. This is expected to be used for CAOM
 * Observation harvesting.
 *
 * @author pdowler
 */
public class HarvestSkip {

    private final URI source;
    private final String name;
    private final URI uri;
    private final String uriBucket;
    
    public Date tryAfter;
    public String errorMessage;
    
    Date lastModified;
    UUID id;

    /**
     * Primary constructor. All arguments must have values.
     * 
     * @param source remote entity source
     * @param name simple class name of the harvested entity
     * @param uri entity uri that was skipped
     */
    public HarvestSkip(URI source, String name, URI uri) {
        this(source, name, uri, null, null);
    }
    
    /**
     * Convenience constructor with optional error content.
     * 
     * @param source remote entity source
     * @param name simple class name of the harvested entity
     * @param uri entity uri that was skipped
     * @param tryAfter timestamp to control retry attempts (optional)
     * @param emsg error message (optional)
     */
    public HarvestSkip(URI source, String name, URI uri, Date tryAfter, String emsg) {
        CaomValidator.assertNotNull(HarvestSkip.class, "source", source);
        CaomValidator.assertNotNull(HarvestSkip.class, "name", name);
        CaomValidator.assertNotNull(HarvestSkip.class, "uri", uri);
        this.source = source;
        this.name = name;
        this.uri = uri;
        this.tryAfter = tryAfter;
        this.errorMessage = emsg;
        this.uriBucket = CaomUtil.computeBucket(uri);
    }

    @Override
    public String toString() {
        return "HarvestSkip[" + source + "," + name + "," + uri + "]";
    }
    
    String toString(DateFormat df) {
        return "HarvestSkip[" + source + "," + name + "," + uri + "," + df.format(tryAfter) + "]";
    }
    
    public URI getSource() {
        return source;
    }

    public String getName() {
        return name;
    }

    public URI getURI() {
        return uri;
    }

    public String getUriBucket() {
        return uriBucket;
    }

    public UUID getID() {
        return id;
    }

    public Date getLastModified() {
        return lastModified;
    }
}
