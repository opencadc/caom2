/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2011.                            (c) 2011.
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

package ca.nrc.cadc.caom2;

import ca.nrc.cadc.caom2.util.CaomValidator;
import ca.nrc.cadc.util.HashUtil;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;

/**
 *
 * @author pdowler
 */
public class ObservationURI implements Comparable<ObservationURI>, Serializable
{
    private static final long serialVersionUID = 201202091030L;

    public static final String SCHEME = "caom";
    
    private String collection;
    private String observationID;
    
    private transient URI uri;

    private ObservationURI() { }

    public ObservationURI(URI uri)
    {
        if ( !SCHEME.equals(uri.getScheme()))
            throw new IllegalArgumentException("invalid scheme: " + uri.getScheme());
        String ssp = uri.getSchemeSpecificPart();
        CaomValidator.assertNotNull(getClass(), "scheme-specific-part", ssp);
        String[] cop = ssp.split("/");
        if (cop.length == 2)
        {
            this.collection = cop[0];
            this.observationID = cop[1];
            CaomValidator.assertNotNull(getClass(), "collection", collection);
            CaomValidator.assertValidPathComponent(getClass(), "collection", collection);
            CaomValidator.assertNotNull(getClass(), "observationID", observationID);
            CaomValidator.assertValidPathComponent(getClass(), "observationID", observationID);
            this.uri = URI.create(SCHEME + ":" + collection + "/" + observationID);
        }
        else
            throw new IllegalArgumentException("input URI has " + cop.length + " parts ("+ssp+"), expected 2: <collection>/<observationID>");
    }

    public ObservationURI(String collection, String observationID)
    {
        CaomValidator.assertNotNull(getClass(), "collection", collection);
        CaomValidator.assertValidPathComponent(getClass(), "collection", collection);
        CaomValidator.assertNotNull(getClass(), "observationID", observationID);
        CaomValidator.assertValidPathComponent(getClass(), "observationID", observationID);
        this.collection = collection;
        this.observationID = observationID;
        this.uri = URI.create(SCHEME + ":" + collection + "/" + observationID);
    }

    @Override
    public String toString()
    {
        return uri.toASCIIString();
    }

    public String getCollection()
    {
        return collection;
    }

    public String getObservationID()
    {
        return observationID;
    }

    public URI getURI()
    {
        return uri;
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null)
            return false;
        if (this == o)
            return true;
        if (o instanceof ObservationURI)
        {
            ObservationURI u = (ObservationURI) o;
            return ( this.compareTo(u) == 0 );
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return uri.hashCode();
    }

    public int compareTo(ObservationURI u)
    {
        int ret = collection.compareTo(u.collection);
        if (ret != 0)
            return ret;
        return observationID.compareTo(u.observationID);
                
    }
}
