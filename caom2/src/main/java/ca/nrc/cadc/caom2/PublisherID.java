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
import java.io.Serializable;
import java.net.URI;

/**
 * Globally unique identifer for a CAOM plane. This is meant to be equivalent to
 * an IVOA publisher dataset identifier. Assumption: the Observation.collection
 * is the path component of the resourceID (e.g. the collection is registered as
 * a DataCollection resource in an IVOA registry). 
 * 
 * 
 * @author pdowler
 */
public class PublisherID implements Comparable<PublisherID>, Serializable
{
    private static final long serialVersionUID = 201609271015L;
    
    public static final String SCHEME = "ivo";
    
    private transient URI resourceID;
    
    private final URI uri;
    
    public PublisherID(URI uri)
    {
        CaomValidator.assertNotNull(getClass(), "uri", uri);
        
        if ( !SCHEME.equals(uri.getScheme()))
            throw new IllegalArgumentException("invalid scheme: " + uri.getScheme());
        String auth = uri.getAuthority();
        String path = uri.getPath();
        String id = uri.getQuery();
        
        CaomValidator.assertNotNull(getClass(), "authority", auth);
        CaomValidator.assertNotNull(getClass(), "path", path);
        CaomValidator.assertNotNull(getClass(), "id", id);
        
        String[] ids = id.split("/");
        if (ids.length == 2)
        {
            String oid = ids[0];
            String pid = ids[1];
            CaomValidator.assertValidPathComponent(getClass(), "observationID", oid);
            CaomValidator.assertValidPathComponent(getClass(), "productID", pid);
            
        }
        else
            throw new IllegalArgumentException("input URI has " + ids.length
                    + " id components ("+id+"), expected 2: <observationID>/<productID>");
        this.uri = uri;
        this.resourceID = URI.create(SCHEME + "://" + auth + "/" + path);
    }
    
    public PublisherID(URI resourceID, String observationID, String productID) 
    { 
        CaomValidator.assertNotNull(PublisherID.class, "resourceID", resourceID);
        CaomValidator.assertNotNull(PublisherID.class, "observationID", observationID);
        CaomValidator.assertNotNull(PublisherID.class, "productID", productID);
        this.uri = URI.create(resourceID.toASCIIString() + "?" + observationID + "/" + productID);
        this.resourceID = resourceID;
    }
    
    public URI getURI()
    {
        return uri;
    }
    
    public URI getResourceID()
    {
        return resourceID;
    }
    
    @Override
    public boolean equals(Object o)
    {
        if (o == null)
            return false;
        if (this == o)
            return true;
        if (o instanceof PublisherID)
        {
            PublisherID u = (PublisherID) o;
            return ( this.hashCode() == u.hashCode() );
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return uri.hashCode();
    }

    @Override
    public int compareTo(PublisherID u)
    {
        return this.uri.compareTo(u.uri);
    }
}
