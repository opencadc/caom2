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

package ca.nrc.cadc.datalink;

import ca.nrc.cadc.caom2.ProductType;
import ca.nrc.cadc.dali.tables.votable.TableField;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * A single result output by the data link service.
 *
 * @author pdowler
 */
public class DataLink implements Iterable<Object>
{
    public static final String DOWNLOAD = "ivo://ivoa.net/std/DataLink/v1.0#DOWNLOAD";
    public static final String CUTOUT = "ivo://ivoa.net/std/CutoutService/v1.0";

    // standard DataLink fields
    private URI id;
    private URL url;
    public URI serviceType;
    public String description;
    public String semantics;
    public String contentType;
    public Long contentLength;
    public String errorMessage;
    
    // custom CADC field
    public List<ProductType> productTypes = new ArrayList<ProductType>();

    public DataLink(URI id, URL url)
    {
        this.id = id;
        this.url = url;
    }

    public URI getID()
    {
        return id;
    }

    public URL getURL()
    {
        return url;
    }

    public int size() { return 9; } // number of fields

    // order: uri, productType, contentType, contentLength, URL
    public Iterator<Object> iterator()
    {
        return new Iterator<Object>()
        {
            int cur = 0;
            public boolean hasNext()
            {
                return (cur < size());
            }

            public Object next()
            {
                int n = cur;
                cur++;
                switch(n)
                {
                    case 0: return id;
                    case 1: return url;
                    case 2: return serviceType;
                    case 3: return semantics;
                    case 4: return description;
                    case 5: return contentType;
                    case 6: return contentLength;
                    case 7: return errorMessage;
                    case 8: return toProductTypeMask(productTypes);
                }
                throw new NoSuchElementException();
            }

            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    private String toProductTypeMask(List<ProductType> productTypes)
    {
        StringBuilder sb = new StringBuilder();
        for (ProductType p : productTypes)
        {
            sb.append(p.getValue());
            sb.append(",");
        }
        if (sb.length() > 0)
            return sb.substring(0, sb.length() - 1);
        return null;
    }

    /**
     * Get list of table fields that matches the iteration order of the DataLink.
     * 
     * @return
     */
    public static List<TableField> getFields()
    {
        List<TableField> fields = new ArrayList<TableField>();
        TableField f;

        f = new TableField("ID", "char");
        f.variableSize = Boolean.TRUE;
        f.ucd = "meta.id";
        f.utype = "datalink:Datalink.ID";
        fields.add(f);

        f = new TableField("accessURL", "char");
        f.variableSize = Boolean.TRUE;
        f.utype = "datalink:Datalink.accessURL";
        fields.add(f);

        f = new TableField("serviceType", "char");
        f.variableSize = Boolean.TRUE;
        f.utype = "datalink:Datalink.serviceType";
        fields.add(f);
        
        f = new TableField("description", "char");
        f.variableSize = Boolean.TRUE;
        f.utype = "datalink:Datalink.description";
        fields.add(f);
        
        f = new TableField("semantics", "char");
        f.variableSize = Boolean.TRUE;
        f.utype = "datalink:Datalink.semantics";
        fields.add(f);

        f = new TableField("contentType", "char");
        f.variableSize = Boolean.TRUE;
        f.utype = "datalink:Datalink.contentType";
        fields.add(f);

        f = new TableField("contentLength", "long");
        f.unit = "byte";
        f.utype = "datalink:Datalink.contentLength";
        fields.add(f);

        f = new TableField("errorMessage", "char");
        f.variableSize = Boolean.TRUE;
        f.utype = "datalink:Datalink.error";
        fields.add(f);
        
        f = new TableField("productType", "char");
        f.variableSize = Boolean.TRUE;
        f.utype = "caom:Artifact.productType";
        fields.add(f);
        
        return fields;
    }
    
    @Override
    public String toString()
    {
        return "DataLink[" + id + "," + url + "]";
    }
}
