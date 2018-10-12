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

package ca.nrc.cadc.caom2.datalink;

import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import ca.nrc.cadc.caom2.ProductType;
import ca.nrc.cadc.dali.tables.votable.VOTableField;

/**
 * A single result output by the data link service.
 *
 * @author pdowler
 */
public class DataLink implements Iterable<Object>
{
    /**
     * Terms from the http://www.ivoa.net/rdf/datalink/core vocabulary plus
     * CAOM extensions.
     */
    public enum Term
    {
        THIS("#this"),
        AUXILIARY("#auxiliary"),
        PREVIEW("#preview"),
        PROGENITOR("#progenitor"),
        DERIVATION("#derivation"),
        PROC("#proc"),
        CUTOUT("#cutout"),
        THUMBNAIL("http://www.openadc.org/caom2#thumbnail"),
        PKG("http://www.openadc.org/caom2#pkg");
        
        private final String value;
        private Term(String value) { this.value = value; }
        public String getValue() { return value; }
    }

    // standard DataLink fields
    private final String id;
    private final Term semantics;
    
    public URL url;
    public String serviceDef;
    public String description;
    
    public String contentType;
    public Long contentLength;
    public String errorMessage;

    // custom CADC fields
    public Boolean readable; // predict that the current user is allowed to download
    
    // associated object that turns into a new resource
    public ServiceDescriptor descriptor; // link-specific service descriptor

    public DataLink(String id, Term semantics)
    {
        this.id = id;
        this.semantics = semantics;
    }

    public String getID()
    {
        return id;
    }

    public Term getSemantics()
    {
        return semantics;
    }

    public int size() { return 9; } // number of fields

    // order: uri, productType, contentType, contentLength, URL
    @Override
    public Iterator<Object> iterator()
    {
        return new Iterator<Object>()
        {
            int cur = 0;
            @Override
            public boolean hasNext()
            {
                return (cur < size());
            }

            @Override
            public Object next()
            {
                int n = cur;
                cur++;
                switch(n)
                {
                    // this MUST match the order of the fields in getFields() below
                    case 0: return id;
                    case 1: return safeToString(url);
                    case 2: return serviceDef;
                    case 3: return errorMessage;
                    case 4: return semantics.getValue();
                    case 5: return description;
                    case 6: return contentType;
                    case 7: return contentLength;
                    case 8: return readable;
                }
                throw new NoSuchElementException();
            }

            @Override
            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    String safeToString(URI uri)
    {
        if (uri == null)
            return null;
        return uri.toASCIIString();
    }

    String safeToString(URL url)
    {
        if (url == null)
            return null;
        return url.toExternalForm();
    }

    /**
     * Get list of table fields that matches the iteration order of the DataLink.
     *
     * @return
     */
    public static List<VOTableField> getFields()
    {
        List<VOTableField> fields = new ArrayList<VOTableField>();
        VOTableField f;

        f = new VOTableField("ID", "char", "*");
        f.ucd = "meta.id;meta.main";
        fields.add(f);

        f = new VOTableField("access_url", "char", "*");
        f.ucd = "meta.ref.url";
        fields.add(f);

        f = new VOTableField("service_def", "char", "*");
        f.ucd = "meta.ref";
        fields.add(f);
        
        f = new VOTableField("error_message", "char", "*");
        f.ucd = "meta.code.error";
        fields.add(f);
        
        f = new VOTableField("semantics", "char", "*");
        f.ucd = "meta.code";
        fields.add(f);
        
        f = new VOTableField("description", "char", "*");
        f.ucd = "meta.note";
        fields.add(f);

        f = new VOTableField("content_type", "char", "*");
        f.ucd = "meta.code.mime";
        fields.add(f);

        f = new VOTableField("content_length", "long");
        f.unit = "byte";
        f.ucd = "phys.size;meta.file";
        fields.add(f);

        // custom
        f = new VOTableField("readable", "boolean");
        f.description = "the caller is allowed to use this link with the current authentication";
        fields.add(f);
        
        return fields;
    }

    @Override
    public String toString()
    {
        return "DataLink[" + id + "," + url + "," + semantics + "]";
    }
}
