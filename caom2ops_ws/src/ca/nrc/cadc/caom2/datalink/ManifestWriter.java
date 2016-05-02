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

import ca.nrc.cadc.dali.tables.TableData;
import ca.nrc.cadc.dali.tables.TableWriter;
import ca.nrc.cadc.dali.tables.votable.VOTableDocument;
import ca.nrc.cadc.dali.tables.votable.VOTableField;
import ca.nrc.cadc.dali.tables.votable.VOTableResource;
import ca.nrc.cadc.dali.tables.votable.VOTableTable;
import ca.nrc.cadc.dali.util.DefaultFormat;
import ca.nrc.cadc.dali.util.Format;
import ca.nrc.cadc.dali.util.FormatFactory;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.URL;
import java.util.Iterator;
import java.util.List;
import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
public class ManifestWriter implements TableWriter<VOTableDocument>
{
    private static final Logger log = Logger.getLogger(ManifestWriter.class);

    public static final String CONTENT_TYPE = "application/x-download-manifest+txt";

    private int uriCol;
    private int urlCol;
    private int errCol;

    public ManifestWriter(int uriColumn, int urlColumn, int errCol)
    {
        this.uriCol = uriColumn;
        this.urlCol = urlColumn;
        this.errCol = errCol;
    }
    
    public String getContentType()
    {
        return CONTENT_TYPE;
    }

    public String getExtension()
    {
        return "txt";
    }

    public String getErrorContentType()
    {
        return "text/plain";
    }
    
    public void setFormatFactory(FormatFactory ff)
    {
        // no-op: hard-coded behaviour only relying on DataLink class
    }
    
    public void write(Throwable thrown, OutputStream out) 
        throws IOException
    {
        Writer writer = new BufferedWriter(new OutputStreamWriter(out));
        writer.write(thrown.getMessage());
        writer.flush();
    }

    public String getErrorContentType()
    {
        return "text/plain";
    }

    public void write(Throwable t, OutputStream out) 
        throws IOException
    {
        Writer writer = new BufferedWriter(new OutputStreamWriter(out, "UTF-8"));
        writer.write(t.getMessage());
        writer.flush();
    }
    
    public void write(VOTableDocument vot, OutputStream out) 
        throws IOException
    {
        write(vot, out, null);
    }
    
    public void write(VOTableDocument vot, OutputStream out, Long maxrec) 
        throws IOException
    {
        Writer writer = new BufferedWriter(new OutputStreamWriter(out, "UTF-8"));
        write(vot, writer, maxrec);
    }
    
    public void write(VOTableDocument vot, Writer out)
        throws IOException
    {
        write(vot, out, null);
    }

    public void write(VOTableDocument vot, Writer out, Long maxrec)
        throws IOException
    {
        log.debug("write: START maxrec=" + maxrec);
        PrintWriter writer = new PrintWriter(out);
        long rows = 0;
        long maxRows = Long.MAX_VALUE;
        if (maxrec != null)
            maxRows = maxrec.longValue();
        
        // find the TableData object in the VOTable
        VOTableResource vr = vot.getResourceByType("results");
        VOTableTable vt = vr.getTable();
        TableData td = vt.getTableData();
        List<VOTableField> fields = vt.getFields();
        TableData data = vt.getTableData();
        Iterator<List<Object>> iter = data.iterator();
        Format fmt = new DefaultFormat();
        while ( iter.hasNext() && rows < maxRows )
        {
            List<Object> row = iter.next();
            Object oURI = row.get(uriCol);
            Object oURL = row.get(urlCol);
            Object oErr = row.get(errCol);
            try
            {
                if (oURL != null)
                {
                    writer.print("OK\t");
                    writer.println(fmt.format(oURL));
                }
                else
                {
                    writer.print("ERROR\t");
                    writer.println(fmt.format(oErr));
                }
            }
            catch(Exception ex)
            {
                writer.print("ERROR\t");
                writer.print(fmt.format(oURI));
                writer.print(": ");
                writer.println(ex.getMessage());
            }
            rows++;
            writer.flush();
        }
        writer.flush();
        log.debug("write: DONE rows=" + rows);
    }

}
