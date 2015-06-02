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

package ca.nrc.cadc.caom2.pkg;


import ca.nrc.cadc.net.HttpDownload;
import java.io.ByteArrayOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
public class TarWriter
{
    private static final Logger log = Logger.getLogger(TarWriter.class);
    
    public static final String CONTENT_TYPE = "application/x-tar";
    
    private final TarArchiveOutputStream tout;
    private final Map<String,List<TarContent>> map = new TreeMap<String,List<TarContent>>();
    
    public TarWriter(OutputStream ostream) 
    { 
        this.tout = new TarArchiveOutputStream(ostream);
        tout.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
    }
    
    private class TarContent
    {
        URL url;
        ArchiveEntry entry;
        String contentMD5;
        String emsg;
    }
    
    public void close()
        throws IOException
    {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String,List<TarContent>> me : map.entrySet())
        {
            for (TarContent tc : me.getValue())
            {
                if (tc.emsg != null)
                    sb.append("ERROR ").append(tc.emsg);
               else
                    sb.append("OK ").append(tc.url).append(" ").append(tc.contentMD5).append(" ").append(tc.entry.getName());
                sb.append("\n");
            }
            boolean openEntry = false;
            try
            {
                String filename = me.getKey() + "/README";
                tout.putArchiveEntry(new DynamicTarEntry(filename, sb.length(), new Date()));
                openEntry = true;
                PrintWriter pw = new PrintWriter(new TarStreamWrapper(tout));
                pw.print(sb.toString());
                pw.flush();
                pw.close(); // safe
            }
            finally
            {
                if (openEntry)
                    tout.closeArchiveEntry();
            }
        }
        
        tout.finish();
        tout.close();
    }
    
    public void addMessage(String path, String msg)
    {
        TarContent tc = new TarContent();
        tc.emsg = msg;
        addItem(path, tc);
    }
    
    private void addItem(String path, TarContent tc)
    {
        List<TarContent> t = map.get(path);
        if (t == null)
        {
            t = new ArrayList<TarContent>();
            map.put(path, t);
        }
        t.add(tc);
    }
    
    public void write(String path, URL url)
        throws TarProxyException, IOException
    {
        TarContent item = new TarContent();
        addItem(path, item);
        item.url = url;
        boolean openEntry = false;
        
        try
        {
            // HEAD to get entry metadata
            ByteArrayOutputStream tmp = new ByteArrayOutputStream();
            HttpDownload get = new HttpDownload(url, tmp);
            get.setHeadOnly(true);
            get.run();
            if (get.getThrowable() != null)
            {
                item.emsg = "HEAD " + url.toExternalForm() + " failed, reason: " + get.getThrowable().getMessage();
                throw new TarProxyException("HEAD " + url.toExternalForm() + " failed", get.getThrowable());
            }
            String filename =  path + "/" + get.getFilename();
            long contentLength = get.getContentLength();
            Date lastModified = get.getLastModified();
            item.contentMD5 = get.getContentMD5();

            // create entry
            log.debug("tar entry: " + filename + "," + contentLength + "," + lastModified);
            item.entry = new DynamicTarEntry(filename, contentLength, lastModified);

            tout.putArchiveEntry(item.entry);
            openEntry = true;

            // stream content
            get = new HttpDownload(url, new TarStreamWrapper(tout)); // wrapper suppresses close() by HttpDownload
            get.run();
            if (get.getThrowable() != null)
            {
                item.emsg = "GET " + url.toExternalForm() + " failed, reason: " + get.getThrowable().toString();
                throw new TarProxyException("GET " + url.toExternalForm() + " failed", get.getThrowable());
            }
            log.debug("server response: " + get.getResponseCode() + "," + get.getContentLength());
        }
        finally
        {
            if (openEntry)
                tout.closeArchiveEntry();
        }
    }
    
    private class TarStreamWrapper extends FilterOutputStream
    {
        public TarStreamWrapper(OutputStream ostream)
        {
            super(ostream);
        }

        @Override
        public void close() throws IOException
        {
            // keep underlying stream open
        }
    }
    
    private class DynamicTarEntry extends TarArchiveEntry
    {
        public DynamicTarEntry(String name, long size, Date lastModifiedDate)
        {
            super(name);
            super.setModTime(lastModifiedDate);
            super.setSize(size);
        }

        @Override
        public boolean isDirectory()
        {
            return false;
        }
    }
}
