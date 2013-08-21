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

import ca.nrc.cadc.caom2ops.LinkQuery;
import ca.nrc.cadc.caom2ops.ArtifactProcessor;
import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.PlaneURI;
import ca.nrc.cadc.dali.tables.TableData;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.ParameterUtil;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
public class DynamicTableData implements TableData
{
    private static final Logger log = Logger.getLogger(DynamicTableData.class);
    
    private Iterator<String> argIter;
    private LinkQuery query;
    private ArtifactProcessor ap;

    private Iterator<List<Object>> curIter;

    public DynamicTableData(Job job, LinkQuery query, ArtifactProcessor ap)
    {
        List<String> args = ParameterUtil.findParameterValues("uri", job.getParameterList());
        this.argIter = args.iterator();
        this.query = query;
        this.ap = ap;
    }

    public Iterator<List<Object>> iterator()
    {
        return new ConcatIterator();
    }

    private class ConcatIterator implements Iterator<List<Object>>
    {

        public boolean hasNext()
        {
            if (argIter == null)
                return false; // done

            if (curIter == null || !curIter.hasNext())
                curIter = getBatchIterator();
            
            if (curIter == null)
            {
                argIter = null;
                return false;
            }

            return curIter.hasNext();
        }

        public List<Object> next()
        {
            return curIter.next();
        }

        public void remove()
        {
            throw new UnsupportedOperationException();
        }

        private Iterator<List<Object>> getBatchIterator()
        {
            if ( !argIter.hasNext() )
                return null; // done

            curIter = null;
            boolean done = false;
            while (!done && argIter.hasNext())
            {
                String s = argIter.next();
                try
                {
                    URI uri = new URI(s);
                    PlaneURI planeURI = new PlaneURI(uri);
                    log.debug("getBatchIterator: " + planeURI);
                    List<Artifact> artifacts = query.performQuery(planeURI);
                    log.debug("getBatchIterator: " + planeURI + ": " + artifacts.size() + " artifacts");
                    List<DataLink> links = ap.process(uri, artifacts);
                    log.debug("getBatchIterator: " + planeURI + ": " + links.size() + " links");
                    if (!links.isEmpty())
                    {
                        List<List<Object>> rows = new ArrayList<List<Object>>();
                        for (DataLink dl : links)
                        {
                            log.debug("adding: " + dl);
                            List<Object> r = new ArrayList<Object>(dl.size());
                            for (Object o : dl)
                                r.add(o);
                            rows.add(r);
                        }
                        curIter = rows.iterator();
                        done = true;
                    }
                }
                catch(URISyntaxException ex)
                {
                    throw new IllegalArgumentException("invalid URI: " + s);
                }
                catch(IOException ex)
                {
                    throw new RuntimeException("query failed: " + s, ex);
                }
                finally { }
            }

            return curIter;
        }
    }



}
