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

import ca.nrc.cadc.caom2ops.UsageFault;
import ca.nrc.cadc.caom2ops.CaomTapQuery;
import ca.nrc.cadc.caom2.PlaneURI;
import ca.nrc.cadc.caom2.PublisherID;
import ca.nrc.cadc.caom2ops.ArtifactQueryResult;
import ca.nrc.cadc.caom2ops.TransientFault;
import ca.nrc.cadc.dali.tables.TableData;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.ParameterUtil;
import java.io.IOException;
import java.net.URI;
import java.security.cert.CertificateException;
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
    
    private int maxrec;
    private Iterator<String> argIter;
    private CaomTapQuery query;
    private boolean artifactOnly;
    private ArtifactProcessor ap;

    private Iterator<List<Object>> curIter;
    
    private List<ServiceDescriptor> serviceDescriptors = new ArrayList<ServiceDescriptor>();

    public DynamicTableData(int maxrec, Job job, CaomTapQuery query, boolean artifactOnly, ArtifactProcessor ap)
    {
        List<String> args = ParameterUtil.findParameterValues("id", job.getParameterList());
        if (args == null || args.isEmpty())
            throw new UsageFault("missing required parameter ID");
        this.maxrec = maxrec;
        this.argIter = args.iterator();
        this.query = query;
        this.artifactOnly = artifactOnly;
        this.ap = ap;
    }

    public Iterator<ServiceDescriptor> descriptors()
    {
        return serviceDescriptors.iterator();
    }
    
    public Iterator<List<Object>> iterator()
    {
        return new ConcatIterator();
    }
    
    private class ConcatIterator implements Iterator<List<Object>>
    {
        private int count = 0;
        public boolean hasNext()
        {
            if (argIter == null)
                return false; // done

            if ( (curIter == null || !curIter.hasNext()) && count < maxrec )
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
            List<Object> ret = curIter.next();
            count++;
            return ret;
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
                URI uri = null;
                PlaneURI planeURI = null;
                PublisherID pubID = null;
                try
                {
                    List<DataLink> links = null;
                    try
                    {
                        uri = new URI(s);
                        if (PublisherID.SCHEME.equals(uri.getScheme()))
                            pubID = new PublisherID(uri);
                        else
                            planeURI = new PlaneURI(uri);
                    }
                    catch(Exception ex)
                    {
                        links = new ArrayList<>(1);
                        DataLink usage = new DataLink(s, DataLink.Term.THIS);
                        usage.errorMessage = "UsageFault: invalid ID: " + s;
                        links.add(usage);
                    }
                    if (pubID != null || planeURI != null)
                    {
                        try
                        {   
                            ArtifactQueryResult ar;
                            if (pubID != null)
                            {
                                log.debug("getBatchIterator: " + pubID);
                                ar = query.performQuery(pubID, artifactOnly);
                            }
                            else
                            {
                                log.debug("getBatchIterator: " + planeURI);
                                ar = query.performQuery(planeURI, artifactOnly);
                            }
                            if (ar.getArtifacts().isEmpty())
                            {
                                links = new ArrayList<>(1);
                                DataLink notFound = new DataLink(s, DataLink.Term.THIS);
                                notFound.errorMessage = "NotFoundFault: " + s;
                                links.add(notFound);
                            }
                            else
                            {
                                log.debug("getBatchIterator: " + uri + ": " + ar.getArtifacts().size() + " artifacts");
                                links = ap.process(uri, ar);
                            }
                        }
                        catch(TransientFault f)
                        {
                            links = new ArrayList<>(1);
                            DataLink fail = new DataLink(s, DataLink.Term.THIS);
                            fail.errorMessage = f.toString();
                            links.add(fail);
                        }
                    }
                    
                    if (links != null && !links.isEmpty())
                    {
                        log.debug("getBatchIterator: " + planeURI + ": " + links.size() + " links");
                        List<List<Object>> rows = new ArrayList<>();
                        for (DataLink dl : links)
                        {
                            log.debug("adding: " + dl);
                            List<Object> r = new ArrayList<>(dl.size());
                            for (Object o : dl)
                            {
                                r.add(o);
                            }
                            rows.add(r);
                            
                            if (dl.descriptor != null)
                            {
                                log.debug("adding: " + dl.descriptor);
                                serviceDescriptors.add(dl.descriptor);
                            }
                        }
                        curIter = rows.iterator();
                        done = true;
                    }
                }
                catch(IOException ex)
                {
                    throw new RuntimeException("query failed: " + s, ex);
                }
                catch(ResourceNotFoundException ex) {
                    throw new RuntimeException("cannot find TAP service: " + s, ex);
                }
                catch(CertificateException ex)
                {
                    throw new RuntimeException("query failed: " + s, ex);
                }
                finally { }
            }

            return curIter;
        }
    }



}
