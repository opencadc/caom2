/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2019.                            (c) 2019.
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

import ca.nrc.cadc.caom2.PlaneURI;
import ca.nrc.cadc.caom2.PublisherID;
import ca.nrc.cadc.caom2ops.ArtifactQueryResult;
import ca.nrc.cadc.caom2ops.CaomTapQuery;
import ca.nrc.cadc.caom2ops.TransientFault;
import ca.nrc.cadc.caom2ops.UsageFault;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.ParameterUtil;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.log4j.Logger;
import org.opencadc.datalink.DataLink;
import org.opencadc.datalink.ServiceDescriptor;
import org.opencadc.datalink.server.DataLinkSource;

/**
 *
 * @author pdowler
 */
public class DynamicTableData implements DataLinkSource {
    private static final Logger log = Logger.getLogger(DynamicTableData.class);

    private Integer maxrec;
    private Iterator<String> argIter;
    private CaomTapQuery query;
    private boolean downloadOnly;
    private ArtifactProcessor ap;

    public DynamicTableData(Job job, CaomTapQuery query, ArtifactProcessor ap) {
        List<String> args = ParameterUtil.findParameterValues("id", job.getParameterList());
        this.argIter = args.iterator();
        this.query = query;
        this.ap = ap;
        log.debug("constructor complete");
    }

    @Override
    public void setDownloadOnly(boolean downloadOnly) {
        this.downloadOnly = downloadOnly;
    }

    @Override
    public void setMaxrec(Integer maxrec) {
        this.maxrec = maxrec;
    }

    @Override
    public Iterator<DataLink> links() {
        log.debug("links() called");
        return new ConcatIterator();
    }

    
    @Override
    public Iterator<ServiceDescriptor> descriptors() {
        return new ArrayList<ServiceDescriptor>(0).iterator();
    }

    private class ConcatIterator implements Iterator<DataLink> {

        private int count = 0;
        private Iterator<DataLink> curIter;

        public boolean hasNext() {
            if (argIter == null) {
                return false; // done
            }
            
            if (curIter != null && !curIter.hasNext()) {
                curIter = null; // exhausted
            }
            
            if (curIter == null && (maxrec == null || count < maxrec)) {
                curIter = getBatchIterator();
            }

            if (curIter == null) {
                log.debug("ConcatIterator.hasNext: curIter==null");
                return false;
            }
            log.debug("ConcatIterator.hasNext: " + curIter.hasNext());
            return curIter.hasNext();
        }

        public DataLink next() {
            DataLink ret = curIter.next();
            count++;
            return ret;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }

        private Iterator<DataLink> getBatchIterator() {
            log.debug("getBatchIterator: START");
            if (!argIter.hasNext()) {
                return null;
            }
            curIter = null;
            while (argIter.hasNext() && curIter == null) {
                String s = argIter.next();
                URI uri = null;
                PublisherID pubID = null;
                try {
                    List<DataLink> links = null;
                    try {
                        uri = new URI(s);
                        pubID = new PublisherID(uri);
                    } catch (URISyntaxException | IllegalArgumentException ex) {
                        links = new ArrayList<>(1);
                        DataLink usage = new DataLink(s, DataLink.Term.THIS);
                        usage.errorMessage = "UsageFault: invalid ID: " + s;
                        links.add(usage);
                    }
                    if (pubID != null) {
                        try {
                            log.debug("getBatchIterator: " + uri);
                            ArtifactQueryResult ar = query.performQuery(pubID, downloadOnly);
                            if (ar == null || ar.getArtifacts().isEmpty()) {
                                links = new ArrayList<>(1);
                                DataLink notFound = new DataLink(s, DataLink.Term.THIS);
                                notFound.errorMessage = "NotFoundFault: " + s;
                                links.add(notFound);
                            } else {
                                log.debug("getBatchIterator: " + uri + ": " + ar.getArtifacts().size() + " artifacts");
                                links = ap.process(uri, ar);
                            }
                        } catch (TransientFault f) {
                            links = new ArrayList<>(1);
                            DataLink fail = new DataLink(s, DataLink.Term.THIS);
                            fail.errorMessage = f.toString();
                            links.add(fail);
                        }
                    }

                    if (links != null && !links.isEmpty()) {
                        log.debug("getBatchIterator: " + uri + ": " + links.size() + " links");
                        curIter = links.iterator();
                    }
                } catch (IOException ex) {
                    throw new RuntimeException("query failed: " + s, ex);
                } catch (ResourceNotFoundException ex) {
                    throw new RuntimeException("cannot find TAP service: " + s, ex);
                } catch (CertificateException ex) {
                    throw new RuntimeException("query failed: " + s, ex);
                }
            }

            return curIter;
        }
    }

}
