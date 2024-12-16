/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2023.                            (c) 2023.
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

package ca.nrc.cadc.caom2.repo.client;

import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.caom2.ObservationResponse;
import ca.nrc.cadc.caom2.ObservationState;
import ca.nrc.cadc.caom2.xml.ObservationReader;
import ca.nrc.cadc.net.HttpGet;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.concurrent.Callable;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;

/**
 * Class devoted to get all information from an observation based of its ObservationState
 *
 * @author jduran
 *
 */
public class Worker implements Callable<ObservationResponse> {

    private static final Logger log = Logger.getLogger(Worker.class);

    private ObservationState state = null;
    private Subject subject = null;
    private String baseHTTPURL = null;

    public Worker(ObservationState state, Subject subject, String url) {
        this.state = state;
        this.subject = subject;
        this.baseHTTPURL = url;
    }

    @Override
    public ObservationResponse call() throws Exception {
        return getObservation();
    }

    public ObservationResponse getObservation() {
        return getObservation(state.getURI().getURI());
    }

    public ObservationResponse getObservation(URI uri) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        String surl = baseHTTPURL + File.separator + state.getURI().getURI().getSchemeSpecificPart();
        log.debug("URL: " + surl);
        URL url = null;
        try {
            url = new URL(surl);
        } catch (MalformedURLException e) {
            throw new RuntimeException("Unable to create URL object for " + surl);
        }
        HttpGet get = new HttpGet(url, bos);

        if (subject != null) {
            Subject.doAs(subject, new RunnableAction(get));
        } else {
            get.run();
        }

        ObservationResponse wr = new ObservationResponse(state);
        if (get.getThrowable() != null) {
            if (get.getThrowable() instanceof Exception) {
                wr.error = (Exception) get.getThrowable();
            } else {
                wr.error = new RuntimeException("failed to get observation", get.getThrowable());
            }
        } else {
            try {
                ObservationReader obsReader = new ObservationReader();
                ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
                wr.observation = obsReader.read(bis);
            } catch (Exception e) {
                wr.error = new IllegalArgumentException("failed to read observation document: " + e.getMessage(), e);
            }
        }
        return wr;
    }

}