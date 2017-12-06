/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2017.                            (c) 2017.
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
************************************************************************
*/

package ca.nrc.cadc.caom2;


import java.net.URI;
import java.security.MessageDigest;
import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
public class ChecksumValidator implements Runnable {
    private static final Logger log = Logger.getLogger(ChecksumValidator.class);

    private final boolean acc;
    private int depth;
    private final Observation obs;
    
    public ChecksumValidator(Observation obs, int depth, boolean acc) { 
        this.obs = obs;
        this.depth = depth;
        this.acc = acc;
    }

    private void compare(StringBuilder sb, URI u1, URI u2) {
        sb.append(u1);
        boolean eq = u1.equals(u2);
        if (eq) {
            sb.append(" == ");
        } else {
            sb.append(" != ");
        }
        sb.append(u2);
        if (!eq) {
            sb.append(" [MISMATCH]");
        }
    }
    
    private void out(String s) {
        System.out.println(s);
    }
    
    @Override
    public void run() {
        try {
            log.info("read: " + obs.getCollection() + "/" + obs.getObservationID() + " :: " + obs.getAccMetaChecksum());
            log.info("depth: " + depth);
            
            StringBuilder cs = new StringBuilder();
            StringBuilder acs = new StringBuilder();
            MessageDigest digest = MessageDigest.getInstance("MD5");
            if (depth > 1) {
                for (Plane pl : obs.getPlanes()) {
                    if (depth > 2) {
                        for (Artifact ar : pl.getArtifacts()) {
                            if (depth > 3) {
                                for (Part pa : ar.getParts()) {
                                    if (depth > 4) {
                                        for (Chunk ch : pa.getChunks()) {
                                            URI chunkCS = ch.computeMetaChecksum(digest);
                                            cs.append("\n      chunk: ").append(ch.getID()).append(" ");
                                            compare(cs, ch.getMetaChecksum(), chunkCS);
                                            if (acc) {
                                                URI chunkACS = ch.computeAccMetaChecksum(digest);
                                                acs.append("\n      chunk: ").append(ch.getID()).append(" ");
                                                compare(acs, ch.getAccMetaChecksum(), chunkACS);
                                            }
                                        }
                                    }
                                    URI partCS = pa.computeMetaChecksum(digest);
                                    cs.append("\n       part: ").append(pa.getID()).append(" ");
                                    compare(cs, pa.getMetaChecksum(), partCS);
                                    if (acc) {
                                        URI partACS = pa.computeAccMetaChecksum(digest);
                                        acs.append("\n      chunk: ").append(pa.getID()).append(" ");
                                        compare(acs, pa.getAccMetaChecksum(), partACS);
                                    }
                                }
                            }
                            URI artifactCS = ar.computeMetaChecksum(digest);
                            cs.append("\n       artifact: ").append(ar.getID()).append(" ");
                            compare(cs, ar.getMetaChecksum(), artifactCS);
                            if (acc) {
                                URI artifactACS = ar.computeAccMetaChecksum(digest);
                                acs.append("\n      artifact: ").append(ar.getID()).append(" ");
                                compare(acs, ar.getAccMetaChecksum(), artifactACS);
                            }
                        }
                    }
                    URI planeCS = pl.computeMetaChecksum(digest);
                    cs.append("\n      plane: ").append(pl.getID()).append(" ");
                    compare(cs, pl.getMetaChecksum(), planeCS);
                    if (acc) {
                        URI planeACS = pl.computeAccMetaChecksum(digest);
                        acs.append("\n     plane: ").append(pl.getID()).append(" ");
                        compare(acs, pl.getAccMetaChecksum(), planeACS);
                    }
                }
            }
            URI observationCS = obs.computeMetaChecksum(digest);
            cs.append("\nobservation: ").append(obs.getID()).append(" ");
            compare(cs, obs.getMetaChecksum(), observationCS);
            
            if (acc) {
                URI observationACS = obs.computeAccMetaChecksum(digest);
                acs.append("\nobservation: ").append(obs.getID()).append(" ");
                compare(acs, obs.getAccMetaChecksum(), observationACS);
            }

            out("** metaChecksum **");
            out(cs.toString());
            if (acc) {
                out("** accMetaChecksum **");
                out(acs.toString());
            }
        } catch (Exception ex) {
            log.error("unexpected exception", ex);
        }
    }
    
    
}
