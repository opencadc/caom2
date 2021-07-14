/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2020.                            (c) 2020.
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

package ca.nrc.cadc.caom2.soda;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.artifact.resolvers.CaomArtifactResolver;
import ca.nrc.cadc.caom2.compute.CutoutUtil;
import ca.nrc.cadc.caom2.types.IllegalPolygonException;
import ca.nrc.cadc.caom2ops.CaomTapQuery;
import ca.nrc.cadc.caom2ops.CutoutGenerator;
import ca.nrc.cadc.caom2ops.ServiceConfig;
import ca.nrc.cadc.dali.Circle;
import ca.nrc.cadc.dali.Interval;
import ca.nrc.cadc.dali.Point;
import ca.nrc.cadc.dali.PolarizationState;
import ca.nrc.cadc.dali.Polygon;
import ca.nrc.cadc.dali.Shape;
import ca.nrc.cadc.dali.util.IntervalFormat;
import ca.nrc.cadc.dali.util.PolarizationStateListFormat;
import ca.nrc.cadc.dali.util.ShapeFormat;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.StorageResolver;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.util.Base64;
import ca.nrc.cadc.wcs.exceptions.NoSuchKeywordException;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.opencadc.soda.server.AbstractSodaJobRunner;
import org.opencadc.soda.server.Cutout;
import org.opencadc.soda.server.SodaPlugin;

/**
 *
 * @author pdowler
 */
public class SodaJobRunner extends AbstractSodaJobRunner implements SodaPlugin {
    private static final Logger log = Logger.getLogger(SodaJobRunner.class);

    public static final String PARAM_LABEL = "LABEL";
    public static final String PARAM_FARADAY = "FARADAY";
    public static final String PARAM_RM = "RM";
    
    static final List<String> CUSTOM_CUT_PARAMS = Arrays.asList(
        PARAM_FARADAY, PARAM_RM
    );
    
    private final RegistryClient reg;
    private final URI sodaURI;
    private final URI tapURI;
    
    CaomArtifactResolver artifactResolver;
    
    public SodaJobRunner() { 
        super();
        this.reg = new RegistryClient();
        ServiceConfig sc = new ServiceConfig();
        this.sodaURI = sc.getSodaID();
        this.tapURI = sc.getTapServiceID();
        this.artifactResolver = new CaomArtifactResolver();
        super.getCustomCutoutParams().addAll(CUSTOM_CUT_PARAMS);
    }

    @Override
    public SodaPlugin getSodaPlugin() {
        return this;
    }

    @Override
    public URL toURL(int serialNum, URI uri, Cutout cutout, Map<String, List<String>> extraParams)
            throws IOException {
        String runID = job.getRunID();
        if (runID == null) {
            runID = job.getID();
        }
        
        try {
            CaomTapQuery query = new CaomTapQuery(tapURI, runID);
            Artifact a = query.performQuery(uri);

            if (a == null) {
                StringBuilder path = new StringBuilder();
                path.append("400");
                path.append("|text/plain");
                path.append("|").append("NotFound: " + uri);
                String msg = Base64.encodeString(path.toString());

                URL serviceURL = reg.getServiceURL(sodaURI, Standards.SODA_SYNC_10, AuthMethod.ANON);
                URL url = new URL(serviceURL.toExternalForm() + "/" + msg);
                URL loc = new URL(url.toExternalForm().replace("/sync", "/soda-echo"));
                return loc;
            }

            // log and ignore custom parameters
            for (Map.Entry<String,List<String>> me : extraParams.entrySet()) {
                StringBuilder sb = new StringBuilder();
                sb.append(me.getKey()).append(" = ");
                for (String v : me.getValue()) {
                    sb.append(v).append(" | ");
                }
                String msg = sb.substring(0, sb.length() - 3);
                log.warn("ignore: " + msg);
            }

            // TODO: CAOM currently supports a single custom 1D axis, but we have to find the right Cutout in the list 
            // for the data in question... temporary hack is to fail until we have data to test with
            if (cutout.customAxis != null) {
                StringBuilder sb = new StringBuilder();
                sb.append("cutout with: ").append(cutout.customAxis);
                sb.append("=[").append(cutout.custom.getLower()).append(",").append(cutout.custom.getUpper()).append("]");
                throw new UnsupportedOperationException(sb.toString());
            }
            
            List<String> strCutout = CutoutUtil.computeCutout(a, 
                dali2caom2(cutout.pos), dali2caom2(cutout.band), dali2caom2(cutout.time), dali2caom2(cutout.pol), null, null);
            if (strCutout != null && !strCutout.isEmpty()) {
                StorageResolver resolver = artifactResolver.getStorageResolver(uri);
                if (resolver instanceof CutoutGenerator) {
                    // get the optional label parameter value
                    List<String> labels = extraParams.get(PARAM_LABEL);
                    String label = null;
                    // ignore LABEL parameter for async mode
                    if (syncOutput != null && labels != null && !labels.isEmpty()) {
                        label = labels.get(0);
                    }
                
                    URL url = ((CutoutGenerator) resolver).toURL(a.getURI(), strCutout, label);
                    log.debug("cutout URL: " + url.toExternalForm());
                    return url;
                } else {
                    throw new UnsupportedOperationException("No CutoutGenerator for " + uri.toString());
                }
            } else {
                StringBuilder sb = new StringBuilder();
                sb.append("NoContent: ").append(uri).append(" vs");
                if (cutout.pos != null) {
                    ShapeFormat f = new ShapeFormat();
                    sb.append(" ").append("POS").append("=").append(f.format(cutout.pos));
                }
                
                if (cutout.band != null) {
                    IntervalFormat f = new IntervalFormat();
                    sb.append(" ").append("BAND").append("=").append(f.format(cutout.band));
                }
                
                if (cutout.time != null) {
                    IntervalFormat f = new IntervalFormat();
                    sb.append(" ").append("TIME").append("=").append(f.format(cutout.time));
                }
            
                if (cutout.pol != null && !cutout.pol.isEmpty()) {
                    PolarizationStateListFormat sf = new PolarizationStateListFormat();
                    sb.append(" ").append("POL").append("=").append(sf.format(cutout.pol));
                }

                StringBuilder path = new StringBuilder();
                path.append("400");
                path.append("|text/plain");
                path.append("|").append(sb.toString());
                String msg = Base64.encodeString(path.toString());

                // hack to get base url for soda service
                URL serviceURL = reg.getServiceURL(sodaURI, Standards.SODA_SYNC_10, AuthMethod.ANON);
                String surl = serviceURL.toExternalForm().replace("/sync", "/soda-echo");
                surl = surl + "/" + msg;
                URL loc = new URL(surl);
                log.debug("echo URL: " + loc);
                return loc;
            }
        } catch (CertificateException ex) {
            throw new IllegalArgumentException("delegated X509 certificate is invalid", ex);
        } catch (NoSuchKeywordException ex) {
            throw new RuntimeException("OOPS", ex);
        } catch (MalformedURLException ex) {
            throw new RuntimeException("BUG: failed to create URL", ex);
        } catch (ResourceNotFoundException ex) {
            throw new RuntimeException("CONFIG: failed to find resource", ex);
        }
    }
        
    private ca.nrc.cadc.caom2.types.Interval dali2caom2(Interval dali) {
        if (dali == null) {
            return null;
        }
        return new ca.nrc.cadc.caom2.types.Interval(
            dali.getLower().doubleValue(), dali.getUpper().doubleValue()
        );
    }
    
    private ca.nrc.cadc.caom2.types.Shape dali2caom2(Shape dali) {
        if (dali == null) {
            return null;
        }
        if (dali instanceof Circle) {
            return dali2caom2((Circle) dali);
        }
        if (dali instanceof Polygon) {
            return dali2caom2((Polygon) dali);
        }
        throw new IllegalArgumentException("unexpected DALI shape: " + dali.getClass().getName());
    }
    
    private ca.nrc.cadc.caom2.types.Circle dali2caom2(Circle dali) {
        if (dali == null) {
            return null;
        }
        return new ca.nrc.cadc.caom2.types.Circle(
            new ca.nrc.cadc.caom2.types.Point(dali.getCenter().getLongitude(), dali.getCenter().getLatitude()), 
            dali.getRadius()
        );
    }
    
    private ca.nrc.cadc.caom2.types.Polygon dali2caom2(Polygon dali) {
        if (dali == null) {
            return null;
        }
        List<ca.nrc.cadc.caom2.types.Point> ps = new ArrayList<ca.nrc.cadc.caom2.types.Point>();
        List<ca.nrc.cadc.caom2.types.Vertex> vs = new ArrayList<ca.nrc.cadc.caom2.types.Vertex>();
        ca.nrc.cadc.caom2.types.SegmentType t = ca.nrc.cadc.caom2.types.SegmentType.MOVE;
        for (Point p : dali.getVertices()) {
            ps.add(new ca.nrc.cadc.caom2.types.Point(p.getLongitude(), p.getLatitude()));
            vs.add(new ca.nrc.cadc.caom2.types.Vertex(p.getLongitude(), p.getLatitude(), t));
            t = ca.nrc.cadc.caom2.types.SegmentType.LINE;
        }
        vs.add(ca.nrc.cadc.caom2.types.Vertex.CLOSE);
        
        ca.nrc.cadc.caom2.types.MultiPolygon mp = new ca.nrc.cadc.caom2.types.MultiPolygon(vs);
        ca.nrc.cadc.caom2.types.Polygon ret = new ca.nrc.cadc.caom2.types.Polygon(ps, mp);
        
        try {
            ret.validate();
        } catch (IllegalPolygonException ex) {
            throw new IllegalArgumentException(ex.getMessage(), ex);
        }
        return ret;
    }
    
    private List<ca.nrc.cadc.caom2.PolarizationState> dali2caom2(List<PolarizationState> dali) {
        if (dali == null) {
            return null;
        }
        List<ca.nrc.cadc.caom2.PolarizationState> ret = new ArrayList<>();
        for (PolarizationState s : dali) {
            ret.add(ca.nrc.cadc.caom2.PolarizationState.toValue(s.name()));
        }
        return ret;
    }
}
