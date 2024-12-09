/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2024.                            (c) 2024.
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

package ca.nrc.cadc.caom2.viz;

import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.Chunk;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.Part;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.compute.PolygonUtil;
import ca.nrc.cadc.caom2.compute.PositionUtil;
import ca.nrc.cadc.caom2.compute.Util;
import ca.nrc.cadc.caom2.compute.types.MultiPolygon;
import ca.nrc.cadc.caom2.compute.types.SegmentType;
import ca.nrc.cadc.caom2.compute.types.Vertex;
import ca.nrc.cadc.caom2.vocab.DataLinkSemantics;
import ca.nrc.cadc.caom2.xml.ObservationReader;
import ca.nrc.cadc.dali.CartesianTransform;
import ca.nrc.cadc.dali.Circle;
import ca.nrc.cadc.dali.MultiShape;
import ca.nrc.cadc.dali.Point;
import ca.nrc.cadc.dali.Polygon;
import ca.nrc.cadc.dali.Shape;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.geom.Ellipse2D;
import java.awt.geom.GeneralPath;
import java.io.File;
import java.io.FileReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import org.apache.log4j.Logger;

/**
 * Simple test program that draws resulting geometry.
 *
 * @author pdowler
 * @version $Version$
 */
public class VizUnion {
    private static Logger log = Logger.getLogger(VizUnion.class);

    private final File obsFile;
    private final URI planeURI;
    private boolean forceRecompute = false;

    public VizUnion(File obsFile, URI planeURI, boolean forceRecompute) {
        this.obsFile = obsFile;
        this.planeURI = planeURI;
        this.forceRecompute = forceRecompute;
    }

    public void doit()
        throws Exception {
        ObservationReader r = new ObservationReader();
        Observation o = r.read(new FileReader(obsFile));

        boolean done = false;
        Iterator<Plane> iter = o.getPlanes().iterator();
        while (!done && iter.hasNext()) {
            Plane p = iter.next();
            log.info("found: " + p.getURI());
            if (planeURI == null || planeURI.equals(p.getURI())) {
                doit(p);
            }
        }
    }

    private void doit(Plane plane)
        throws Exception {
        Shape bounds = null;
        MultiShape samples = null;
        if (plane.position != null && plane.position.getBounds() != null && !forceRecompute) {
            // use polygon from input file
            bounds = plane.position.getBounds();
            samples = plane.position.getSamples();
        } else {
            try {
                log.info("recomputing union... ");
                PositionUtil.ComputedBounds cb = PositionUtil.computeBounds(plane.getArtifacts(), DataLinkSemantics.THIS);
                bounds = cb.bounds;
                samples = cb.samples;
                log.info("recomputing union... DONE");
            } catch (Exception ipe) {
                log.warn("computeShape failed", ipe);
            }
        }
        if (bounds == null) {
            log.error("could not compute union: " + plane.getURI());
            return;
        } else {
            log.info("bounds: " + bounds);
            log.info("center: " + bounds.getCenter());
            log.info("area: " + bounds.getArea());
            log.info("samples: " + samples.getShapes().size());
        }
            
        DisplayPane dp = new DisplayPane();

        dp.setPlane(plane, bounds, samples);
        JFrame f = new JFrame("CAOM-2.0 VizTest : " + plane.getURI());
        f.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        f.getContentPane().add(dp);
        f.pack();
        f.setLocation(1000, 200);
        f.setVisible(true);
    }

    static class DisplayPane extends JPanel {
        private DrawArea viewer;

        public DisplayPane() {
            super(new BorderLayout());
            JLabel status = new JLabel("");
            this.viewer = new DrawArea(status, "", false);
            this.add(viewer, BorderLayout.CENTER);
            this.add(status, BorderLayout.SOUTH);
            this.setPreferredSize(new Dimension(800, 800));
        }


        public void setPlane(Plane plane, Shape bounds, MultiShape samples)
            throws Exception {
            
            viewer.clear();
            
            CartesianTransform trans = CartesianTransform.getTransform(bounds);

            Shape tmp = trans.transform(bounds);
            Polygon ptmp;
            if (tmp instanceof Circle) {
                Circle c = (Circle) tmp;
                ptmp = Circle.generatePolygonApproximation(c, 21);
            } else {
                ptmp = (Polygon) tmp;
            }
            
            MultiPolygon hull = PolygonUtil.convert(ptmp);
            GeneralPath gpa = renderPolygon(hull, Color.RED, 4.0f, true);
            ptmp = null;

            log.info("render MultiShape: " + samples.getShapes().size());
            int i = 0;
            for (Shape s : samples.getShapes()) {
                tmp = trans.transform(s);
                if (tmp instanceof Circle) {
                    Circle c = (Circle) tmp;
                    ptmp = Circle.generatePolygonApproximation(c, 21);
                } else {
                    ptmp = (Polygon) tmp;
                }
                log.info("render sample: " + i++ + " " + ptmp);
                MultiPolygon ms = PolygonUtil.convert(ptmp);
                renderPolygon(ms, Color.BLUE, 1.0f, false);
            }

            if (!trans.isNull()) {
                log.info("rendering with " + trans);
                CartesianTransform vtrans = trans.getInverseTransform();
                viewer.setTransform(vtrans);
            }
            
            viewer.setPrefix("sky coordinates: ");
            viewer.setFitShape(gpa);
            viewer.repaint();
        }

        public GeneralPath renderPolygon(MultiPolygon tmp, Color color, float thick, boolean showVerts) {
            log.debug("Polygon -> GeneralPath");
            GeneralPath gpa = PolygonUtil.toGeneralPath(tmp);

            double polySize = tmp.getSize(); //Math.max(box.width, box.height);

            // render bounds
            if (gpa != null) {
                viewer.add(gpa, color, false, thick);
            }

            if (showVerts) {
                // render vertices 
                double sz = 0.02 * polySize;
                for (int i = 0; i < tmp.getVertices().size(); i++) {
                    Vertex v = tmp.getVertices().get(i);
                    log.debug("tmp vertex: " + v);
                    if (!SegmentType.CLOSE.equals(v.getType())) {
                        java.awt.Shape s = new Ellipse2D.Double(v.getLongitude() - 0.5 * sz, v.getLatitude() - 0.5 * sz, sz, sz);
                        viewer.add(s, Color.BLACK, false);
                    }
                }

                Point c = tmp.getCenter();
                log.info("tmp center: " + c.getLongitude() + "," + c.getLatitude());
                log.info("tmp area: " + tmp.getArea());
                java.awt.Shape s = new Ellipse2D.Double(c.getLongitude() - 0.5 * sz, c.getLatitude() - 0.5 * sz, sz, sz);
                viewer.add(s, color, true);
            }
            
            return gpa;
        }
    }
}
