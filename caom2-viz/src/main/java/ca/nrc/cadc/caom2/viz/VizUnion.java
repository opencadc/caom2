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

package ca.nrc.cadc.caom2.viz;

import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.Chunk;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.Part;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.ProductType;
import ca.nrc.cadc.caom2.compute.PolygonUtil;
import ca.nrc.cadc.caom2.compute.PositionUtil;
import ca.nrc.cadc.caom2.compute.Util;
import ca.nrc.cadc.caom2.types.CartesianTransform;
import ca.nrc.cadc.caom2.types.MultiPolygon;
import ca.nrc.cadc.caom2.types.Point;
import ca.nrc.cadc.caom2.types.Polygon;
import ca.nrc.cadc.caom2.types.SegmentType;
import ca.nrc.cadc.caom2.types.Vertex;
import ca.nrc.cadc.caom2.xml.ObservationReader;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.geom.Ellipse2D;
import java.awt.geom.GeneralPath;
import java.io.File;
import java.io.FileReader;
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
    private final String productID;
    private boolean forceRecompute = false;

    public VizUnion(File obsFile, String productID, boolean forceRecompute) {
        this.obsFile = obsFile;
        this.productID = productID;
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
            log.info("found: " + p.getProductID());
            if (productID == null || productID.equals(p.getProductID())) {
                doit(p);
            }
        }
    }

    private void doit(Plane plane)
        throws Exception {
        DisplayPane dp = new DisplayPane(forceRecompute);
        dp.setPlane(plane);
        JFrame f = new JFrame("CAOM-2.0 VizTest : " + plane.getProductID());
        f.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        f.getContentPane().add(dp);
        f.pack();
        f.setLocation(1000, 200);
        f.setVisible(true);
    }

    static class DisplayPane extends JPanel {
        private DrawArea viewer;
        private boolean recomp;

        public DisplayPane(boolean recomp) {
            super(new BorderLayout());
            JLabel status = new JLabel("");
            this.viewer = new DrawArea(status, "", false);
            this.add(viewer, BorderLayout.CENTER);
            this.add(status, BorderLayout.SOUTH);
            this.setPreferredSize(new Dimension(800, 800));

            this.recomp = recomp;
        }


        public void setPlane(Plane plane)
            throws Exception {
            Polygon bounds = null;
            ProductType ptype = Util.choseProductType(plane.getArtifacts());
            if (plane.position != null && plane.position.bounds != null && !recomp) {
                // use polygon from input file
                bounds = (Polygon) plane.position.bounds;
            } else {
                try {
                    log.info("recomputing union... " + ptype);
                    bounds = PositionUtil.computeBounds(plane.getArtifacts(), ptype);
                    log.info("recomputing union... DONE");
                } catch (Exception ipe) {
                    log.warn("computeShape failed", ipe);
                }
            }

            if (bounds == null) {
                log.error("could not compute union...");
                System.exit(1);
            } else {
                log.info("bounds: " + bounds);
                log.info("center: " + bounds.getCenter());
                log.info("area: " + bounds.getArea());
            }

            viewer.clear();

            CartesianTransform trans = CartesianTransform.getTransform(bounds.getSamples());
            setArtifacts(plane.getArtifacts(), trans, ptype);


            boolean doHull = true;
            MultiPolygon hull = new MultiPolygon();
            SegmentType t = SegmentType.MOVE;
            for (Point p : bounds.getPoints()) {
                hull.getVertices().add(new Vertex(p.cval1, p.cval2, t));
                t = SegmentType.LINE;
            }
            hull.getVertices().add(Vertex.CLOSE);
            renderPolygon(hull, trans, Color.RED, 4.0f, true);

            renderPolygon(bounds.getSamples(), trans, Color.BLUE, 1.0f, false);

            viewer.repaint();
        }

        public void renderPolygon(MultiPolygon bounds, CartesianTransform trans, Color color, float thick, boolean showVerts) {
            log.info("Polygon -> GeneralPath");
            MultiPolygon tmp = trans.transform(bounds);
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
                    log.info("tmp vertex: " + v);
                    if (!SegmentType.CLOSE.equals(v.getType())) {
                        java.awt.Shape s = new Ellipse2D.Double(v.cval1 - 0.5 * sz, v.cval2 - 0.5 * sz, sz, sz);
                        viewer.add(s, Color.BLACK, false);
                    }
                }

                Point c = tmp.getCenter();
                log.info("tmp center: " + c.cval1 + "," + c.cval2);
                log.info("tmp area: " + tmp.getArea());
                java.awt.Shape s = new Ellipse2D.Double(c.cval1 - 0.5 * sz, c.cval2 - 0.5 * sz, sz, sz);
                viewer.add(s, color, true);
            }

            if (!trans.isNull()) {
                log.info("rendering with " + trans);
                CartesianTransform vtrans = trans.getInverseTransform();
                viewer.setTransform(vtrans);
            }
            viewer.setPrefix("sky coordinates: ");
            viewer.setFitShape(gpa);
        }

        public void setArtifacts(Set<Artifact> artifacts, CartesianTransform trans, ProductType productType)
            throws Exception {
            List<MultiPolygon> comps = new ArrayList<MultiPolygon>();
            int i = 0;
            for (Artifact a : artifacts) {
                for (Part p : a.getParts()) {
                    for (Chunk c : p.getChunks()) {
                        if (Util.useChunk(a.getProductType(), p.productType, c.productType, productType)) {
                            if (c.position != null) {
                                MultiPolygon poly = PositionUtil.toPolygon(c.position);
                                if (poly != null) {
                                    comps.add(poly);
                                }
                            }
                        }
                    }
                }
            }
            renderComponents(comps, trans);
        }

        public void renderComponents(List<MultiPolygon> polys, CartesianTransform trans) {
            ArrayList<GeneralPath> gpP = new ArrayList<GeneralPath>();
            if (polys != null) {
                log.info("Polygon[] -> GeneralPath");
                for (MultiPolygon p : polys) {
                    p = trans.transform(p);
                    gpP.add(PolygonUtil.toGeneralPath(p));
                }
            }
            log.info("rendering filled artifacts...");
            for (GeneralPath gp : gpP) {
                viewer.add(gp, Color.LIGHT_GRAY, true);
            }

            log.info("rendering artifact outlines...");
            for (GeneralPath gp : gpP) {
                viewer.add(gp, Color.GRAY, false);
            }
        }

    }
}
