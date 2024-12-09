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

import ca.nrc.cadc.dali.CartesianTransform;
import ca.nrc.cadc.dali.Point;
import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.Shape;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.event.MouseMotionAdapter;
import java.awt.geom.AffineTransform;
import java.awt.geom.NoninvertibleTransformException;
import java.awt.geom.Rectangle2D;
import java.awt.image.BufferedImage;
import java.util.ArrayList;
import java.util.Iterator;
import javax.swing.JLabel;
import javax.swing.JPanel;

/**
 * Simple drawing area widget for 2D graphics.
 *
 * @author $Author: jburke $
 * @version $Revision: 1.7 $
 */
public class DrawArea extends JPanel {
    private static final long serialVersionUID = 200002171500L;

    private ArrayList shapes;
    private Shape fitShape;
    private AffineTransform trans = null;
    private JLabel statusField;
    private String prefix;
    private boolean sexiCoords;
    private BufferedImage bimg;

    //private double scale;
    private CartesianTransform geomTransform;

    // write the user-space coordinates in the lower-left corner
    private java.awt.geom.Point2D ptSrc = new java.awt.Point();
    private java.awt.geom.Point2D ptDst = new java.awt.geom.Point2D.Double();
    private DisplayPoint ptDisplay = new DisplayPoint();

    private static class DisplayPoint {
        double cval1;
        double cval2;
    }

    public DrawArea(JLabel status, String statusPrefix, boolean sexiCoords) {
        super(true); // double-buffered
        this.prefix = statusPrefix;
        //this.sexiCoords = sexiCoords;
        shapes = new ArrayList();
        statusField = status;
        makeGUI();
    }

    /**
     * @param prefix
     */
    public void setPrefix(String prefix) {
        this.prefix = prefix;
        clearMPD();
    }

    public void setTransform(CartesianTransform trans) {
        this.geomTransform = trans;
    }

    private void makeGUI() {
        setBackground(Color.white);
        if (statusField != null) {
            addMouseListener(new MouseHandler());
            addMouseMotionListener(new MouseMotionHandler());
        }
    }

    private void updateMPD(int x, int y) {
        if (trans == null) {
            return;
        }
        ptSrc.setLocation(x, y);
        try {
            trans.inverseTransform(ptSrc, ptDst);
        } catch (NoninvertibleTransformException ex) {
            System.out.println("Exception: " + ex);
            return;
        }
        ptDisplay.cval1 = ptDst.getX();
        ptDisplay.cval2 = ptDst.getY();
        if (geomTransform != null) {
            Point p = new Point(ptDisplay.cval1, ptDisplay.cval2);
            p = geomTransform.transform(p);
            ptDisplay.cval1 = p.getLongitude();
            ptDisplay.cval2 = p.getLatitude();
        }
        if (sexiCoords) {
            //String[] s = CoordUtil.degreesToSexigessimal(ptDisplay.cval1, ptDisplay.cval1);
            //statusField.setText(prefix + s[0] + "  " + s[1]);
        } else {
            String sx = Double.toString(ptDisplay.cval1);
            String sy = Double.toString(ptDisplay.cval2);
            int ix = sx.length();
            if (ix > 10) {
                ix = 10;
                if (ptDst.getX() < 0.0) {
                    ix++;
                }
            }
            int iy = sy.length();
            if (iy > 10) {
                iy = 10;
                if (ptDst.getY() < 0.0) {
                    iy++;
                }
            }
            String s = sx.substring(0, ix) + "  ,  " + sy.substring(0, iy);
            statusField.setText(prefix + s);
        }
    }

    // clear the coordinate text
    private void clearMPD() {
        if (statusField.getText().startsWith("exit")) {
            return;
        }
        statusField.setText(prefix);
    }

    public void setFitShape(Shape s) {
        this.fitShape = s;
    }

    public void add(Shape shape, Color color, boolean fill) {
        add(shape, color, fill, 1.0f);
    }

    public void add(Shape shape, Color color, boolean fill, float thickness) {
        shapes.add(new CShape(shape, color, fill, thickness));
    }

    /**
     *
     */
    public void clear() {
        shapes.clear();
    }

    /**
     *
     */
    public void paintComponent(Graphics g) {
        Dimension d = getSize();
        Graphics2D g2 = createGraphics2D(d.width, d.height);
        render(g2);
        g2.dispose();
        g.drawImage(bimg, 0, 0, this);
    }

    /**
     * @param w
     * @param h
     * @return
     */
    public Graphics2D createGraphics2D(int w, int h) {
        Graphics2D g2 = null;
        if (bimg == null || bimg.getWidth() != w || bimg.getHeight() != h) {
            bimg = (BufferedImage) createImage(w, h);
        }

        g2 = bimg.createGraphics();
        g2.setBackground(Color.WHITE);
        g2.clearRect(0, 0, w, h);
        g2.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        g2.setRenderingHint(RenderingHints.KEY_RENDERING, RenderingHints.VALUE_RENDER_QUALITY);
        return g2;
    }

    private void render(Graphics2D gc) {
        if (shapes.size() > 0) {
            Rectangle2D rect = null;
            if (fitShape != null) {
                rect = CoordSystemUtil.getBounds2D(new Shape[] {fitShape});
            } else {
                rect = CoordSystemUtil.getBounds2D(shapes);
            }
            double space = Math.max(rect.getWidth() * 0.1, rect.getHeight() * 0.1);

            // try to get line thickness ~2 pixels
            float thickness = (float) (1.0f * rect.getWidth() / ((float) this.getWidth()));

            //CoordSystemUtil.setFittedEvenCoordSystem(gc, this, rect, space);
            CoordSystemUtil.setFittedCoordSystem(gc, this, rect, space);
            trans = gc.getTransform();

            //paintGrid(gc);

            Iterator i = shapes.iterator();
            while (i.hasNext()) {
                CShape cs = (CShape) i.next();
                gc.setStroke(new BasicStroke(thickness * cs.thickness));
                paintShape(gc, cs.shape, cs.color, cs.fill);
            }
            AffineTransform orig = gc.getTransform();
            gc.setTransform(orig);
        }
    }

    // paints a shape
    private void paintShape(Graphics2D gc, Shape s, Color c, boolean fill) {
        gc.setPaint(c);
        gc.setColor(c);
        if (fill) {
            gc.fill(s);
        } else {
            gc.draw(s);
        }
    }

    // package access to CoordSystemUtil.getBounds2D(List) can see this class
    static class CShape {
        public Shape shape;
        public Color color;
        public boolean fill;
        public float thickness;

        public CShape(Shape s, Color c, boolean f, float t) {
            shape = s;
            color = c;
            fill = f;
            thickness = t;
        }
    }

    class MouseMotionHandler extends MouseMotionAdapter {
        public void mouseDragged(MouseEvent e) {
            updateMPD(e.getX(), e.getY());
        }

        public void mouseMoved(MouseEvent e) {
            updateMPD(e.getX(), e.getY());
        }
    }
    
    class MouseHandler extends MouseAdapter {
        public void mousePressed(MouseEvent e) {
            // anything?
        }

        public void mouseReleased(MouseEvent e) {

        }

        public void mouseExited(MouseEvent e) {
            clearMPD();
        }
    }
}

// end of DrawArea.java

