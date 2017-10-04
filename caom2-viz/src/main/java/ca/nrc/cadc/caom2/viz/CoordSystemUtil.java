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

//import ca.nrc.cadc.association.viewer.DrawArea.CShape;

import java.awt.Component;
import java.awt.Container;
import java.awt.Graphics2D;
import java.awt.Shape;
import java.awt.geom.AffineTransform;
import java.awt.geom.GeneralPath;
import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.util.List;


/**
 * is used to set a coordinate system.
 * <p>Title: Coordinate system</p>
 * <p>Description: gives various static methods for setting a coordinate system</p>
 * <p>Copyright: Byrge Birkeland (c) 2003</p>
 * <p>Company: Agder University College</p>
 *
 * @author Byrge Birkeland
 * @version 1.0
 */

public class CoordSystemUtil {

    /**
     * CM is one centimeter on the screen
     */

    public static final double CM = 100.0 / 2.54;

    /**
     * sets a coordinate system with given units, origin and orientation.
     *
     * @param g      the Graphics2D context
     * @param unitx  the unit on the x axis in number of pixels
     * @param unity  the unit on the y axis in number of pixels
     * @param origx  the x coordinate of the origin measured from the upper
     *               left corner
     * @param origy  the y coordinate of the origin measured from the upper
     *               left corner
     * @param turned decides whether the y axis is turned upwards (turned=true)
     *               or downwards (turned=false).
     */

    public static void setCoordSystem(Graphics2D g,
                                      double unitx, double unity, double origx, double origy, boolean turned) {
        g.scale(unitx, unity);
        g.translate(origx, origy);
        if (turned) {
            g.scale(1, -1);
        }
    }

    /**
     * sets a coordinate system with given units and origin. The y axis is turned
     * upwards
     *
     * @param g     the Graphics2D context
     * @param unitx the unit on the x axis in number of pixels
     * @param unity the unit on the y axis in number of pixels
     * @param origx the x coordinate of the origin measured from the upper
     *              left corner
     * @param origy the y coordinate of the origin measured from the upper
     *              left corner
     */

    public static void setCoordSystem(Graphics2D g,
                                      double unitx, double unity, double origx, double origy) {
        setCoordSystem(g, unitx, unity, origx, origy, true);
    }

    /**
     * sets a coordinate system with given units, origin and orientation.
     *
     * @param g      the Graphics2D context
     * @param unit   the common unit on the axes in number of pixels
     * @param origx  the x coordinate of the origin measured from the upper
     *               left corner
     * @param origy  the y coordinate of the origin measured from the upper
     *               left corner
     * @param turned decides whether the y axis is turned upwards (turned=true)
     *               or downwards (turned=false).
     */


    public static void setCoordSystem(Graphics2D g,
                                      double unit, double origx, double origy, boolean turned) {
        g.scale(unit, unit);
        g.translate(origx, origy);
        if (turned) {
            g.scale(1, -1);
        }
    }

    /**
     * sets a coordinate system with given units and origin. The y axis is turned
     * upwards.
     *
     * @param g     the Graphics2D context
     * @param unit  the common unit on the axes in number of pixels
     * @param origx the x coordinate of the origin measured from the upper
     *              left corner
     * @param origy the y coordinate of the origin measured from the upper
     *              left corner
     */

    public static void setCoordSystem(Graphics2D g,
                                      double unit, double origx, double origy) {
        setCoordSystem(g, unit, origx, origy, true);
    }

    /**
     * sets the coordinate system wuth given units and origin in the lower left
     * corner of the screen.
     *
     * @param g     the Graphics2D context
     * @param comp  the component to give a coordinate system
     * @param unitx the unit on the x axis
     * @param unity the unit on the y axis
     */

    public static void setLowerLeftCoordSystem(Graphics2D g, Component comp,
                                               double unitx, double unity) {
        int dy = comp.getHeight();
        g.translate(0, dy);
        g.scale(unitx, -unity);
        //g.translate(1,1);
    }

    /**
     * sets the coordinate system with given unit and origin in the lower left
     * corner of the screen.
     *
     * @param g    the Graphics2D context
     * @param comp the component to give a coordinate system
     * @param unit the common unit on axes
     */

    public static void setLowerLeftCoordSystem(Graphics2D g, Component comp,
                                               double unit) {
        setLowerLeftCoordSystem(g, comp, unit, unit);
    }

    /**
     * sets the coordinate unit 1CM and origin in the lower left corner of the
     * screen.
     *
     * @param g    the Graphics2D context
     * @param comp the component to give a coordinate system
     */

    public static void setLowerLeftCoordSystem(Graphics2D g, Component comp) {
        setLowerLeftCoordSystem(g, comp, CM, CM);
    }

    /**
     * sets the coordinate system with given units and origin at the lower centre
     * corner of the component in question.
     *
     * @param g     the Graphics2D context
     * @param comp  the component to give a coordinate system
     * @param unitx the unit on the x axis on axes
     * @param unity the unit on the x axis on axes
     */

    public static void setLowerCenteredCoordSystem(Graphics2D g, Component comp,
                                                   double unitx, double unity) {
        int dy = comp.getHeight();
        int dx = comp.getWidth();
        g.translate(dx / 2, dy);
        g.scale(unitx, -unity);
        g.translate(0, 1);
    }

    /**
     * sets the coordinate system with given unit and origin at the upper left
     * corner of the component in question.
     *
     * @param g    the Graphics2D context
     * @param unit the common unit on axes
     */

    public static void setLowerCenteredCoordSystem(Graphics2D g,
                                                   double unit) {
        setUpperLeftCoordSystem(g, unit, unit);
    }

    /**
     * sets the coordinate system with given unit and origin at the lower centre
     * corner of the component in question.
     *
     * @param g    the Graphics2D context
     * @param comp the component to give a coordinate system
     * @param unit the common unit on axes
     */

    public static void setLowerCenteredCoordSystem(Graphics2D g, Component comp,
                                                   double unit) {
        setLowerCenteredCoordSystem(g, comp, unit, unit);
    }

    /**
     * sets the coordinate system with unit 1 CM and origin at the lower centre
     * corner of the component in question.
     *
     * @param g    the Graphics2D context
     * @param comp the component to give a coordinate system
     */

    public static void setLowerCenteredCoordSystem(Graphics2D g, Component comp) {
        setLowerCenteredCoordSystem(g, comp, CM, CM);
    }

    /**
     * sets the coordinate system with given units and origin at the upper left
     * corner of the component in question.
     *
     * @param g     the Graphics2D context
     * @param unitx the unit on the x axis on axes
     * @param unity the unit on the x axis on axes
     */

    public static void setUpperLeftCoordSystem(Graphics2D g, double unitx, double unity) {
        g.scale(unitx, unity);
    }

    /**
     * sets the coordinate system with unit 1 CM and origin at the upper left
     * corner of the component in question.
     *
     * @param g the Graphics2D context
     */

    public static void setUpperLeftCoordSystem(Graphics2D g) {
        g.scale(CM, CM);
    }

    /**
     * sets the coordinate system with given units and origin at the centre of
     * the component in question.
     *
     * @param g     the Graphics2D context
     * @param comp  the component to be given a coordinate system
     * @param unitx the unit on the x axis
     * @param unity the unit on the y axis
     */

    public static void setCenteredCoordSystem(Graphics2D g, Component comp,
                                              double unitx, double unity) {
        int dx = comp.getWidth();
        int dy = comp.getHeight();
        g.translate(dx / 2, dy / 2);
        g.scale(unitx, -unity);
    }

    /**
     * sets the coordinate system with given unit and origin at the centre of
     * the component in question.
     *
     * @param g    the Graphics2D context
     * @param comp the component to be given a coordinate system
     * @param unit the common unit on the axes
     */

    public static void setCenteredCoordSystem(Graphics2D g, Component comp,
                                              double unit) {
        int dx = comp.getWidth();
        int dy = comp.getHeight();
        g.translate(dx / 2, dy / 2);
        g.scale(unit, -unit);
    }

    /**
     * sets the coordinate system with unit 1 CM and origin at the centre of
     * the component in question.
     *
     * @param g    the Graphics2D context
     * @param comp the component to be given a coordinate system
     */

    public static void setCenteredCoordSystem(Graphics2D g, Component comp) {
        int dx = comp.getWidth();
        int dy = comp.getHeight();
        g.translate(dx / 2, dy / 2);
        g.scale(CM, -CM);
    }

    /**
     * sets a left centered coordinate system with given unit.
     *
     * @param g    the Graphics2D context
     * @param comp the component to be given a coordinate system
     * @param unit the common unit on the axes
     */

    public static void setLeftCenteredCoordSystem(Graphics2D g, Component comp,
                                                  double unit) {
        //int dx = comp.getWidth();
        int dy = comp.getHeight();
        g.translate(0, dy / 2);
        g.scale(unit, -unit);
        //g.translate(1,0);
    }

    /**
     * sets a left centered coordinate system with unit 1 cm.
     *
     * @param g    the Graphics2D context
     * @param comp the component to be given a coordinate system
     */

    public static void setLeftCenteredCoordSystem(Graphics2D g, Component comp) {
        //int dx = comp.getWidth();
        int dy = comp.getHeight();
        g.translate(0, dy / 2);
        g.scale(CM, -CM);
        //g.translate(1,0);
    }

    /**
     * constructs a grid covering a rectangular area on the screen.
     *
     * @param lowerX the lower boundary of the x interval
     * @param upperX the upper boundary of the x interval
     * @param numX the number of subintervals of the x interval
     * @param lowerY the lower boundary of the y interval
     * @param upperY the upper boundary of the y interval
     * @param numY the number of subintervals of the y interval
     * @return the GeneralPath object representing the grid
     */

    public static GeneralPath grid(
        double lowerX, double upperX, int numX, //x interval
        double lowerY, double upperY, int numY) { //y interval {
        double dx = (upperX - lowerX) / numX;
        double dy = (upperY - lowerY) / numY;
        double x = lowerX;
        double y = lowerY;
        GeneralPath gp = new GeneralPath();
        for (int i = 0; i <= numX; i++) {
            gp.moveTo((float) x, (float) lowerY);
            gp.lineTo((float) x, (float) upperY);
            x += dx;
        }
        for (int j = 0; j <= numY; j++) {
            gp.moveTo((float) lowerX, (float) y);
            gp.lineTo((float) upperX, (float) y);
            y += dy;
        }
        return gp;
    }

    /**
     * draws a grid covering a rectangular area on the screen.
     *
     * @param g  the Graphics2D context
     * @param lowerX the lower boundary of the x interval
     * @param upperX the upper boundary of the x interval
     * @param numX the number of subintervals of the x interval
     * @param lowerY the lower boundary of the y interval
     * @param upperY the upper boundary of the y interval
     * @param numY the number of subintervals of the y interval
     */

    public static void drawGrid(Graphics2D g,
                                double lowerX, double upperX, int numX, //x interval
                                double lowerY, double upperY, int numY) {
        GeneralPath gp = grid(lowerX, upperX, numX, lowerY, upperY, numY);
        g.draw(gp);
    }

    /**
     * draws a coordinate grid with one length unit distance in each direction.
     *
     * @param g    the Graphics2D context
     * @param comp the component to draw on
     */

    public static void drawIntegralCoordinateGrid(Graphics2D g, Component comp, int delta) {
        AffineTransform at = g.getTransform();
        AffineTransform bt = new AffineTransform();
        try {
            bt = at.createInverse();
        } catch (Exception e) {
            System.out.println("Non-invertible transform");
        }

        Container c = comp.getParent();
        //int xx=comp.getX(),yy=comp.getY(),Dx=comp.getWidth(), Dy=comp.getHeight();
        int xx = c.getX();
        int yy = c.getY();
        int dx = c.getWidth();
        int dy = c.getHeight();
        Point2D.Double[] p = new Point2D.Double[2];
        Point2D.Double[] q = new Point2D.Double[2];
        p[0] = new Point2D.Double(xx, yy);
        p[1] = new Point2D.Double(xx + dx, yy + dy);
        for (int i = 0; i < 2; i++) {
            q[i] = new Point2D.Double();
            bt.transform(p[i], q[i]);
        }
        double lowerX = q[0].x;
        double upperX = q[1].x;
        double y0 = q[0].y;
        double y1 = q[1].y;
        GeneralPath gp = new GeneralPath();
        double x;
        double y;
        x = Math.ceil(lowerX);
        while (x < upperX) {
            gp.moveTo((float) x, (float) y0);
            gp.lineTo((float) x, (float) y1);
            x++;
        }
        double lowerY = y0 < y1 ? y0 : y1;
        double upperY = y0 < y1 ? y1 : y0;
        y = Math.ceil(lowerY);
        while (y < upperY) {
            gp.moveTo((float) lowerX, (float) y);
            gp.lineTo((float) upperX, (float) y);
            y++;
        }
        g.draw(gp);

        //g.setColor(Color.lightGray);
        gp.reset();
        x = Math.ceil(lowerX);
        while (x < upperX) {
            if (x % delta == 0) {
                gp.moveTo((float) x, (float) y0);
                gp.lineTo((float) x, (float) y1);
            }
            x++;
        }
        y = Math.ceil(lowerY);
        while (y < upperY) {
            if (y % delta == 0) {
                gp.moveTo((float) lowerX, (float) y);
                gp.lineTo((float) upperX, (float) y);
            }
            y++;
        }
        g.draw(gp);

    }

    /**
     * draws a pair of coordinate axes through the origin.
     *
     * @param g    the Graphics2D context
     * @param comp the component to draw on
     */

    public static void drawCoordinateAxes(Graphics2D g, Component comp) {
        AffineTransform at = g.getTransform();
        AffineTransform bt = new AffineTransform();
        try {
            bt = at.createInverse();
        } catch (Exception e) {
            System.out.println("Non-invertible transform");
        }
        Container c = comp.getParent();
        int x = c.getX();
        int y = c.getY();
        int dx = c.getWidth();
        int dy = c.getHeight();
        //int x=comp.getX(),y=comp.getY(),Dx=comp.getWidth(), Dy=comp.getHeight();
        Point2D.Double[] p = new Point2D.Double[2];
        Point2D.Double[] q = new Point2D.Double[2];
        p[0] = new Point2D.Double(x, y);
        p[1] = new Point2D.Double(x + dx, y + dy);
        for (int i = 0; i < 2; i++) {
            q[i] = new Point2D.Double();
            bt.transform(p[i], q[i]);
        }
        double lowerX = q[0].x;
        double upperX = q[1].x;
        double lowerY = q[0].y;
        double upperY = q[1].y;
        //y0=Q[0].y, y1=Q[1].y,yL=y0<y1?y0:y1,yH=y0<y1?y1:y0;
        GeneralPath gp = new GeneralPath();
        gp.moveTo((float) lowerX, 0f);
        gp.lineTo((float) upperX, 0f);
        gp.moveTo(0f, (float) lowerY);
        gp.lineTo(0f, (float) upperY);
        g.draw(gp);
    }

    /**
     * sets a coordinate system such that the rectangle [xL,xH] x [yL,yH] fits
     * exactly into the component in question.
     *
     * @param g    the Graphics2D context
     * @param comp the component in question
     * @param lowerX   the lower boundary of the x interval
     * @param upperX   the upper boundary of the x interval
     * @param lowerY   the lower boundary of the y interval
     * @param upperY   the upper boundary of the y interval
     */

    public static void setFittedCoordSystem(Graphics2D g, Component comp,
                                            double lowerX, double upperX, double lowerY, double upperY) {
        int compX = comp.getX();
        //int yG = comp.getY();
        int dxG = comp.getWidth();
        int dyG = comp.getHeight();
        double a = (upperX - lowerX) / dxG;
        double b = (lowerY - upperY) / dyG;
        double c = lowerX - a * compX;
        double f = upperY;
        //F=yH-E*yG;
        AffineTransform at = new AffineTransform(a, 0, 0, b, c, f);
        AffineTransform bt = new AffineTransform();
        try {
            bt = at.createInverse();
        } catch (Exception e) {
            // silently fail
        }
        g.setTransform(bt);
    }

    /**
     * sets a coordinate system such that the rectangle [xL,xH] x [yL,yH] fits
     * into the component in question, but with some added space outside of the
     * rectangle.
     *
     * @param g    the Graphics2D context
     * @param comp the component in question
     * @param lowerX   the lower boundary of the x interval
     * @param upperX   the upper boundary of the x interval
     * @param lowerY   the lower boundary of the y interval
     * @param upperY   the upper boundary of the y interval
     * @param r    the distance from the rectangle [xL,xH] x [yL,yH] to the border of
     *             the component
     */

    public static void setFittedCoordSystem(Graphics2D g, Component comp,
                                            double lowerX, double upperX, double lowerY, double upperY, double r) {
        double xxL = lowerX - r;
        double xxH = upperX + r;
        double yyL = lowerY - r;
        double yyH = upperY + r;
        setFittedCoordSystem(g, comp, xxL, xxH, yyL, yyH);
    }

    /**
     * set a coordinate system for the component such that a given Rectangle2D will
     * fit exactly into the component.
     *
     * @param g    the Graphics2D context
     * @param comp the component in question
     * @param rek  the Rectangle2D object to fit into the component
     */

    public static void setFittedCoordSystem(Graphics2D g, Component comp,
                                            Rectangle2D rek) {
        double lowerX = rek.getX();
        double upperX = lowerX + rek.getWidth();
        double lowerY = rek.getY();
        double upperY = lowerY + rek.getHeight();
        setFittedCoordSystem(g, comp, lowerX, upperX, lowerY, upperY);
    }

    /**
     * set a coordinate system for the component such that a given Rectangle2D will
     * fit into the component, but such that there will be som distance from the
     * rectangle to the borders of the component.
     *
     * @param g    the Graphics2D context
     * @param comp the component in question
     * @param rek  the Rectangle2D object to fit into the component
     * @param r    the distance from rectangle to the border of the component
     */

    public static void setFittedCoordSystem(Graphics2D g, Component comp,
                                            Rectangle2D rek, double r) {
        Rectangle2D re = new Rectangle2D.Double(rek.getX() - r, rek.getY() - r,
            rek.getWidth() + 2 * r, rek.getHeight() + 2 * r);
        setFittedCoordSystem(g, comp, re);
    }

    /**
     * sets a coordinate system for the component such that a given Shape argument
     * will fit exactly into the component.
     *
     * @param g    the Graphics2D context
     * @param comp the component in question
     * @param sh   the shape to be fitted into the coordinate system
     */

    public static void setFittedCoordSystem(Graphics2D g, Component comp,
                                            Shape sh) {
        Rectangle2D rek = sh.getBounds2D();
        setFittedCoordSystem(g, comp, rek);
    }

    /**
     * sets a coordinate system for the component such that a given Shape argument
     * will fit into the component, but such that there will be a some white space
     * ouside of the shape.
     *
     * @param g    the Graphics2D context
     * @param comp the component in question
     * @param sh   the shape to be fitted into the coordinate system
     * @param r    the distance from the bounding rectangle for the shape to the
     *             border of the component
     */

    public static void setFittedCoordSystem(Graphics2D g, Component comp,
                                            Shape sh, double r) {
        Rectangle2D rek = sh.getBounds2D();
        setFittedCoordSystem(g, comp, rek, r);
    }

    public static void setFittedCoordSystem(Graphics2D g, Component comp,
                                            Shape[] sh) {
        Rectangle2D rek = getBounds2D(sh);
        setFittedCoordSystem(g, comp, rek);
    }

    /**
     * sets a coordinate system for the component such that a given Shape[] array
     * fits into the component but with some extra space.
     *
     * @param g    the Graphics2D context
     * @param comp the component
     * @param sh   the array of shapes
     * @param r    the distance from the bounding rectangle of the shapes to the
     *             border of the component
     */

    public static void setFittedCoordSystem(Graphics2D g, Component comp,
                                            Shape[] sh, double r) {
        Rectangle2D rek = getBounds2D(sh);
        setFittedCoordSystem(g, comp, rek, r);
    }

    /**
     * finds the least Rectangle2D that contains a given array of Shape objects.
     *
     * @param sh a Shape[] array
     * @return the leat Rectangle2D contating all the shapes
     */

    public static Rectangle2D getBounds2D(Shape[] sh) {
        int n = sh.length;
        //Rectangle2D[] rek=new Rectangle2D[N];
        Rectangle2D bigrek = sh[0].getBounds2D();
        if (n > 1) {
            for (int i = 1; i < n; i++) {
                bigrek.add(sh[i].getBounds2D());
            }
        }
        return bigrek;
    }

    /**
     * @param shapes
     * @return
     */

    public static Rectangle2D getBounds2D(List shapes) {
        int n = shapes.size();
        //Rectangle2D[] rek=new Rectangle2D[N];
        Rectangle2D bigrek = ((DrawArea.CShape) shapes.get(0)).shape.getBounds2D();
        if (n > 1) {
            for (int i = 1; i < n; i++) {
                bigrek.add(((DrawArea.CShape) shapes.get(i)).shape.getBounds2D());
            }
        }
        return bigrek;
    }

    /**
     * sets a coordinate system for the component such that a given Shape[] array
     * fits exactly into the component.
     *
     * @param g    the Graphics2D context
     * @param comp the component
     * @param sh   the array of shapes
     */

    /**
     * set a coordinate system for the component such that a given Rectangle2D will
     * fit exactly into the component, keeping the same unit on both axes.
     *
     * @param g    the Graphics2D context
     * @param comp the component in question
     * @param rek  the Rectangle2D object to fit into the component
     */

    public static void setFittedEvenCoordSystem(Graphics2D g, Component comp,
                                                Rectangle2D rek) {
        double x = rek.getX();
        double y = rek.getY();
        double deltax = rek.getWidth();
        double deltay = rek.getHeight();
        //int xW = comp.getX();
        //int yW = comp.getY();
        int dxW = comp.getWidth();
        int dyW = comp.getHeight();
        double kkW = (1.0 * dyW) / dxW;
        double kkG = deltay / deltax;
        //double k = kW/kG;
        Rectangle2D re = new Rectangle2D.Double();
        if (kkW > kkG) {
            re.setRect(x, y, deltax, kkW * deltax);
        } else {
            re.setRect(x, y, deltay / kkW, deltay);
        }
        setFittedCoordSystem(g, comp, re);
    }

    /**
     * sets a coordinate system for the component such that a given Rectangle2D will
     * fit into the component, but such that there will be some distance from the
     * rectangle to the borders of the component. Furthermore, the units on the
     * two coordinate axes are the same.
     *
     * @param g    the Graphics2D context
     * @param comp the component in question
     * @param rek  the Rectangle2D object to fit into the component
     * @param r    the distance from rectangle to the border of the component
     */

    public static void setFittedEvenCoordSystem(Graphics2D g, Component comp,
                                                Rectangle2D rek, double r) {
        Rectangle2D re = new Rectangle2D.Double(rek.getX() - r, rek.getY() - r,
            rek.getWidth() + 2 * r, rek.getHeight() + 2 * r);
        setFittedEvenCoordSystem(g, comp, re);
    }


    /**
     * @param g
     * @param comp
     * @param sh
     */
    public static void setFittedEvenCoordSystem(Graphics2D g, Component comp, Shape sh) {
        Rectangle2D rek = sh.getBounds2D();
        setFittedEvenCoordSystem(g, comp, rek);
    }

    /**
     * sets a coordinate system for the component such that a given Shape argument
     * will fit into the component, but such that there will be a some white space
     * ouside of the shape. Furthermore, the units on the
     * two coordinate axes are the same.
     *
     * @param g    the Graphics2D context
     * @param comp the component in question
     * @param sh   the shape to be fitted into the coordinate system
     * @param r    the distance from the bounding rectangle for the shape to the
     *             border of the component
     */

    public static void setFittedEvenCoordSystem(Graphics2D g, Component comp, Shape sh, double r) {
        Rectangle2D rek = sh.getBounds2D();
        setFittedEvenCoordSystem(g, comp, rek, r);
    }

    /**
     * set a coordinate system for the component such that a given Shape[] array will
     * fit exactly into the component, keeping the same unit on both axes.
     *
     * @param g    the Graphics2D context
     * @param comp the component in question
     * @param sh   the Shape[] array object to fit into the component
     */

    public static void setFittedEvenCoordSystem(Graphics2D g, Component comp, Shape[] sh) {
        Rectangle2D rek = getBounds2D(sh);
        setFittedEvenCoordSystem(g, comp, rek);
    }

    /**
     * set a coordinate system for the component such that a given Shape[] array will
     * fit into the component, keeping the same unit on both axes, but such that there
     * is a distance between the bounding rectangle of the Shape array and the border
     * of the component.
     *
     * @param g    the Graphics2D context
     * @param comp the component in question
     * @param sh   the Shape[] array object to fit into the component
     * @param r    the distance between the bounding rectangle of the Shape array and
     *             the border of the component
     */

    public static void setFittedEvenCoordSystem(Graphics2D g, Component comp,
                                                Shape[] sh, double r) {
        Rectangle2D rek = getBounds2D(sh);
        setFittedEvenCoordSystem(g, comp, rek, r);
    }

}

