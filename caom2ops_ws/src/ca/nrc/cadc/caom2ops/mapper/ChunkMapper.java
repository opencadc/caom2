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

package ca.nrc.cadc.caom2ops.mapper;

import ca.nrc.cadc.caom2.Chunk;
import ca.nrc.cadc.caom2.EnergyTransition;
import ca.nrc.cadc.caom2.ProductType;
import ca.nrc.cadc.caom2.wcs.Axis;
import ca.nrc.cadc.caom2.wcs.Coord2D;
import ca.nrc.cadc.caom2.wcs.CoordAxis1D;
import ca.nrc.cadc.caom2.wcs.CoordAxis2D;
import ca.nrc.cadc.caom2.wcs.CoordBounds1D;
import ca.nrc.cadc.caom2.wcs.CoordBounds2D;
import ca.nrc.cadc.caom2.wcs.CoordError;
import ca.nrc.cadc.caom2.wcs.CoordFunction1D;
import ca.nrc.cadc.caom2.wcs.CoordFunction2D;
import ca.nrc.cadc.caom2.wcs.CoordRange1D;
import ca.nrc.cadc.caom2.wcs.CoordRange2D;
import ca.nrc.cadc.caom2.wcs.Dimension2D;
import ca.nrc.cadc.caom2.wcs.ObservableAxis;
import ca.nrc.cadc.caom2.wcs.PolarizationWCS;
import ca.nrc.cadc.caom2.wcs.RefCoord;
import ca.nrc.cadc.caom2.wcs.Slice;
import ca.nrc.cadc.caom2.wcs.SpatialWCS;
import ca.nrc.cadc.caom2.wcs.SpectralWCS;
import ca.nrc.cadc.caom2.wcs.TemporalWCS;
import ca.nrc.cadc.caom2ops.Util;
import java.text.DateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.log4j.Logger;

/**
*
* @author pdowler
*/
public class ChunkMapper implements VOTableRowMapper<Chunk>
{
    private static final Logger log = Logger.getLogger(ChunkMapper.class);
    
    private Map<String,Integer> map;

    public ChunkMapper(Map<String,Integer> map)
    {
            this.map = map;
    }
    public Chunk mapRow(List<Object> data, DateFormat dateFormat)
    {
        log.debug("mapping Chunk");
            UUID id = Util.getUUID(data, map.get("caom2:Chunk.id"));
        if (id == null)
            return null;

        Chunk c = new Chunk();

        String pt = Util.getString(data, map.get("caom2:Chunk.productType"));
        log.debug("found c.productType = " + pt);
        if (pt != null)
            c.productType = ProductType.toValue(pt);

        c.naxis = Util.getInteger(data, map.get("caom2:Chunk.naxis"));
        c.positionAxis1 = Util.getInteger(data, map.get("caom2:Chunk.positionAxis1"));
        c.positionAxis2 = Util.getInteger(data, map.get("caom2:Chunk.positionAxis2"));
        c.energyAxis = Util.getInteger(data, map.get("caom2:Chunk.energyAxis"));
        c.timeAxis = Util.getInteger(data, map.get("caom2:Chunk.timeAxis"));
        c.polarizationAxis = Util.getInteger(data, map.get("caom2:Chunk.polarizationAxis"));
        c.observableAxis = Util.getInteger(data, map.get("caom2:Chunk.observableAxis"));

        // position
        String posctype1 = Util.getString(data, map.get("caom2:Chunk.position.axis.axis1.ctype"));
        String poscunit1 = Util.getString(data, map.get("caom2:Chunk.position.axis.axis1.cunit"));
        String posctype2 = Util.getString(data, map.get("caom2:Chunk.position.axis.axis2.ctype"));
        String poscunit2 = Util.getString(data, map.get("caom2:Chunk.position.axis.axis2.cunit"));
        Double e1s = Util.getDouble(data, map.get("caom2:Chunk.position.axis.error1.syser"));
        Double e1r = Util.getDouble(data, map.get("caom2:Chunk.position.axis.error1.rnder"));
        Double e2s = Util.getDouble(data, map.get("caom2:Chunk.position.axis.error2.syser"));
        Double e2r = Util.getDouble(data, map.get("caom2:Chunk.position.axis.error2.rnder"));
        CoordRange2D posrange = null; // Util.decodeCoordRange2D( Util.getString(data, map.get(CHUNK_POS_AXIS_RANGE)) );
        Double start1pix = Util.getDouble(data, map.get("caom2:Chunk.position.axis.range.start.coord1.pix"));
        Double start1val = Util.getDouble(data, map.get("caom2:Chunk.position.axis.range.start.coord1.val"));
        Double start2pix = Util.getDouble(data, map.get("caom2:Chunk.position.axis.range.start.coord2.pix"));
        Double start2val = Util.getDouble(data, map.get("caom2:Chunk.position.axis.range.start.coord2.val"));
        Double end1pix = Util.getDouble(data, map.get("caom2:Chunk.position.axis.range.end.coord1.pix"));
        Double end1val = Util.getDouble(data, map.get("caom2:Chunk.position.axis.range.end.coord1.val"));
        Double end2pix = Util.getDouble(data, map.get("caom2:Chunk.position.axis.range.end.coord2.pix"));
        Double end2val = Util.getDouble(data, map.get("caom2:Chunk.position.axis.range.end.coord2.val"));
        if (start1pix != null)
            posrange = new CoordRange2D(
                    new Coord2D(new RefCoord(start1pix, start1val), new RefCoord(start2pix, start2val)),
                    new Coord2D(new RefCoord(end1pix, end1val), new RefCoord(end2pix, end2val)));

        CoordBounds2D posbounds = Util.decodeCoordBounds2D( Util.getString(data, map.get("caom2:Chunk.position.axis.bounds")) );

        CoordFunction2D posfunction = null; // Util.decodeCoordFunction2D( Util.getString(data, map.get(CHUNK_POS_AXIS_FUNCTION)) );
        Long naxis1 = Util.getLong(data, map.get("caom2:Chunk.position.axis.function.dimension.naxis1"));
        Long naxis2 = Util.getLong(data, map.get("caom2:Chunk.position.axis.function.dimension.naxis2"));
        Double c1pix = Util.getDouble(data, map.get("caom2:Chunk.position.axis.function.refCoord.coord1.pix"));
        Double c1val = Util.getDouble(data, map.get("caom2:Chunk.position.axis.function.refCoord.coord1.val"));
        Double c2pix = Util.getDouble(data, map.get("caom2:Chunk.position.axis.function.refCoord.coord2.pix"));
        Double c2val = Util.getDouble(data, map.get("caom2:Chunk.position.axis.function.refCoord.coord2.val"));
        Double cd11 = Util.getDouble(data, map.get("caom2:Chunk.position.axis.function.cd11"));
        Double cd12 = Util.getDouble(data, map.get("caom2:Chunk.position.axis.function.cd12"));
        Double cd21 = Util.getDouble(data, map.get("caom2:Chunk.position.axis.function.cd21"));
        Double cd22 = Util.getDouble(data, map.get("caom2:Chunk.position.axis.function.cd22"));
        if (naxis1 != null)
            posfunction = new CoordFunction2D(new Dimension2D(naxis1, naxis2),
                    new Coord2D(new RefCoord(c1pix, c1val), new RefCoord(c2pix, c2val)),
                    cd11, cd12, cd21, cd22);

        String coordsys = Util.getString(data, map.get("caom2:Chunk.position.coordsys"));
        Double equinox = Util.getDouble(data, map.get("caom2:Chunk.position.equinox"));
        Double posres = Util.getDouble(data, map.get("caom2:Chunk.position.resolution"));
        if (posctype1 != null)
        {
            CoordAxis2D axis = new CoordAxis2D(new Axis(posctype1, poscunit1), new Axis(posctype2, poscunit2));
            if (e1s != null || e1r != null)
                axis.error1 = new CoordError(e1s, e1r);
            if (e2s != null || e2r != null)
                axis.error2 = new CoordError(e2s, e2r);
            axis.range = posrange;
            axis.bounds = posbounds;
            axis.function = posfunction;
            c.position = new SpatialWCS(axis);
            c.position.coordsys = coordsys;
            c.position.equinox = equinox;
            c.position.resolution = posres;
        }

        // energy
        String enctype = Util.getString(data, map.get("caom2:Chunk.energy.axis.axis.ctype"));
        String encunit = Util.getString(data, map.get("caom2:Chunk.energy.axis.axis.cunit"));
        Double enes = Util.getDouble(data, map.get("caom2:Chunk.energy.axis.error.syser"));
        Double ener = Util.getDouble(data, map.get("caom2:Chunk.energy.axis.error.rnder"));
        CoordRange1D enrange = null; // Util.decodeCoordRange1D( Util.getString(data, map.get("caom2:Chunk.energy.axis.range")) );
        Double pix1 = Util.getDouble(data, map.get("caom2:Chunk.energy.axis.range.start.pix"));
        Double val1 = Util.getDouble(data, map.get("caom2:Chunk.energy.axis.range.start.val"));
        Double pix2 = Util.getDouble(data, map.get("caom2:Chunk.energy.axis.range.end.pix"));
        Double val2 = Util.getDouble(data, map.get("caom2:Chunk.energy.axis.range.end.val"));
        if (pix1 != null)
            enrange = new CoordRange1D(new RefCoord(pix1, val1), new RefCoord(pix2, val2));

        CoordBounds1D enbounds = Util.decodeCoordBounds1D( Util.getString(data, map.get("caom2:Chunk.energy.axis.bounds")) );
        CoordFunction1D enfunction = null; // Util.decodeCoordFunction1D( Util.getString(data, map.get("caom2:Chunk.energy.axis.function")) );
        Long naxis = Util.getLong(data, map.get("caom2:Chunk.energy.axis.function.naxis"));
        Double pix = Util.getDouble(data, map.get("caom2:Chunk.energy.axis.function.refCoord.pix"));
        Double val = Util.getDouble(data, map.get("caom2:Chunk.energy.axis.function.refCoord.val"));
        Double delta = Util.getDouble(data, map.get("caom2:Chunk.energy.axis.function.delta"));
        if (naxis != null)
            enfunction = new CoordFunction1D(naxis, delta, new RefCoord(pix, val));

        String specsys = Util.getString(data, map.get("caom2:Chunk.energy.specsys"));
        String ssysobs = Util.getString(data, map.get("caom2:Chunk.energy.ssysobs"));
        String ssyssrc = Util.getString(data, map.get("caom2:Chunk.energy.ssyssrc"));
        Double restfrq = Util.getDouble(data, map.get("caom2:Chunk.energy.restfrq"));
        Double restwav = Util.getDouble(data, map.get("caom2:Chunk.energy.restwav"));
        Double velosys = Util.getDouble(data, map.get("caom2:Chunk.energy.velosys"));
        Double zsource = Util.getDouble(data, map.get("caom2:Chunk.energy.zsource"));
        Double velang = Util.getDouble(data, map.get("caom2:Chunk.energy.velang"));
        String bandpassName = Util.getString(data, map.get("caom2:Chunk.energy.bandpassName"));
        Double enres = Util.getDouble(data, map.get("caom2:Chunk.energy.resolvingPower"));
        String species = Util.getString(data, map.get("caom2:Chunk.energy.transition.species"));
        String trans = Util.getString(data, map.get("caom2:Chunk.energy.transition.transition"));
        if (enctype != null)
        {
            CoordAxis1D axis = new CoordAxis1D(new Axis(enctype, encunit));
            if (enes != null || ener != null)
                axis.error = new CoordError(enes, ener);
            axis.range = enrange;
            axis.bounds = enbounds;
            axis.function = enfunction;
            c.energy = new SpectralWCS(axis, specsys);
            c.energy.ssysobs = ssysobs;
            c.energy.ssyssrc = ssyssrc;
            c.energy.restfrq = restfrq;
            c.energy.restwav = restwav;
            c.energy.velosys = velosys;
            c.energy.zsource = zsource;
            c.energy.velang = velang;
            c.energy.bandpassName = bandpassName;
            c.energy.resolvingPower = enres;
            if (species != null)
                c.energy.transition = new EnergyTransition(species, trans);
        }

        // time
        
        String tctype = Util.getString(data, map.get("caom2:Chunk.time.axis.axis.ctype"));
        String tcunit = Util.getString(data, map.get("caom2:Chunk.time.axis.axis.cunit"));
        Double tes = Util.getDouble(data, map.get("caom2:Chunk.time.axis.error.syser"));
        Double ter = Util.getDouble(data, map.get("caom2:Chunk.time.axis.error.rnder"));
        CoordRange1D trange = null; // Util.decodeCoordRange1D( Util.getString(data, map.get("caom2:Chunk.time.axis.range")) );
        pix1 = Util.getDouble(data, map.get("caom2:Chunk.time.axis.range.start.pix"));
        val1 = Util.getDouble(data, map.get("caom2:Chunk.time.axis.range.start.val"));
        pix2 = Util.getDouble(data, map.get("caom2:Chunk.time.axis.range.end.pix"));
        val2 = Util.getDouble(data, map.get("caom2:Chunk.time.axis.range.end.val"));
        if (pix1 != null)
            trange = new CoordRange1D(new RefCoord(pix1, val1), new RefCoord(pix2, val2));

        CoordBounds1D tbounds = Util.decodeCoordBounds1D( Util.getString(data, map.get("caom2:Chunk.time.axis.bounds")) );
        CoordFunction1D tfunction = null; // Util.decodeCoordFunction1D( Util.getString(data, map.get("caom2:Chunk.time.axis.function")) );
        naxis = Util.getLong(data, map.get("caom2:Chunk.time.axis.function.naxis"));
        pix = Util.getDouble(data, map.get("caom2:Chunk.time.axis.function.refCoord.pix"));
        val = Util.getDouble(data, map.get("caom2:Chunk.time.axis.function.refCoord.val"));
        delta = Util.getDouble(data, map.get("caom2:Chunk.time.axis.function.delta"));
        if (naxis != null)
            tfunction = new CoordFunction1D(naxis, delta, new RefCoord(pix, val));

        String timesys = Util.getString(data, map.get("caom2:Chunk.time.timesys"));
        String trefpos = Util.getString(data, map.get("caom2:Chunk.time.trepos"));
        Double mjdref = Util.getDouble(data, map.get("caom2:Chunk.time.mjdref"));
        Double exposure = Util.getDouble(data, map.get("caom2:Chunk.time.exposure"));
        Double tres = Util.getDouble(data, map.get("caom2:Chunk.time.resolution"));
        if (tctype != null)
        {
            CoordAxis1D axis = new CoordAxis1D(new Axis(tctype, tcunit));
            if (tes != null || ter != null)
                axis.error = new CoordError(tes, ter);
            axis.range = trange;
            axis.bounds = tbounds;
            axis.function = tfunction;
            c.time = new TemporalWCS(axis);
            c.time.timesys = timesys;
            c.time.trefpos = trefpos;
            c.time.mjdref = mjdref;
            c.time.exposure = exposure;
            c.time.resolution = tres;
        }

        // polarization
        String pctype = Util.getString(data, map.get("caom2:Chunk.polarization.axis.axis.ctype"));
        String pcunit = Util.getString(data, map.get("caom2:Chunk.polarization.axis.axis.cunit"));
        Double pes = Util.getDouble(data, map.get("caom2:Chunk.polarization.axis.error.syser"));
        Double per = Util.getDouble(data, map.get("caom2:Chunk.polarization.axis.error.rnder"));
        CoordRange1D prange = null; // Util.decodeCoordRange1D( Util.getString(data, map.get("caom2:Chunk.polarization.range")) );
        pix1 = Util.getDouble(data, map.get("caom2:Chunk.polarization.axis.range.start.pix"));
        val1 = Util.getDouble(data, map.get("caom2:Chunk.polarization.axis.range.start.val"));
        pix2 = Util.getDouble(data, map.get("caom2:Chunk.polarization.axis.range.end.pix"));
        val2 = Util.getDouble(data, map.get("caom2:Chunk.polarization.axis.range.end.val"));
        if (pix1 != null)
            prange = new CoordRange1D(new RefCoord(pix1, val1), new RefCoord(pix2, val2));

        CoordBounds1D pbounds = Util.decodeCoordBounds1D( Util.getString(data, map.get("caom2:Chunk.polarization.axis.bounds")) );
        CoordFunction1D pfunction = null; // Util.decodeCoordFunction1D( Util.getString(data, map.get("caom2:Chunk.polarization.function")) );
        naxis = Util.getLong(data, map.get("caom2:Chunk.polarization.axis.function.naxis"));
        pix = Util.getDouble(data, map.get("caom2:Chunk.polarization.axis.function.refCoord.pix"));
        val = Util.getDouble(data, map.get("caom2:Chunk.polarization.axis.function.refCoord.val"));
        delta = Util.getDouble(data, map.get("caom2:Chunk.polarization.axis.function.delta"));
        if (naxis != null)
            pfunction = new CoordFunction1D(naxis, delta, new RefCoord(pix, val));

        if (pctype != null)
        {
            CoordAxis1D axis = new CoordAxis1D(new Axis(pctype, pcunit));
            if (pes != null || per != null)
                axis.error = new CoordError(pes, per);
            axis.range = prange;
            axis.bounds = pbounds;
            axis.function = pfunction;
            c.polarization = new PolarizationWCS(axis);
        }

        // observable
        String oda = Util.getString(data, map.get("caom2:Chunk.observable.dependent.axis.ctype"));
        String odu = Util.getString(data, map.get("caom2:Chunk.observable.dependent.axis.cunit"));
        Long odb = Util.getLong(data, map.get("caom2:Chunk.observable.dependent.bin"));
        String oia = Util.getString(data, map.get("caom2:Chunk.observable.independent.axis.ctype"));
        String oiu = Util.getString(data, map.get("caom2:Chunk.observable.independent.axis.cunit"));
        Long oib = Util.getLong(data, map.get("caom2:Chunk.observable.independent.bin"));
        if (oda != null)
        {
            Slice dep = new Slice(new Axis(oda, odu), odb);
            c.observable = new ObservableAxis(dep);
            if (oia != null)
                c.observable.independent = new Slice(new Axis(oia, oiu), oib);
        }

        Date lastModified = Util.getDate(data, map.get("caom2:Chunk.lastModified"));
        log.debug("found: chunk.lastModified = " + lastModified);
        Date maxLastModified = Util.getDate(data, map.get("caom2:Chunk.maxLastModified"));
        log.debug("found: chunk.maxLastModified = " + maxLastModified);

        Util.assignID(c, id);
        Util.assignLastModified(c, lastModified, "lastModified");
        Util.assignLastModified(c, maxLastModified, "maxLastModified");

        return c;
    }

}


