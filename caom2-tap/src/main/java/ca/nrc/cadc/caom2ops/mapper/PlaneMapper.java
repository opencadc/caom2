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

import ca.nrc.cadc.caom2.CalibrationLevel;
import ca.nrc.cadc.caom2.DataProductType;
import ca.nrc.cadc.caom2.DataQuality;
import ca.nrc.cadc.caom2.Energy;
import ca.nrc.cadc.caom2.EnergyBand;
import ca.nrc.cadc.caom2.EnergyTransition;
import ca.nrc.cadc.caom2.Metrics;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.Polarization;
import ca.nrc.cadc.caom2.PolarizationState;
import ca.nrc.cadc.caom2.Position;
import ca.nrc.cadc.caom2.Provenance;
import ca.nrc.cadc.caom2.Quality;
import ca.nrc.cadc.caom2.Time;
import ca.nrc.cadc.caom2.types.Interval;
import ca.nrc.cadc.caom2.types.Polygon;
import ca.nrc.cadc.caom2.types.SegmentType;
import ca.nrc.cadc.caom2.types.SubInterval;
import ca.nrc.cadc.caom2.types.Vertex;
import ca.nrc.cadc.caom2.wcs.Dimension2D;
import ca.nrc.cadc.caom2ops.Util;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
public class PlaneMapper implements VOTableRowMapper<Plane>
{
    private static final Logger log = Logger.getLogger(PlaneMapper.class);
    
    private Map<String,Integer> map;

    public PlaneMapper(Map<String,Integer> map)
    {
            this.map = map;
    }
    
    public Plane mapRow(List<Object> data, DateFormat dateFormat)
    {
        log.debug("mapping Plane");
        UUID id = Util.getUUID(data, map.get("caom2:Plane.id"));
        if (id == null)
            return null;
        
        try
        {
            String productID = Util.getString(data, map.get("caom2:Plane.productID"));
            
            Plane plane = new Plane(productID);
            
            Integer cal = Util.getInteger(data, map.get("caom2:Plane.calibrationLevel"));
            if (cal != null)
                plane.calibrationLevel = CalibrationLevel.toValue(cal);
            
            String dpType = Util.getString(data, map.get("caom2:Plane.dataProductType"));
            if (dpType != null)
                plane.dataProductType = DataProductType.toValue(dpType);
            
            plane.dataRelease = Util.getDate(data, map.get("caom2:Plane.dataRelease"));
            plane.metaRelease = Util.getDate(data, map.get("caom2:Plane.metaRelease"));
            
            plane.creatorID = Util.getURI(data, map.get("caom2:Plane.creatorID"));
                
            ca.nrc.cadc.stc.Polygon posBounds = (ca.nrc.cadc.stc.Polygon) Util.getObject(data, map.get("caom2:Plane.position.bounds"));
            if (posBounds != null)
            {
                plane.position = new Position();
                Polygon poly = new Polygon();
                SegmentType t = SegmentType.MOVE;
                for (int i=0; i<posBounds.getCoordPairs().size(); i++)
                {
                    ca.nrc.cadc.stc.CoordPair c = posBounds.getCoordPairs().get(i);
                    Vertex v = new Vertex(c.getX(), c.getY(), t);
                    t = SegmentType.LINE; // subsequent vertices
                    poly.getVertices().add(v);
                }
                poly.getVertices().add(Vertex.CLOSE);
                plane.position.bounds = poly;
                
                Long dim1 = Util.getLong(data, map.get("caom2:Plane.position.dimension.naxis1"));
                Long dim2 = Util.getLong(data, map.get("caom2:Plane.position.dimension.naxis2"));
                if (dim1 != null && dim2 != null)
                    plane.position.dimension = new Dimension2D(dim1, dim2);
                plane.position.resolution = Util.getDouble(data, map.get("caom2:Plane.position.resolution"));
                plane.position.sampleSize = Util.getDouble(data, map.get("caom2:Plane.position.sampleSize"));
                plane.position.timeDependent = Util.getBoolean(data, map.get("caom2:Plane.position.timeDependent"));
            }
            
            Double nrgBounds1 = Util.getDouble(data, map.get("caom2:Plane.energy.bounds.lower"));
            Double nrgBounds2 = Util.getDouble(data, map.get("caom2:Plane.energy.bounds.upper"));
            List<Double> eSampleVals = Util.getDoubleList(data, map.get("caom2:Plane.energy.bounds"));
            if (nrgBounds1 != null && nrgBounds2 != null)
            {
                plane.energy = new Energy();
                plane.energy.bounds = new Interval(nrgBounds1, nrgBounds2);
                if (eSampleVals != null && eSampleVals.size() > 2) // actual sub-samples
                {
                    for (int i=0; i<eSampleVals.size(); i++)
                    {
                        plane.energy.bounds.getSamples().add(new SubInterval(eSampleVals.get(i), eSampleVals.get(i+1)));
                        i++; // skip upper
                    }
                }
                plane.energy.bandpassName = Util.getString(data, map.get("caom2:Plane.energy.bandpassName"));
                plane.energy.dimension = Util.getLong(data, map.get("caom2:Plane.energy.dimension"));
                String emBand = Util.getString(data, map.get("caom2:Plane.energy.emBand"));
                if (emBand != null)
                    plane.energy.emBand = EnergyBand.toValue(emBand);
                plane.energy.resolvingPower = Util.getDouble(data, map.get("caom2:Plane.energy.resolvingPower"));
                plane.energy.restwav = Util.getDouble(data, map.get("caom2:Plane.energy.restwav"));
                plane.energy.sampleSize = Util.getDouble(data, map.get("caom2:Plane.energy.sampleSize"));
                String spec = Util.getString(data, map.get("caom2:Plane.energy.transition.species"));
                String trans = Util.getString(data, map.get("caom2:Plane.energy.transition.transition"));
                if (spec != null && trans != null)
                    plane.energy.transition = new EnergyTransition(spec, trans);
            }
            
            Double tBounds1 = Util.getDouble(data, map.get("caom2:Plane.time.bounds.lower"));
            Double tBounds2 = Util.getDouble(data, map.get("caom2:Plane.time.bounds.upper"));
            List<Double> tSampleVals = Util.getDoubleList(data, map.get("caom2:Plane.time.bounds"));
            if (tBounds1 != null && tBounds2 != null)
            {
                plane.time = new Time();
                plane.time.bounds = new Interval(tBounds1, tBounds2);
                if (tSampleVals != null && tSampleVals.size() > 2) // actual sub-samples
                {
                    for (int i=0; i<tSampleVals.size(); i++)
                    {
                        plane.time.bounds.getSamples().add(new SubInterval(tSampleVals.get(i), tSampleVals.get(i+1)));
                        i++; // skip upper
                    }
                }
                plane.time.dimension = Util.getLong(data, map.get("caom2:Plane.time.dimension"));
                plane.time.resolution = Util.getDouble(data, map.get("caom2:Plane.time.resolution"));
                plane.time.exposure = Util.getDouble(data, map.get("caom2:Plane.time.exposure"));
                plane.time.sampleSize = Util.getDouble(data, map.get("caom2:Plane.time.sampleSize"));
                
            }

            String polStates = Util.getString(data, map.get("caom2:Plane.polarization.states"));
            if (polStates != null)
            {
                plane.polarization = new Polarization();
                plane.polarization.states = new ArrayList<PolarizationState>();
                Util.decodeStates(polStates, plane.polarization.states);
                plane.polarization.dimension = Util.getLong(data, map.get("caom2:Plane.polarization.dimension"));
            }
            
            Metrics metrics = new Metrics();
            metrics.background = Util.getDouble(data, map.get("caom2:Plane.metrics.background"));
            metrics.backgroundStddev = Util.getDouble(data, map.get("caom2:Plane.metrics.backgroundStddev"));
            metrics.fluxDensityLimit = Util.getDouble(data, map.get("caom2:Plane.metrics.fluxDensityLimit"));
            metrics.magLimit = Util.getDouble(data, map.get("caom2:Plane.metrics.magLimit"));
            metrics.sourceNumberDensity = Util.getDouble(data, map.get("caom2:Plane.metrics.sourceNumberDensity"));
            // cosmetic but consistent with ObservationReader:
            if (metrics.background != null || metrics.backgroundStddev != null
                    || metrics.fluxDensityLimit != null || metrics.magLimit != null
                    || metrics.sourceNumberDensity != null)
            {
                    plane.metrics = metrics;
            }
            
            String provName = Util.getString(data, map.get("caom2:Plane.provenance.name"));
            if (provName != null)
            {
                plane.provenance = new Provenance(provName);
                plane.provenance.lastExecuted = Util.getDate(data, map.get("caom2:Plane.provenance.lastExecuted"));
                plane.provenance.producer = Util.getString(data, map.get("caom2:Plane.provenance.producer"));
                plane.provenance.project = Util.getString(data, map.get("caom2:Plane.provenance.project"));
                String sref = Util.getString(data, map.get("caom2:Plane.provenance.reference"));
                if (sref != null)
                    plane.provenance.reference = new URI(sref);
                plane.provenance.runID = Util.getString(data, map.get("caom2:Plane.provenance.runID"));
                plane.provenance.version = Util.getString(data, map.get("caom2:Plane.provenance.version"));
                String inputs = Util.getString(data, map.get("caom2:Plane.provenance.inputs"));
                Util.decodePlaneURIs(inputs, plane.provenance.getInputs());
                String keywords = Util.getString(data, map.get("caom2:Plane.provenance.keywords"));
                Util.decodeListString(keywords, plane.provenance.getKeywords());
            }
            
            String qualityFlag = Util.getString(data, map.get("caom2:Plane.quality.flag"));
            if (qualityFlag != null)
                plane.quality = new DataQuality(Quality.toValue(qualityFlag));
            
            Date lastModified = Util.getDate(data, map.get("caom2:Plane.lastModified"));
            Date maxLastModified = Util.getDate(data, map.get("caom2:Plane.maxLastModified"));
            Util.assignLastModified(plane, lastModified, "lastModified");
            Util.assignLastModified(plane, maxLastModified, "maxLastModified");
            
            URI metaChecksum = Util.getURI(data, map.get("caom2:Plane.metaChecksum"));
            URI accMetaChecksum = Util.getURI(data, map.get("caom2:Plane.accMetaChecksum"));
            Util.assignMetaChecksum(plane, metaChecksum, "metaChecksum");
            Util.assignMetaChecksum(plane, accMetaChecksum, "accMetaChecksum");
            
            Util.assignID(plane, id);

            return plane;
        }
        catch(URISyntaxException ex)
        {
            throw new UnexpectedContentException("invalid URI", ex);
        }
        finally { }
    }
}
