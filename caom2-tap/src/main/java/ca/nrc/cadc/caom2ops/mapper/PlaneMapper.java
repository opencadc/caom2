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
*  $Revision: 5 $
*
************************************************************************
 */

package ca.nrc.cadc.caom2ops.mapper;

import ca.nrc.cadc.caom2.CalibrationLevel;
import ca.nrc.cadc.caom2.CustomAxis;
import ca.nrc.cadc.caom2.DataProductType;
import ca.nrc.cadc.caom2.DataQuality;
import ca.nrc.cadc.caom2.Energy;
import ca.nrc.cadc.caom2.EnergyTransition;
import ca.nrc.cadc.caom2.Metrics;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.Polarization;
import ca.nrc.cadc.caom2.Position;
import ca.nrc.cadc.caom2.Provenance;
import ca.nrc.cadc.caom2.Quality;
import ca.nrc.cadc.caom2.Time;
import ca.nrc.cadc.caom2.types.Circle;
import ca.nrc.cadc.caom2.types.Interval;
import ca.nrc.cadc.caom2.types.MultiPolygon;
import ca.nrc.cadc.caom2.types.Point;
import ca.nrc.cadc.caom2.types.Polygon;
import ca.nrc.cadc.caom2.types.SampledInterval;
import ca.nrc.cadc.caom2.types.SegmentType;
import ca.nrc.cadc.caom2.types.Vertex;
import ca.nrc.cadc.caom2.util.CaomUtil;
import ca.nrc.cadc.caom2.wcs.Dimension2D;
import java.net.URI;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.UUID;
import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
public class PlaneMapper implements VOTableRowMapper<Plane> {

    private static final Logger log = Logger.getLogger(PlaneMapper.class);

    private Map<String, Integer> map;

    public PlaneMapper(Map<String, Integer> map) {
        this.map = map;
    }

    public Plane mapRow(List<Object> data, DateFormat dateFormat) {
        log.debug("mapping Plane");
        UUID id = Util.getUUID(data, map.get("caom2:Plane.id"));
        if (id == null) {
            return null;
        }

        try {
            String productID = Util.getString(data, map.get("caom2:Plane.productID"));

            Plane plane = new Plane(productID);

            Integer cal = Util.getInteger(data, map.get("caom2:Plane.calibrationLevel"));
            if (cal != null) {
                plane.calibrationLevel = CalibrationLevel.toValue(cal);
            }

            String dpType = Util.getString(data, map.get("caom2:Plane.dataProductType"));
            if (dpType != null) {
                plane.dataProductType = DataProductType.toValue(dpType);
            }

            plane.dataRelease = Util.getDate(data, map.get("caom2:Plane.dataRelease"));
            List<URI> drg = Util.getURIList(data, map.get("caom2:Plane.dataReadGroups")); // CAOM-2.4
            if (drg != null) {
                plane.getDataReadGroups().addAll(drg);
            }
            plane.metaRelease = Util.getDate(data, map.get("caom2:Plane.metaRelease"));
            List<URI> mrg = Util.getURIList(data, map.get("caom2:Plane.metaReadGroups")); // CAOM-2.4
            if (mrg != null) {
                plane.getMetaReadGroups().addAll(mrg);
            }

            plane.creatorID = Util.getURI(data, map.get("caom2:Plane.creatorID"));

            // position
            ca.nrc.cadc.dali.Shape posBounds = (ca.nrc.cadc.dali.Shape) Util.getObject(data, map.get("caom2:Plane.position.bounds"));
            double[] posBoundsSamples = (double[]) Util.getObject(data, map.get("caom2:Plane.position.bounds.samples"));
            if (posBounds != null) {
                plane.position = new Position();
                if (posBounds instanceof ca.nrc.cadc.dali.Circle) {
                    ca.nrc.cadc.dali.Circle c = (ca.nrc.cadc.dali.Circle) posBounds;
                    plane.position.bounds = new Circle(new Point(c.getCenter().getLongitude(), c.getCenter().getLatitude()), c.getRadius());
                } else if (posBounds instanceof ca.nrc.cadc.dali.Polygon) {
                    List<Point> pts = new ArrayList<Point>();
                    ca.nrc.cadc.dali.Polygon pp = (ca.nrc.cadc.dali.Polygon) posBounds;
                    for (ca.nrc.cadc.dali.Point p : pp.getVertices()) {
                        pts.add(new Point(p.getLongitude(), p.getLatitude()));
                    }
                    MultiPolygon mp = new MultiPolygon();
                    if (posBoundsSamples != null) {
                        for (int i = 0; i < posBoundsSamples.length; i += 3) {
                            double cv1 = posBoundsSamples[i];
                            double cv2 = posBoundsSamples[i + 1];
                            SegmentType st = SegmentType.toValue((int) posBoundsSamples[i + 2]);
                            Vertex v = new Vertex(cv1, cv2, st);
                            mp.getVertices().add(v);
                        }
                        plane.position.bounds = new Polygon(pts, mp);
                    } else {
                        log.warn("cannot reconstruct Plane.position.bounds Polygon: Plane.position.bounds.samples was null");
                    }
                } else {
                    throw new RuntimeException("OOPS: unexpected type for caom2:Plane.position.bounds: " + posBounds.getClass().getName());
                }

                Long dim1 = Util.getLong(data, map.get("caom2:Plane.position.dimension.naxis1"));
                Long dim2 = Util.getLong(data, map.get("caom2:Plane.position.dimension.naxis2"));
                if (dim1 != null && dim2 != null) {
                    plane.position.dimension = new Dimension2D(dim1, dim2);
                }
                plane.position.resolution = Util.getDouble(data, map.get("caom2:Plane.position.resolution"));
                double[] resBounds = (double[]) Util.getObject(data, map.get("caom2:Plane.position.bounds.resolutionBounds")); // CAOM 2.4
                if (resBounds != null) {
                    plane.position.resolutionBounds = new Interval(resBounds[0], resBounds[1]);
                }
                plane.position.sampleSize = Util.getDouble(data, map.get("caom2:Plane.position.sampleSize"));
                plane.position.timeDependent = Util.getBoolean(data, map.get("caom2:Plane.position.timeDependent"));
            }

            // energy
            ca.nrc.cadc.dali.DoubleInterval nrgBounds
                    = (ca.nrc.cadc.dali.DoubleInterval) Util.getObject(data, map.get("caom2:Plane.energy.bounds"));
            double[] nrgBoundsSamples = (double[]) Util.getObject(data, map.get("caom2:Plane.energy.bounds.samples"));
            if (nrgBounds != null) {
                plane.energy = new Energy();
                plane.energy.bounds = new SampledInterval(nrgBounds.getLower(), nrgBounds.getUpper());
                if (nrgBoundsSamples != null) {
                    //for (ca.nrc.cadc.dali.DoubleInterval si : nrgSamples)
                    for (int i = 0; i < nrgBoundsSamples.length; i += 2) {
                        //plane.energy.bounds.getSamples().add(new SubInterval(si.getLower(), si.getUpper()));
                        plane.energy.bounds.getSamples().add(new Interval(nrgBoundsSamples[i], nrgBoundsSamples[i + 1]));
                    }
                } else {
                    // HACK: backwards compat
                    plane.energy.bounds.getSamples().add(new Interval(nrgBounds.getLower(), nrgBounds.getUpper()));
                }
                plane.energy.bandpassName = Util.getString(data, map.get("caom2:Plane.energy.bandpassName"));
                plane.energy.dimension = Util.getLong(data, map.get("caom2:Plane.energy.dimension"));
                String ebs = Util.getString(data, map.get("caom2:Plane.energy.emBand"));
                if (ebs != null) {
                    CaomUtil.decodeBands(ebs, plane.energy.getEnergyBands());
                }
                plane.energy.resolvingPower = Util.getDouble(data, map.get("caom2:Plane.energy.resolvingPower"));
                double[] resBounds = (double[]) Util.getObject(data, map.get("caom2:Plane.energy.bounds.resolvingPowerBounds")); // CAOM 2.4
                if (resBounds != null) {
                    plane.energy.resolvingPowerBounds = new Interval(resBounds[0], resBounds[1]);
                }
                plane.energy.restwav = Util.getDouble(data, map.get("caom2:Plane.energy.restwav"));
                plane.energy.sampleSize = Util.getDouble(data, map.get("caom2:Plane.energy.sampleSize"));
                String spec = Util.getString(data, map.get("caom2:Plane.energy.transition.species"));
                String trans = Util.getString(data, map.get("caom2:Plane.energy.transition.transition"));
                if (spec != null && trans != null) {
                    plane.energy.transition = new EnergyTransition(spec, trans);
                }
            }

            // time
            ca.nrc.cadc.dali.DoubleInterval timBounds
                    = (ca.nrc.cadc.dali.DoubleInterval) Util.getObject(data, map.get("caom2:Plane.time.bounds"));
            double[] timBoundsSamples = (double[]) Util.getObject(data, map.get("caom2:Plane.time.bounds.samples"));
            if (timBounds != null) {
                plane.time = new Time();
                plane.time.bounds = new SampledInterval(timBounds.getLower(), timBounds.getUpper());
                if (timBoundsSamples != null) {
                    // actual sub-samples
                    //for (ca.nrc.cadc.dali.DoubleInterval si : timSamples)
                    for (int i = 0; i < timBoundsSamples.length; i += 2) {
                        //plane.time.bounds.getSamples().add(new SubInterval(si.getLower(), si.getUpper()));
                        plane.time.bounds.getSamples().add(new Interval(timBoundsSamples[i], timBoundsSamples[i + 1]));
                    }
                } else {
                    // HACK: backwards compat
                    plane.time.bounds.getSamples().add(new Interval(timBounds.getLower(), timBounds.getUpper()));
                }
                plane.time.dimension = Util.getLong(data, map.get("caom2:Plane.time.dimension"));
                plane.time.resolution = Util.getDouble(data, map.get("caom2:Plane.time.resolution"));
                double[] resBounds = (double[]) Util.getObject(data, map.get("caom2:Plane.time.bounds.resolutionBounds")); // CAOM 2.4
                if (resBounds != null) {
                    plane.time.resolutionBounds = new Interval(resBounds[0], resBounds[1]);
                }
                plane.time.exposure = Util.getDouble(data, map.get("caom2:Plane.time.exposure"));
                plane.time.sampleSize = Util.getDouble(data, map.get("caom2:Plane.time.sampleSize"));
            }

            // polarization
            String polStates = Util.getString(data, map.get("caom2:Plane.polarization.states"));
            if (polStates != null) {
                plane.polarization = new Polarization();
                plane.polarization.states = new TreeSet<>();
                Util.decodeStates(polStates, plane.polarization.states);
                plane.polarization.dimension = Util.getLong(data, map.get("caom2:Plane.polarization.dimension"));
            }

            // custom
            String customCtype = Util.getString(data, map.get("caom2:Plane.custom.ctype")); // CAOM 2.4
            if (customCtype != null) {
                plane.custom = new CustomAxis(customCtype);
                ca.nrc.cadc.dali.DoubleInterval cusBounds
                        = (ca.nrc.cadc.dali.DoubleInterval) Util.getObject(data, map.get("caom2:Plane.custom.bounds"));
                double[] cusBoundsSamples = (double[]) Util.getObject(data, map.get("caom2:Plane.time.custom.samples"));
                if (cusBounds != null) {
                    plane.custom.bounds = new SampledInterval(cusBounds.getLower(), cusBounds.getUpper());
                    if (cusBoundsSamples != null) {
                        for (int i = 0; i < cusBoundsSamples.length; i += 2) {
                            plane.custom.bounds.getSamples().add(new Interval(cusBoundsSamples[i], cusBoundsSamples[i + 1]));
                        }
                    }
                    plane.custom.dimension = Util.getLong(data, map.get("caom2:Plane.custom.dimension"));
                }
            }

            Metrics metrics = new Metrics();
            metrics.background = Util.getDouble(data, map.get("caom2:Plane.metrics.background"));
            metrics.backgroundStddev = Util.getDouble(data, map.get("caom2:Plane.metrics.backgroundStddev"));
            metrics.fluxDensityLimit = Util.getDouble(data, map.get("caom2:Plane.metrics.fluxDensityLimit"));
            metrics.magLimit = Util.getDouble(data, map.get("caom2:Plane.metrics.magLimit"));
            metrics.sourceNumberDensity = Util.getDouble(data, map.get("caom2:Plane.metrics.sourceNumberDensity"));
            metrics.sampleSNR = Util.getDouble(data, map.get("caom2:Plane.metrics.sampleSNR"));
            // cosmetic but consistent with ObservationReader:
            if (metrics.background != null || metrics.backgroundStddev != null
                    || metrics.fluxDensityLimit != null || metrics.magLimit != null
                    || metrics.sourceNumberDensity != null || metrics.sampleSNR != null) {
                plane.metrics = metrics;
            }

            String provName = Util.getString(data, map.get("caom2:Plane.provenance.name"));
            if (provName != null) {
                plane.provenance = new Provenance(provName);
                plane.provenance.lastExecuted = Util.getDate(data, map.get("caom2:Plane.provenance.lastExecuted"));
                plane.provenance.producer = Util.getString(data, map.get("caom2:Plane.provenance.producer"));
                plane.provenance.project = Util.getString(data, map.get("caom2:Plane.provenance.project"));
                String sref = Util.getString(data, map.get("caom2:Plane.provenance.reference"));
                if (sref != null) {
                    plane.provenance.reference = new URI(sref);
                }
                plane.provenance.runID = Util.getString(data, map.get("caom2:Plane.provenance.runID"));
                plane.provenance.version = Util.getString(data, map.get("caom2:Plane.provenance.version"));
                String inputs = Util.getString(data, map.get("caom2:Plane.provenance.inputs"));
                Util.decodePlaneURIs(inputs, plane.provenance.getInputs());
                String keywords = Util.getString(data, map.get("caom2:Plane.provenance.keywords"));
                Util.decodeKeywordList(keywords, plane.provenance.getKeywords());
            }

            String qualityFlag = Util.getString(data, map.get("caom2:Plane.quality.flag"));
            if (qualityFlag != null) {
                plane.quality = new DataQuality(Quality.toValue(qualityFlag));
            }

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
        } catch (Exception ex) {
            throw new UnexpectedContentException("invalid content: " + ex.getMessage(), ex);
        }
    }
}
