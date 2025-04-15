/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2025.                            (c) 2025.
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

package org.opencadc.caom2.db;

import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.db.mappers.TimestampRowMapper;
import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.opencadc.caom2.Artifact;
import org.opencadc.caom2.ArtifactDescription;
import org.opencadc.caom2.Chunk;
import org.opencadc.caom2.DeletedArtifactDescriptionEvent;
import org.opencadc.caom2.DeletedObservationEvent;
import org.opencadc.caom2.DerivedObservation;
import org.opencadc.caom2.Energy;
import org.opencadc.caom2.EnergyTransition;
import org.opencadc.caom2.Observation;
import org.opencadc.caom2.Part;
import org.opencadc.caom2.Plane;
import org.opencadc.caom2.Position;
import org.opencadc.caom2.Time;
import org.opencadc.caom2.db.mappers.ArtifactDescriptionMapper;
import org.opencadc.caom2.db.mappers.ArtifactMapper;
import org.opencadc.caom2.db.mappers.DeletedArtifactDescriptionEventMapper;
import org.opencadc.caom2.db.mappers.DeletedObservationEventMapper;
import org.opencadc.caom2.db.mappers.ObservationMapper;
import org.opencadc.caom2.db.mappers.ObservationSkeletonExtractor;
import org.opencadc.caom2.db.mappers.ObservationStateExtractor;
import org.opencadc.caom2.db.mappers.ObservationStateMapper;
import org.opencadc.caom2.db.mappers.PartialRowMapper;
import org.opencadc.caom2.db.mappers.PlaneMapper;
import org.opencadc.caom2.db.skel.ArtifactSkeleton;
import org.opencadc.caom2.db.skel.ChunkSkeleton;
import org.opencadc.caom2.db.skel.ObservationSkeleton;
import org.opencadc.caom2.db.skel.PartSkeleton;
import org.opencadc.caom2.db.skel.PlaneSkeleton;
import org.opencadc.caom2.db.skel.Skeleton;
import org.opencadc.caom2.util.CaomUtil;
import org.opencadc.caom2.util.ObservationState;
import org.opencadc.caom2.vocab.DataLinkSemantics;
import org.opencadc.caom2.wcs.Axis;
import org.opencadc.caom2.wcs.Coord2D;
import org.opencadc.caom2.wcs.CoordAxis1D;
import org.opencadc.caom2.wcs.CoordAxis2D;
import org.opencadc.caom2.wcs.CoordBounds1D;
import org.opencadc.caom2.wcs.CoordBounds2D;
import org.opencadc.caom2.wcs.CoordError;
import org.opencadc.caom2.wcs.CoordFunction1D;
import org.opencadc.caom2.wcs.CoordFunction2D;
import org.opencadc.caom2.wcs.CoordRange1D;
import org.opencadc.caom2.wcs.CoordRange2D;
import org.opencadc.caom2.wcs.CustomWCS;
import org.opencadc.caom2.wcs.Dimension2D;
import org.opencadc.caom2.wcs.ObservableAxis;
import org.opencadc.caom2.wcs.PolarizationWCS;
import org.opencadc.caom2.wcs.RefCoord;
import org.opencadc.caom2.wcs.Slice;
import org.opencadc.caom2.wcs.SpatialWCS;
import org.opencadc.caom2.wcs.SpectralWCS;
import org.opencadc.caom2.wcs.TemporalWCS;
import org.opencadc.persist.Entity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;

/**
 *
 * @author pdowler
 */
public class SQLGenerator {

    private static Logger log = Logger.getLogger(SQLGenerator.class);

    static final int MAX_DEPTH = 5;
    
    protected static final Class[] ENTITY_CLASSES = {
        Observation.class, Plane.class, Artifact.class, Part.class, Chunk.class, DeletedObservationEvent.class, 
        ArtifactDescription.class, DeletedArtifactDescriptionEvent.class
    };
    
    protected static final Class[] SKELETON_CLASSES = {
        ObservationSkeleton.class, PlaneSkeleton.class, ArtifactSkeleton.class, PartSkeleton.class, ChunkSkeleton.class
    };
    
    protected static final Class[] STATE_CLASSES = {
        ObservationState.class
    };
    
    protected static final Class[] JOIN_CLASSES = {
        ObservationMember.class, ProvenanceInput.class
    };

    public static final String SIMPLE_TYPE = "S";
    public static final String DERIVED_TYPE = "D";

    private final Calendar utcCalendar = Calendar.getInstance(DateUtil.UTC);

    protected String database;
    protected String schema;

    /**
     * Store boolean values as integer (0 or 1). Default: true. 
     * This is to conform to the TAP-1.0 usage.
     */
    protected boolean useIntegerForBoolean = true; // TAP default
    
    /**
     * Persist alternate representations to support optimisations. Default: false.
     */
    protected boolean persistOptimisations = true;
    
    /**
     * Store optimized read access tuples in asset table(s). Default: false.
     * This causes Plane metaReadAccess values to also be stored in the child 
     * Artifact, Part, and Chunk tables so it is easier to do some access control 
     * manipulations (e.g. in TAP query processing).
     */
    protected boolean persistReadAccessWithAsset = false;
    
    protected int numOptObservationColumns;
    protected int numOptPlaneColumns;
    protected int numOptArtifactColumns;
    protected int numOptPartColumns;
    protected int numOptChunkColumns;

    // map of Class to the table that stores instances
    protected final Map<Class, String> tableMap = new TreeMap<>(new ClassComp());

    // map of Class to String[] with all the column names
    protected final Map<Class, String[]> columnMap = new TreeMap<>(new ClassComp());

    // map of column names whose values must be cast in insert or update
    protected final Map<String, String> castMap = new TreeMap<>();

    // map of Class to standard alias name used in all select queries (w/ joins)
    protected final Map<Class, String> aliasMap = new TreeMap<>(new ClassComp());

    private SQLDialect dbDialect;
    
    SQLGenerator(String database, String schema) {
        this.database = database;
        this.schema = schema;
        init();
    }

    private void init() {
        this.dbDialect = new PostgreSQLDialect(useIntegerForBoolean);
        
        for (Class c : ENTITY_CLASSES) {
            String s = c.getSimpleName();
            tableMap.put(c, s);
            aliasMap.put(c, c.getSimpleName());
        }
        for (Class c : SKELETON_CLASSES) {
            String s = c.getSimpleName();
            s = s.replace("Skeleton", ""); // skeleton classes read from underlying tables
            tableMap.put(c, s);
            aliasMap.put(c, c.getSimpleName());
        }
        for (Class c : STATE_CLASSES) {
            String s = c.getSimpleName();
            s = s.replace("State", ""); // state classes read from underlying tables
            tableMap.put(c, s);
            aliasMap.put(c, c.getSimpleName());
        }
        for (Class c : JOIN_CLASSES) {
            String s = c.getSimpleName();
            tableMap.put(c, s);
            aliasMap.put(c, c.getSimpleName());
        }

        // IMPORTANT:
        // - the primary key column is LAST in the list of columns so that
        //   insert and update statements have the same argument number and order
        // - the foreign key column is FIRST so the ResultSetExtractor can tell when
        //   it hits a null object
        // - the typeCode column is first so the right subclass of observation can be created
        
        String[] obsColumns = ObservationMapper.COLUMNS;
        if (persistOptimisations && ObservationMapper.OPT_COLUMNS.length > 0) {
            this.numOptObservationColumns = ObservationMapper.OPT_COLUMNS.length;
            obsColumns = addExtraColumns(obsColumns, ObservationMapper.OPT_COLUMNS);
        }
        columnMap.put(Observation.class, obsColumns);
        
        String[] obsMembersColumns = new String[] {
            "parentID", "memberID"
        };
        columnMap.put(ObservationMember.class, obsMembersColumns);

        String[] planeColumns = PlaneMapper.COLUMNS;
        if (persistOptimisations && PlaneMapper.OPT_COLUMNS.length > 0) {
            
            this.numOptPlaneColumns = PlaneMapper.OPT_COLUMNS.length;
            planeColumns = addExtraColumns(planeColumns, PlaneMapper.OPT_COLUMNS);
        }
        columnMap.put(Plane.class, planeColumns);
        
        String[] provInputColumns = new String[] {
            "outputID", "inputID"
        };
        columnMap.put(ProvenanceInput.class, provInputColumns);

        String[] artifactColumns = ArtifactMapper.COLUMNS;
        if (persistOptimisations) {
            String[] extraCols = new String[] {
                // TODO
            };
            this.numOptArtifactColumns = extraCols.length;
            artifactColumns = addExtraColumns(artifactColumns, extraCols);
        }
        columnMap.put(Artifact.class, artifactColumns);

        String[] partColumns = new String[]{
            "artifactID", // FK 
            "name", "productType",
            "lastModified", "maxLastModified",
            "metaChecksum", "accMetaChecksum", "metaProducer",
            "partID" // PK
        };
        if (persistOptimisations) {
            String[] extraCols = new String[] {
                // TODO
            };
            this.numOptPartColumns = extraCols.length;
            partColumns = addExtraColumns(partColumns, extraCols);
        }
        columnMap.put(Part.class, partColumns);

        String[] chunkColumns = new String[] {
            "partID", // FK
            "productType", "naxis",
            "positionAxis1", "positionAxis2", "energyAxis", "timeAxis", "polarizationAxis", "customAxis",
            "observableAxis",
            
            "position_axis_axis1_ctype",
            "position_axis_axis1_cunit",
            "position_axis_axis2_ctype",
            "position_axis_axis2_cunit",
            "position_axis_error1_syser",
            "position_axis_error1_rnder",
            "position_axis_error2_syser",
            "position_axis_error2_rnder",
            "position_axis_range_start_coord1_pix", "position_axis_range_start_coord1_val",
            "position_axis_range_start_coord2_pix", "position_axis_range_start_coord2_val",
            "position_axis_range_end_coord1_pix", "position_axis_range_end_coord1_val",
            "position_axis_range_end_coord2_pix", "position_axis_range_end_coord2_val",
            "position_axis_bounds",
            "position_axis_function_dimension_naxis1", "position_axis_function_dimension_naxis2",
            "position_axis_function_refcoord_coord1_pix", "position_axis_function_refCoord_coord1_val",
            "position_axis_function_refCoord_coord2_pix", "position_axis_function_refCoord_coord2_val",
            "position_axis_function_cd11", "position_axis_function_cd12",
            "position_axis_function_cd21", "position_axis_function_cd22",
            "position_coordsys", "position_equinox", "position_resolution",
            
            "energy_axis_axis_ctype", "energy_axis_axis_cunit",
            "energy_axis_error_syser", "energy_axis_error_rnder",
            "energy_axis_range_start_pix", "energy_axis_range_start_val",
            "energy_axis_range_end_pix", "energy_axis_range_end_val",
            "energy_axis_bounds",
            "energy_axis_function_naxis",
            "energy_axis_function_refCoord_pix", "energy_axis_function_refCoord_val",
            "energy_axis_function_delta",
            "energy_specsys", "energy_ssysobs", "energy_ssyssrc",
            "energy_restfrq", "energy_restwav",
            "energy_velosys", "energy_zsource", "energy_velang",
            "energy_bandpassName", "energy_resolvingPower",
            "energy_transition_species", "energy_transition_transition",
            
            "time_axis_axis_ctype", "time_axis_axis_cunit",
            "time_axis_error_syser", "time_axis_error_rnder",
            "time_axis_range_start_pix", "time_axis_range_start_val",
            "time_axis_range_end_pix", "time_axis_range_end_val",
            "time_axis_bounds",
            "time_axis_function_naxis",
            "time_axis_function_refCoord_pix", "time_axis_function_refCoord_val",
            "time_axis_function_delta",
            "time_timesys", "time_trefpos", "time_mjdref",
            "time_exposure", "time_resolution",
            
            "polarization_axis_axis_ctype", "polarization_axis_axis_cunit",
            "polarization_axis_error_syser", "polarization_axis_error_rnder",
            "polarization_axis_range_start_pix", "polarization_axis_range_start_val",
            "polarization_axis_range_end_pix", "polarization_axis_range_end_val",
            "polarization_axis_bounds",
            "polarization_axis_function_naxis",
            "polarization_axis_function_refCoord_pix", "polarization_axis_function_refCoord_val",
            "polarization_axis_function_delta",
            
            "custom_axis_axis_ctype", "custom_axis_axis_cunit",
            "custom_axis_error_syser", "custom_axis_error_rnder",
            "custom_axis_range_start_pix", "custom_axis_range_start_val",
            "custom_axis_range_end_pix", "custom_axis_range_end_val",
            "custom_axis_bounds",
            "custom_axis_function_naxis",
            "custom_axis_function_refCoord_pix", "custom_axis_function_refCoord_val",
            "custom_axis_function_delta",
            
            "observable_dependent_axis_ctype",
            "observable_dependent_axis_cunit",
            "observable_dependent_bin",
            "observable_independent_axis_ctype",
            "observable_independent_axis_cunit",
            "observable_independent_bin",
            "lastModified", "maxLastModified",
            "metaChecksum", "accMetaChecksum", "metaProducer",
            "chunkID" // PK
        };
        if (persistOptimisations) {
            String[] extraCols = new String[] {
                // TODO
            };
            this.numOptChunkColumns = extraCols.length;
            chunkColumns = addExtraColumns(chunkColumns, extraCols);
        }
        columnMap.put(Chunk.class, chunkColumns);

        columnMap.put(ObservationState.class, ObservationStateMapper.COLUMNS);
        columnMap.put(ArtifactDescription.class, ArtifactDescriptionMapper.COLUMNS);
        columnMap.put(DeletedObservationEvent.class, DeletedArtifactDescriptionEventMapper.COLUMNS);
        columnMap.put(DeletedArtifactDescriptionEvent.class, DeletedObservationEventMapper.COLUMNS);
        
        // FK column first, PK last
        columnMap.put(ObservationSkeleton.class, new String[] {"lastModified", "maxLastModified", "metaChecksum", "accMetaChecksum", "obsID"});
        columnMap.put(PlaneSkeleton.class, new String[] {"obsID", "lastModified", "maxLastModified", "metaChecksum", "accMetaChecksum", "planeID"});
        columnMap.put(ArtifactSkeleton.class, new String[] {"planeID", "lastModified", "maxLastModified", "metaChecksum", "accMetaChecksum", "artifactID"});
        columnMap.put(PartSkeleton.class, new String[] {"artifactID", "lastModified", "maxLastModified", "metaChecksum", "accMetaChecksum", "partID"});
        columnMap.put(ChunkSkeleton.class, new String[] {"partID", "lastModified", "maxLastModified", "metaChecksum", "accMetaChecksum", "chunkID"});
    }

    // insert the extra columns before the CaomEntity columns and PK (last 6)
    private String[] addExtraColumns(String[] origCols, String[] extraCols) {
        int n = origCols.length + extraCols.length;
        String[] allCols = new String[n];

        System.arraycopy(origCols, 0, allCols, 0, origCols.length - 6);
        int num = origCols.length - 6;
        System.arraycopy(extraCols, 0, allCols, num, extraCols.length);
        num += extraCols.length;
        System.arraycopy(origCols, origCols.length - 6, allCols, num, 6);
        return allCols;
    }

    public String getCatalog() {
        return database;
    }

    public String getSchema() {
        return schema;
    }

    /**
     * The default implementation uses the SQL standard CURRENT_TIMESTAMP symbol.
     * @return 
     */
    
    public String getCurrentTimeSQL() {
        return "select CURRENT_TIMESTAMP";
    }

    public String getSelectSQL(UUID id, int depth, boolean skeleton) {
        StringBuilder sb = new StringBuilder();
        String alias = getAlias(Observation.class);
        if (skeleton) {
            alias = getAlias(ObservationSkeleton.class);
        }
        sb.append("SELECT ");
        sb.append(getObservationSelect(depth, skeleton));
        sb.append(" WHERE ");
        sb.append(alias);
        sb.append(".");
        if (skeleton) {
            sb.append(getPrimaryKeyColumn(ObservationSkeleton.class));
        } else {
            sb.append(getPrimaryKeyColumn(Observation.class));
        }
        sb.append(" = ");
        sb.append(dbDialect.literal(id));
        String orderBy = getOrderColumns(depth);
        if (skeleton) {
            orderBy = getSkeletonOrderColumns(depth);
        }
        if (orderBy != null) {
            sb.append(" ORDER BY ");
            sb.append(orderBy);
        }
        return sb.toString();
    }

    // just SELECT {columns} FROM {table} AS {alias}
    protected StringBuilder getSelectSQL(Class clz) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        sb.append(getColumns(clz));
        sb.append(" FROM ");
        sb.append(getTable(clz));
        sb.append(" AS ");
        sb.append(getAlias(clz));
        
        return sb;
    }

    public String getSelectSQL(Class clz, UUID id) {
        return getSelectSQL(clz, id, true, false);
    }
    
    public String getSelectSQL(Class clz, UUID id, boolean primaryKey, boolean forUpdate) {
        StringBuilder sb = getSelectSQL(clz);
        sb.append(" WHERE ");
        if (primaryKey) {
            sb.append(getPrimaryKeyColumn(clz));
        } else {
            sb.append(getForeignKeyColumn(clz));
        }
        sb.append(" = ");
        sb.append(dbDialect.literal(id));
        if (forUpdate) {
            sb.append(" FOR UPDATE");
        }
        return sb.toString();
    }

    protected String getLimitConstraint(Integer batchSize) {
        if (batchSize == null) {
            return null;
        }
        return "LIMIT " + batchSize;
    }

    // package access so tests can delete to cleanup
    String getDeleteSQL(Class c, UUID id, boolean primaryKey) {
        if (Observation.class.isAssignableFrom(c)) {
            c = Observation.class;
        }
        StringBuilder sb = new StringBuilder();
        sb.append("DELETE FROM ");
        sb.append(getTable(c));
        sb.append(" WHERE ");
        if (primaryKey) {
            sb.append(getPrimaryKeyColumn(c));
        } else {
            sb.append(getForeignKeyColumn(c));
        }
        sb.append(" = ");
        sb.append(dbDialect.literal(id));
        return sb.toString();
    }

    public String getPrimaryKeyColumn(Class c) {
        if (Observation.class.isAssignableFrom(c)) {
            c = Observation.class;
        }
        String[] cols = columnMap.get(c);
        return cols[cols.length - 1]; // last column is PK
    }

    public String getForeignKeyColumn(Class c) {
        if (Observation.class.isAssignableFrom(c) 
                || ObservationSkeleton.class.isAssignableFrom(c)
                || ArtifactDescription.class.isAssignableFrom(c)
                || DeletedObservationEvent.class.isAssignableFrom(c)
                || DeletedArtifactDescriptionEvent.class.isAssignableFrom(c)) {
            throw new IllegalArgumentException(c.getSimpleName() + " does not have a foreign key");
        }
        String[] cols = columnMap.get(c);
        return cols[0]; // first column is FK
    }

    private String getInsertSQL(Class clz) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ");
        sb.append(getTable(clz));
        sb.append(" (");
        String[] cols = columnMap.get(clz);
        for (int c = 0; c < cols.length; c++) {
            if (c > 0) {
                sb.append(",");
            }
            sb.append(cols[c]);
        }
        sb.append(" ) VALUES (");
        for (int c = 0; c < cols.length; c++) {
            if (c > 0) {
                sb.append(",");
            }
            sb.append("?");

            // experimental cast support
            String cast = castMap.get(cols[c]);
            if (cast != null) {
                sb.append("::").append(cast);
            }
        }
        sb.append(")");

        return sb.toString();
    }

    private String getUpdateSQL(Class clz) {
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE ");
        sb.append(getTable(clz));
        sb.append(" SET ");
        String[] cols = columnMap.get(clz);
        for (int c = 0; c < cols.length - 1; c++) { // PK is last
            if (c > 0) {
                sb.append(",");
            }
            sb.append(cols[c]);
            sb.append(" = ?");

            // experimental cast support
            String cast = castMap.get(cols[c]);
            if (cast != null) {
                sb.append("::").append(cast);
            }
        }
        sb.append(" WHERE ");
        sb.append(getPrimaryKeyColumn(clz));
        sb.append(" = ?");

        return sb.toString();
    }

    // HACK: quick hack to update optimization columns in child classes
    // lots of hard-coded and assumptions here
    protected String getUpdateChildOptimisationSQL(Class child) {
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE ");
        sb.append(getTable(child));
        sb.append(" SET ");

        // TODO: lookup instead of hard-coded
        String[] cols = new String[]{"metaRelease", "planeID"};
        for (int c = 0; c < cols.length - 1; c++) { // PK is last
            if (c > 0) {
                sb.append(",");
            }
            sb.append(cols[c]);
            sb.append(" = ?");
        }
        sb.append(" WHERE ");
        sb.append(cols[cols.length - 1]); // PK
        sb.append(" = ?");

        return sb.toString();
    }

    public EntityGet<? extends Entity> getEntityGet(Class<? extends Entity> c, boolean lock) {
        if (ArtifactDescription.class.isAssignableFrom(c)) {
            return new ArtifactDescriptionGet(lock);
        }
        if (DeletedObservationEvent.class.isAssignableFrom(c)) {
            return new DeletedObservationEventGet(lock);
        }
        if (DeletedArtifactDescriptionEvent.class.isAssignableFrom(c)) {
            return new DeletedArtifactDescriptionEventGet(lock);
        }
        throw new UnsupportedOperationException();
    }

    public EntityPut getEntityPut(Class<? extends Entity> c, boolean isUpdate) {
        if (Observation.class.isAssignableFrom(c)) {
            return new ObservationPut(isUpdate);
        }

        if (Plane.class.equals(c)) {
            return new PlanePut(isUpdate);
        }

        if (Artifact.class.equals(c)) {
            return new ArtifactPut(isUpdate);
        }

        if (Part.class.equals(c)) {
            return new PartPut(isUpdate);
        }

        if (Chunk.class.equals(c)) {
            return new ChunkPut(isUpdate);
        }

        if (ArtifactDescription.class.isAssignableFrom(c)) {
            return new ArtifactDescriptionPut(isUpdate);
        }
        if (DeletedObservationEvent.class.isAssignableFrom(c)) {
            return new DeletedObservationEventPut(isUpdate);
        }
        if (DeletedArtifactDescriptionEvent.class.isAssignableFrom(c)) {
            return new DeletedArtifactDescriptionEventPut(isUpdate);
        }
        throw new UnsupportedOperationException();
    }

    class ArtifactDescriptionGet implements EntityGet<ArtifactDescription> {
        private UUID id;
        private boolean lock;

        public ArtifactDescriptionGet(boolean lock) {
            this.lock = lock;
        }
        
        @Override
        public void setID(UUID id) {
            this.id = id;
        }

        @Override
        public ArtifactDescription execute(JdbcTemplate jdbc) {
            String sql = getSelectSQL(ArtifactDescription.class, id);
            RowMapper<ArtifactDescription> rm = new ArtifactDescriptionMapper();
            List<ArtifactDescription> found = jdbc.query(sql, rm);
            if (found.isEmpty()) {
                return null;
            }
            return found.get(0);
        }
    }

    class DeletedArtifactDescriptionEventGet implements EntityGet<DeletedArtifactDescriptionEvent> {
        private UUID id;
        private boolean lock;

        public DeletedArtifactDescriptionEventGet(boolean lock) {
            this.lock = lock;
        }
        
        @Override
        public void setID(UUID id) {
            this.id = id;
        }

        @Override
        public DeletedArtifactDescriptionEvent execute(JdbcTemplate jdbc) {
            String sql = getSelectSQL(DeletedArtifactDescriptionEvent.class, id);
            RowMapper<DeletedArtifactDescriptionEvent> rm = new DeletedArtifactDescriptionEventMapper();
            List<DeletedArtifactDescriptionEvent> found = jdbc.query(sql, rm);
            if (found.isEmpty()) {
                return null;
            }
            return found.get(0);
        }
    }

    class DeletedObservationEventGet implements EntityGet<DeletedObservationEvent> {
        private UUID id;
        private boolean lock;

        public DeletedObservationEventGet(boolean lock) {
            this.lock = lock;
        }
        
        @Override
        public void setID(UUID id) {
            this.id = id;
        }

        @Override
        public DeletedObservationEvent execute(JdbcTemplate jdbc) {
            String sql = getSelectSQL(DeletedObservationEvent.class, id);
            RowMapper<DeletedObservationEvent> rm = new DeletedObservationEventMapper();
            List<DeletedObservationEvent> found = jdbc.query(sql, rm);
            if (found.isEmpty()) {
                return null;
            }
            return found.get(0);
        }
    }

    public static class ObservationStateGet implements PreparedStatementCreator {
        private SQLGenerator gen;
        private UUID id;
        private URI uri;
        private boolean forUpdate;
        
        ObservationStateGet(SQLGenerator gen) {
            this.gen = gen;
        }

        public void setIdentifier(UUID id, URI uri) {
            this.id = id;
            this.uri = uri;
        }

        public void setForUpdate(boolean forUpdate) {
            this.forUpdate = forUpdate;
        }
        
        public ObservationState execute(JdbcTemplate jdbc) {
            return jdbc.query(this, new ObservationStateExtractor());
        }
        
        @Override
        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
            if (id == null && uri == null) {
                throw new IllegalStateException("BUG: cannot execute before calling setIdentifier");
            }
            StringBuilder sb = gen.getSelectSQL(ObservationState.class);
            if (id != null) {
                sb.append(" WHERE obsID = ?");
            } else {
                sb.append(" WHERE uri = ?");
            }
            if (forUpdate) {
                sb.append(" FOR UPDATE");
            }
            String sql = sb.toString();
            PreparedStatement prep = conn.prepareStatement(sql);
            log.debug(sql);
            if (id != null) {
                prep.setObject(1, id);
            } else {
                prep.setString(1, uri.toASCIIString());
            }
            return prep;
        }
    }

    public static class ArtifactGet implements PreparedStatementCreator {
        private SQLGenerator gen;
        private URI uri;
        
        ArtifactGet(SQLGenerator gen) {
            this.gen = gen;
        }

        public void setURI(URI uri) {
            this.uri = uri;
        }

        @Override
        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
            if (uri == null) {
                throw new IllegalStateException("BUG: cannot execute before calling setURI");
            }
            StringBuilder sb = gen.getSelectSQL(Artifact.class);
            sb.append(" WHERE ");
            sb.append("uri = ?");
            String sql = sb.toString();
            PreparedStatement prep = conn.prepareStatement(sql);
            log.debug(sql);
            prep.setString(1, uri.toASCIIString());
            return prep;
        }
    }
    
    class DeletedObservationEventPut implements EntityPut<DeletedObservationEvent>, PreparedStatementCreator {
        private boolean update;
        private DeletedObservationEvent value;

        public DeletedObservationEventPut(boolean update) {
            this.update = update;
        }
        
        public void execute(JdbcTemplate jdbc) {
            jdbc.update(this);
        }

        @Override
        public void setValue(DeletedObservationEvent value, UUID unused) {
            this.value = value;
        }
        
        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
            String sql = null;
            if (update) {
                sql = getUpdateSQL(DeletedObservationEvent.class);
            } else {
                sql = getInsertSQL(DeletedObservationEvent.class);
            }
            PreparedStatement prep = conn.prepareStatement(sql);
            log.debug(sql);
            loadValues(prep);
            return prep;
        }

        private void loadValues(PreparedStatement ps)
                throws SQLException {
            if (value == null) {
                throw new IllegalStateException("null DeletedObservation");
            }

            StringBuilder sb = null;
            if (log.isDebugEnabled()) {
                sb = new StringBuilder();
            }

            int col = 1;
            dbDialect.safeSetURI(sb, ps, col++, value.getURI());
            dbDialect.safeSetDate(sb, ps, col++, value.getLastModified(), utcCalendar);
            dbDialect.safeSetURI(sb, ps, col++, value.getMetaChecksum());
            dbDialect.safeSetURI(sb, ps, col++, value.metaProducer);
            dbDialect.safeSetUUID(sb, ps, col++, value.getID());

            if (sb != null) {
                log.debug(sb.toString());
            }
        }
        
    }
    
    class DeletedArtifactDescriptionEventPut implements EntityPut<DeletedArtifactDescriptionEvent>, PreparedStatementCreator {
        private boolean update;
        private DeletedArtifactDescriptionEvent value;

        public DeletedArtifactDescriptionEventPut(boolean update) {
            this.update = update;
        }
        
        public void execute(JdbcTemplate jdbc) {
            jdbc.update(this);
        }

        
        public void setValue(DeletedArtifactDescriptionEvent value, UUID ignore) {
            this.value = value;
        }
        
        
        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
            String sql = null;
            if (update) {
                sql = getUpdateSQL(DeletedArtifactDescriptionEvent.class);
            } else {
                sql = getInsertSQL(DeletedArtifactDescriptionEvent.class);
            }
            PreparedStatement prep = conn.prepareStatement(sql);
            log.debug(sql);
            loadValues(prep);
            return prep;
        }

        private void loadValues(PreparedStatement ps)
                throws SQLException {
            if (value == null) {
                throw new IllegalStateException("null DeletedArtifactDescription");
            }

            StringBuilder sb = null;
            if (log.isDebugEnabled()) {
                sb = new StringBuilder();
            }

            int col = 1;
            dbDialect.safeSetURI(sb, ps, col++, value.getURI());
            dbDialect.safeSetDate(sb, ps, col++, value.getLastModified(), utcCalendar);
            dbDialect.safeSetURI(sb, ps, col++, value.getMetaChecksum());
            dbDialect.safeSetURI(sb, ps, col++, value.metaProducer);
            dbDialect.safeSetUUID(sb, ps, col++, value.getID());

            if (sb != null) {
                log.debug(sb.toString());
            }
        }
    }

    class ArtifactDescriptionPut implements EntityPut<ArtifactDescription>, PreparedStatementCreator {
        private boolean update;
        private ArtifactDescription value;

        public ArtifactDescriptionPut(boolean update) {
            this.update = update;
        }
        
        public void execute(JdbcTemplate jdbc) {
            jdbc.update(this);
        }

        
        public void setValue(ArtifactDescription value, UUID ignore) {
            this.value = value;
        }
        
        
        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
            String sql = null;
            if (update) {
                sql = getUpdateSQL(ArtifactDescription.class);
            } else {
                sql = getInsertSQL(ArtifactDescription.class);
            }
            PreparedStatement prep = conn.prepareStatement(sql);
            log.debug(sql);
            loadValues(prep);
            return prep;
        }

        private void loadValues(PreparedStatement ps)
                throws SQLException {
            if (value == null) {
                throw new IllegalStateException("null ArtifactDescription");
            }

            StringBuilder sb = null;
            if (log.isDebugEnabled()) {
                sb = new StringBuilder();
            }

            int col = 1;
            dbDialect.safeSetURI(sb, ps, col++, value.getURI());
            dbDialect.safeSetString(sb, ps, col++, value.getDescription());
            dbDialect.safeSetDate(sb, ps, col++, value.getLastModified(), utcCalendar);
            dbDialect.safeSetURI(sb, ps, col++, value.getMetaChecksum());
            dbDialect.safeSetURI(sb, ps, col++, value.metaProducer);
            dbDialect.safeSetUUID(sb, ps, col++, value.getID());

            if (sb != null) {
                log.debug(sb.toString());
            }
        }
    }

    public EntityDelete getEntityDelete(Class<? extends Entity> c, boolean primaryKey) {
        return new BaseEntityDelete(c, primaryKey);
    }

    // delete single entity by primary key or foreign key
    private class BaseEntityDelete implements EntityDelete<Entity> {

        private Class<? extends Entity> clz;
        private boolean byPK;
        private UUID id;

        public BaseEntityDelete(Class<? extends Entity> c, boolean byPK) {
            this.clz = c;
            this.byPK = byPK;
        }

        
        public void execute(JdbcTemplate jdbc) {
            if (persistOptimisations) {
                // delete join table by FK
                if (Observation.class.equals(clz)) {
                    String sql = getDeleteSQL(ObservationMember.class, id, false); // delete by FK aka first column
                    log.debug("delete: " + sql);
                    jdbc.update(sql);
                } else if (Plane.class.equals(clz)) {
                    String sql = getDeleteSQL(ProvenanceInput.class, id, false); // delete by FK aka first column
                    log.debug("delete: " + sql);
                    jdbc.update(sql);
                }
            }
            
            // delete entity by FK or PK
            String sql = getDeleteSQL(clz, id, byPK);
            log.debug("delete: " + sql);
            jdbc.update(sql);
        }

        public void setID(UUID id) {
            this.id = id;
        }
    }

    private class ObservationPut implements EntityPut<Observation>, PreparedStatementCreator {

        boolean update;
        Observation obs;
        
        boolean insertMembers = false;
        boolean deleteMembers = false;
        ObservationMember member;

        ObservationPut(boolean update) {
            this.update = update;
        }

        // three-step execute:
        // 1. insert/update observation
        // 2. delete previous ObservationMember join tuples
        // composite only:
        // 3. insert current ObservationMember join tuples
        @Override
        public void execute(JdbcTemplate jdbc) {
            jdbc.update(this);
            
            if (!persistOptimisations) {
                return;
            }
            
            deleteMembers = true;
            jdbc.update(this);
            deleteMembers = false;
                
            if (obs instanceof DerivedObservation) {
                insertMembers = true;
                DerivedObservation co = (DerivedObservation) obs;
                for (URI uri : co.getMembers()) {
                    setValue(new ObservationMember(co.getID(), uri));
                    jdbc.update(this);
                }
                insertMembers = false;
            }
        }

        @Override
        public void setValue(Observation obs, UUID unused) {
            this.obs = obs;
        }

        private void setValue(ObservationMember member) {
            this.member = member;
        }
        
        @Override
        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
            String sql = null;
            if (deleteMembers) {
                sql = getDeleteSQL(ObservationMember.class, obs.getID(), false); // delete by FK aka first column
            } else if (insertMembers) {
                sql = getInsertSQL(ObservationMember.class);
            } else if (update) {
                sql = getUpdateSQL(Observation.class);
            } else {
                sql = getInsertSQL(Observation.class);
            }
            PreparedStatement prep = conn.prepareStatement(sql);
            log.debug(sql);
            if (deleteMembers) {
                // no values
            } else if (insertMembers) {
                loadValuesMember(prep);
            } else {
                loadValuesObservation(prep);
            }
            return prep;
        }

        private void loadValuesObservation(PreparedStatement ps)
                throws SQLException {
            if (obs == null) {
                throw new IllegalStateException("null observation");
            }

            StringBuilder sb = null;
            if (log.isDebugEnabled()) {
                sb = new StringBuilder();
            }

            int col = 1;
            if (obs instanceof DerivedObservation) {
                dbDialect.safeSetString(sb, ps, col++, DERIVED_TYPE);
            } else {
                dbDialect.safeSetString(sb, ps, col++, SIMPLE_TYPE);
            }
            dbDialect.safeSetURI(sb, ps, col++, obs.getURI());
            dbDialect.safeSetString(sb, ps, col++, obs.getUriBucket());
            dbDialect.safeSetString(sb, ps, col++, obs.getCollection());
            dbDialect.safeSetString(sb, ps, col++, obs.getAlgorithm().getName());
            dbDialect.safeSetString(sb, ps, col++, obs.type);
            if (obs.intent != null) {
                dbDialect.safeSetString(sb, ps, col++, obs.intent.getValue());
            } else {
                dbDialect.safeSetString(sb, ps, col++, null);
            }
            dbDialect.safeSetInteger(sb, ps, col++, obs.sequenceNumber);
            dbDialect.safeSetDate(sb, ps, col++, obs.metaRelease, utcCalendar);
            dbDialect.safeSetArray(sb, ps, col++, obs.getMetaReadGroups());
            
            if (obs.proposal != null) {
                dbDialect.safeSetString(sb, ps, col++, obs.proposal.getID());
                dbDialect.safeSetString(sb, ps, col++, obs.proposal.pi);
                dbDialect.safeSetString(sb, ps, col++, obs.proposal.project);
                dbDialect.safeSetString(sb, ps, col++, obs.proposal.title);
                dbDialect.safeSetKeywords(sb, ps, col++, obs.proposal.getKeywords());
                dbDialect.safeSetURI(sb, ps, col++, obs.proposal.reference);
            } else {
                dbDialect.safeSetString(sb, ps, col++, null);
                dbDialect.safeSetString(sb, ps, col++, null);
                dbDialect.safeSetString(sb, ps, col++, null);
                dbDialect.safeSetString(sb, ps, col++, null);
                dbDialect.safeSetKeywords(sb, ps, col++, null);
                dbDialect.safeSetURI(sb, ps, col++, null);
            }
            if (obs.target != null) {
                dbDialect.safeSetString(sb, ps, col++, obs.target.getName());
                dbDialect.safeSetURI(sb, ps, col++, obs.target.targetID);
                if (obs.target.type != null) {
                    dbDialect.safeSetString(sb, ps, col++, obs.target.type.getValue());
                } else {
                    dbDialect.safeSetString(sb, ps, col++, null);
                }
                dbDialect.safeSetBoolean(sb, ps, col++, obs.target.standard);
                dbDialect.safeSetDouble(sb, ps, col++, obs.target.redshift);
                dbDialect.safeSetBoolean(sb, ps, col++, obs.target.moving);
                dbDialect.safeSetKeywords(sb, ps, col++, obs.target.getKeywords());
            } else {
                dbDialect.safeSetString(sb, ps, col++, null);
                dbDialect.safeSetURI(sb, ps, col++, null);
                dbDialect.safeSetString(sb, ps, col++, null);
                dbDialect.safeSetBoolean(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetBoolean(sb, ps, col++, null);
                dbDialect.safeSetKeywords(sb, ps, col++, null);
            }
            if (obs.targetPosition != null) {
                dbDialect.safeSetString(sb, ps, col++, obs.targetPosition.getCoordsys());
                dbDialect.safeSetPoint(sb, ps, col++, obs.targetPosition.getCoordinates());
                dbDialect.safeSetDouble(sb, ps, col++, obs.targetPosition.equinox);
            } else {
                dbDialect.safeSetString(sb, ps, col++, null);
                dbDialect.safeSetPoint(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
            }

            if (obs.telescope != null) {
                dbDialect.safeSetString(sb, ps, col++, obs.telescope.getName());
                dbDialect.safeSetDouble(sb, ps, col++, obs.telescope.geoLocationX);
                dbDialect.safeSetDouble(sb, ps, col++, obs.telescope.geoLocationY);
                dbDialect.safeSetDouble(sb, ps, col++, obs.telescope.geoLocationZ);
                dbDialect.safeSetKeywords(sb, ps, col++, obs.telescope.getKeywords());
                if (obs.telescope.trackingMode != null) {
                    dbDialect.safeSetString(sb, ps, col++, obs.telescope.trackingMode.getValue());
                } else {
                    dbDialect.safeSetString(sb, ps, col++, null);
                }
            } else {
                dbDialect.safeSetString(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetKeywords(sb, ps, col++, null);
                dbDialect.safeSetString(sb, ps, col++, null);
            }

            if (obs.instrument != null) {
                dbDialect.safeSetString(sb, ps, col++, obs.instrument.getName());
                dbDialect.safeSetKeywords(sb, ps, col++, obs.instrument.getKeywords());
            } else {
                dbDialect.safeSetString(sb, ps, col++, null);
                dbDialect.safeSetKeywords(sb, ps, col++, null);
            }
            
            if (obs.environment != null) {
                dbDialect.safeSetDouble(sb, ps, col++, obs.environment.seeing);
                dbDialect.safeSetDouble(sb, ps, col++, obs.environment.humidity);
                dbDialect.safeSetDouble(sb, ps, col++, obs.environment.elevation);
                dbDialect.safeSetDouble(sb, ps, col++, obs.environment.tau);
                dbDialect.safeSetDouble(sb, ps, col++, obs.environment.wavelengthTau);
                dbDialect.safeSetDouble(sb, ps, col++, obs.environment.ambientTemp);
                dbDialect.safeSetBoolean(sb, ps, col++, obs.environment.photometric);
            } else {
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetBoolean(sb, ps, col++, null);
            }

            if (obs.requirements != null) {
                dbDialect.safeSetString(sb, ps, col++, obs.requirements.getFlag().getValue());
            } else {
                dbDialect.safeSetString(sb, ps, col++, null);
            }
            
            if (obs instanceof DerivedObservation) {
                DerivedObservation co = (DerivedObservation) obs;
                dbDialect.safeSetArray(sb, ps, col++, co.getMembers());
            } else {
                dbDialect.safeSetArray(sb, ps, col++, null);
            }
            
            if (persistOptimisations) {
                if (obs.targetPosition != null) {
                    dbDialect.safeSetPointOptimization(sb, ps, col++, obs.targetPosition.getCoordinates());
                } else {
                    dbDialect.safeSetPointOptimization(sb, ps, col++, null);
                }
            }

            dbDialect.safeSetDate(sb, ps, col++, obs.getLastModified(), utcCalendar);
            dbDialect.safeSetDate(sb, ps, col++, obs.getMaxLastModified(), utcCalendar);
            dbDialect.safeSetURI(sb, ps, col++, obs.getMetaChecksum());
            dbDialect.safeSetURI(sb, ps, col++, obs.getAccMetaChecksum());
            dbDialect.safeSetURI(sb, ps, col++, obs.metaProducer);
            dbDialect.safeSetUUID(sb, ps, col++, obs.getID());

            if (sb != null) {
                log.debug(sb.toString());
            }
        }
        
        private void loadValuesMember(PreparedStatement ps)
                throws SQLException {
            if (obs == null) {
                throw new IllegalStateException("null observation");
            }

            StringBuilder sb = null;
            if (log.isDebugEnabled()) {
                sb = new StringBuilder();
            }

            int col = 1;
            dbDialect.safeSetUUID(sb, ps, col++, member.getParentID());
            dbDialect.safeSetURI(sb, ps, col++, member.getMemberID());
        }
    }

    private class PlanePut implements EntityPut<Plane>, PreparedStatementCreator {

        private boolean update;
        private UUID parent;
        private Plane plane;
        //private List<CaomEntity> parents;

        private boolean deleteInputs = false;
        private boolean insertInputs = false;
        private ProvenanceInput input;

        private boolean doOpt = false;
        private Class childClass = null;

        PlanePut(boolean update) {
            this.update = update;
        }

        @Override
        public void execute(JdbcTemplate jdbc) {
            jdbc.update(this);

            if (!persistOptimisations) {
                return;
            }

            deleteInputs = true;
            jdbc.update(this);
            deleteInputs = false;
            
            if (plane.provenance != null) {
                insertInputs = true;
                for (URI uri : plane.provenance.getInputs()) {
                    input = new ProvenanceInput(plane.getID(), uri);
                    jdbc.update(this);
                }
                input = null;
                insertInputs = false;
            }
            
            /*
            doOpt = true;
            
            childClass = Artifact.class;
            jdbc.update(this);

            childClass = Part.class;
            jdbc.update(this);

            childClass = Chunk.class;
            jdbc.update(this);
            */
        }
        
        @Override
        public void setValue(Plane plane, UUID parent) {
            this.plane = plane;
            this.parent = parent;
        }
        
        @Override
        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
            String sql = null;
            if (deleteInputs) {
                sql = getDeleteSQL(ProvenanceInput.class, plane.getID(), false); // delete by FK
            } else if (insertInputs) {
                sql = getInsertSQL(ProvenanceInput.class);
            } else if (doOpt) {
                sql = getUpdateChildOptimisationSQL(childClass);
            } else {
                if (update) {
                    sql = getUpdateSQL(Plane.class);
                } else {
                    sql = getInsertSQL(Plane.class);
                }
            } 

            PreparedStatement prep = conn.prepareStatement(sql);
            log.debug(sql);
            if (deleteInputs) {
                // no values to set
            } else if (insertInputs) {
                loadValuesInput(prep);
            //} else if (doOpt) {
            //    loadValuesForOpt(prep);
            } else {
                loadValues(prep);
            }
            return prep;
        }

        private void loadValues(PreparedStatement ps)
                throws SQLException {
            if (plane == null) {
                throw new IllegalStateException("null observation");
            }

            StringBuilder sb = null;
            if (log.isDebugEnabled()) {
                sb = new StringBuilder();
            }

            int col = 1;
            dbDialect.safeSetUUID(sb, ps, col++, parent); // obsID
            dbDialect.safeSetURI(sb, ps, col++, plane.getURI());
            dbDialect.safeSetURI(sb, ps, col++, plane.publisherID);
            dbDialect.safeSetDate(sb, ps, col++, plane.metaRelease, utcCalendar);
            dbDialect.safeSetArray(sb, ps, col++, plane.getMetaReadGroups());
            dbDialect.safeSetDate(sb, ps, col++, plane.dataRelease, utcCalendar);
            dbDialect.safeSetArray(sb, ps, col++, plane.getDataReadGroups());
            if (plane.dataProductType != null) {
                dbDialect.safeSetString(sb, ps, col++, plane.dataProductType.getValue());
            } else {
                dbDialect.safeSetString(sb, ps, col++, null);
            }
            if (plane.calibrationLevel != null) {
                dbDialect.safeSetInteger(sb, ps, col++, plane.calibrationLevel.getValue());
            } else {
                dbDialect.safeSetInteger(sb, ps, col++, null);
            }

            if (plane.provenance != null) {
                dbDialect.safeSetString(sb, ps, col++, plane.provenance.getName());
                if (plane.provenance.reference != null) {
                    dbDialect.safeSetString(sb, ps, col++, plane.provenance.reference.toASCIIString());
                } else {
                    dbDialect.safeSetString(sb, ps, col++, null);
                }
                dbDialect.safeSetString(sb, ps, col++, plane.provenance.version);
                dbDialect.safeSetString(sb, ps, col++, plane.provenance.project);
                dbDialect.safeSetString(sb, ps, col++, plane.provenance.producer);
                dbDialect.safeSetString(sb, ps, col++, plane.provenance.runID);
                dbDialect.safeSetDate(sb, ps, col++, plane.provenance.lastExecuted, utcCalendar);
                dbDialect.safeSetKeywords(sb, ps, col++, plane.provenance.getKeywords());
                dbDialect.safeSetArray(sb, ps, col++, plane.provenance.getInputs());
            } else {
                dbDialect.safeSetString(sb, ps, col++, null);
                dbDialect.safeSetString(sb, ps, col++, null);
                dbDialect.safeSetString(sb, ps, col++, null);
                dbDialect.safeSetString(sb, ps, col++, null);
                dbDialect.safeSetString(sb, ps, col++, null);
                dbDialect.safeSetString(sb, ps, col++, null);
                dbDialect.safeSetDate(sb, ps, col++, null, utcCalendar);
                dbDialect.safeSetKeywords(sb, ps, col++, null);
                dbDialect.safeSetArray(sb, ps, col++, null);
            }

            if (plane.metrics != null) {
                dbDialect.safeSetDouble(sb, ps, col++, plane.metrics.sourceNumberDensity);
                dbDialect.safeSetDouble(sb, ps, col++, plane.metrics.background);
                dbDialect.safeSetDouble(sb, ps, col++, plane.metrics.backgroundStddev);
                dbDialect.safeSetDouble(sb, ps, col++, plane.metrics.fluxDensityLimit);
                dbDialect.safeSetDouble(sb, ps, col++, plane.metrics.magLimit);
                dbDialect.safeSetDouble(sb, ps, col++, plane.metrics.sampleSNR);
            } else {
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
            }

            if (plane.quality != null) {
                dbDialect.safeSetString(sb, ps, col++, plane.quality.getFlag().getValue());
            } else {
                dbDialect.safeSetString(sb, ps, col++, null);
            }
            
            // observable
            if (plane.observable != null) {
                dbDialect.safeSetString(sb, ps, col++, plane.observable.getUCD().getValue());
                if (plane.observable.calibration != null) {
                    dbDialect.safeSetString(sb, ps, col++, plane.observable.calibration.getValue());
                } else {
                    dbDialect.safeSetString(sb, ps, col++, null);
                }
            } else {
                dbDialect.safeSetString(sb, ps, col++, null);
                dbDialect.safeSetString(sb, ps, col++, null);
            }
            
            //position
            if (plane.position != null) {
                Position pos = plane.position;
                dbDialect.safeSetShape(sb, ps, col++, pos.getBounds());
                dbDialect.safeSetMultiShape(sb, ps, col++, pos.getSamples());
                if (pos.minBounds != null) {
                    dbDialect.safeSetShape(sb, ps, col++, pos.minBounds);
                } else {
                    dbDialect.safeSetShape(sb, ps, col++, null);
                }
                dbDialect.safeSetDimension(sb, ps, col++, pos.dimension);
                dbDialect.safeSetInterval(sb, ps, col++, pos.maxRecoverableScale);
                dbDialect.safeSetDouble(sb, ps, col++, pos.resolution);
                dbDialect.safeSetInterval(sb, ps, col++, pos.resolutionBounds);
                dbDialect.safeSetDouble(sb, ps, col++, pos.sampleSize);
                if (pos.calibration != null) {
                    dbDialect.safeSetString(sb, ps, col++, pos.calibration.getValue());
                } else {
                    dbDialect.safeSetString(sb, ps, col++, null);
                }
            } else {
                dbDialect.safeSetShape(sb, ps, col++, null);
                dbDialect.safeSetMultiShape(sb, ps, col++, null);
                dbDialect.safeSetShape(sb, ps, col++, null);
                dbDialect.safeSetDimension(sb, ps, col++, null);
                dbDialect.safeSetInterval(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetInterval(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetString(sb, ps, col++, null);
            }

            //energy
            if (plane.energy != null) {
                Energy nrg = plane.energy;
                dbDialect.safeSetInterval(sb, ps, col++, nrg.getBounds());
                dbDialect.safeSetIntervalList(sb, ps, col++, nrg.getSamples());
                dbDialect.safeSetString(sb, ps, col++, nrg.bandpassName);
                dbDialect.safeSetString(sb, ps, col++, CaomUtil.encodeBands(nrg.getEnergyBands()));
                dbDialect.safeSetLong(sb, ps, col++, nrg.dimension);
                dbDialect.safeSetDouble(sb, ps, col++, nrg.resolvingPower);
                dbDialect.safeSetInterval(sb, ps, col++, nrg.resolvingPowerBounds);
                dbDialect.safeSetDouble(sb, ps, col++, nrg.resolution);
                dbDialect.safeSetInterval(sb, ps, col++, nrg.resolutionBounds);
                dbDialect.safeSetDouble(sb, ps, col++, nrg.sampleSize);
                if (nrg.transition != null) {
                    dbDialect.safeSetString(sb, ps, col++, nrg.transition.getSpecies());
                    dbDialect.safeSetString(sb, ps, col++, nrg.transition.getTransition());
                } else {
                    dbDialect.safeSetString(sb, ps, col++, null);
                    dbDialect.safeSetString(sb, ps, col++, null);
                }
                dbDialect.safeSetDouble(sb, ps, col++, nrg.rest);
                if (nrg.calibration != null) {
                    dbDialect.safeSetString(sb, ps, col++, nrg.calibration.getValue());
                } else {
                    dbDialect.safeSetString(sb, ps, col++, null);
                }
            } else {
                dbDialect.safeSetInterval(sb, ps, col++, null);
                dbDialect.safeSetIntervalList(sb, ps, col++, null);
                dbDialect.safeSetString(sb, ps, col++, null);
                dbDialect.safeSetString(sb, ps, col++, null);
                dbDialect.safeSetLong(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetInterval(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetInterval(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetString(sb, ps, col++, null);
                dbDialect.safeSetString(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetString(sb, ps, col++, null);
            }

            //time
            if (plane.time != null) {
                Time tim = plane.time;
                dbDialect.safeSetInterval(sb, ps, col++, tim.getBounds());
                dbDialect.safeSetIntervalList(sb, ps, col++, tim.getSamples());
                dbDialect.safeSetLong(sb, ps, col++, tim.dimension);
                dbDialect.safeSetDouble(sb, ps, col++, tim.exposure);
                dbDialect.safeSetInterval(sb, ps, col++, tim.exposureBounds);
                dbDialect.safeSetDouble(sb, ps, col++, tim.resolution);
                dbDialect.safeSetInterval(sb, ps, col++, tim.resolutionBounds);
                dbDialect.safeSetDouble(sb, ps, col++, tim.sampleSize);
                if (tim.calibration != null) {
                    dbDialect.safeSetString(sb, ps, col++, tim.calibration.getValue());
                } else {
                    dbDialect.safeSetString(sb, ps, col++, null);
                }
            } else {
                dbDialect.safeSetInterval(sb, ps, col++, null);
                dbDialect.safeSetIntervalList(sb, ps, col++, null);
                dbDialect.safeSetLong(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetInterval(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetInterval(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetString(sb, ps, col++, null);
            }

            //polarization
            if (plane.polarization != null) {
                dbDialect.safeSetString(sb, ps, col++, CaomUtil.encodeStates(plane.polarization.getStates()));
                dbDialect.safeSetInteger(sb, ps, col++, plane.polarization.dimension);
            } else {
                dbDialect.safeSetString(sb, ps, col++, null);
                dbDialect.safeSetInteger(sb, ps, col++, null);
            }
            
            // custom
            if (plane.custom != null) {
                dbDialect.safeSetString(sb, ps, col++, plane.custom.getCtype());
                dbDialect.safeSetInterval(sb, ps, col++, plane.custom.getBounds());
                dbDialect.safeSetIntervalList(sb, ps, col++, plane.custom.getSamples());
                dbDialect.safeSetLong(sb, ps, col++, plane.custom.dimension);
            } else {
                dbDialect.safeSetString(sb, ps, col++, null);
                dbDialect.safeSetInterval(sb, ps, col++, null);
                dbDialect.safeSetIntervalList(sb, ps, col++, null);
                dbDialect.safeSetLong(sb, ps, col++, null);
            }

            // visibility
            if (plane.visibility != null) {
                dbDialect.safeSetInterval(sb, ps, col++, plane.visibility.getDistance());
                dbDialect.safeSetDouble(sb, ps, col++, plane.visibility.getDistributionEccentricity());
                dbDialect.safeSetDouble(sb, ps, col++, plane.visibility.getDistributionFill());
            } else {
                dbDialect.safeSetInterval(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
            }

            int mainCols = col  - 1;
            log.debug("main columns: " + (PlaneMapper.COLUMNS.length - 6) + " filled: " + mainCols);
            
            if (persistOptimisations) {
                if (plane.position != null) {
                    Position pos = plane.position;
                    dbDialect.safeSetShapeOptimization(sb, ps, col++, pos.getBounds());
                    dbDialect.safeSetPointOptimization(sb, ps, col++, pos.getBounds().getCenter());
                    dbDialect.safeSetDouble(sb, ps, col++, pos.getBounds().getArea());
                    dbDialect.safeSetDouble(sb, ps, col++, pos.getBounds().getSize());
                    if (pos.minBounds != null) {
                        dbDialect.safeSetShapeOptimization(sb, ps, col++, pos.minBounds);
                        dbDialect.safeSetPointOptimization(sb, ps, col++, pos.getBounds().getCenter());
                        dbDialect.safeSetDouble(sb, ps, col++, pos.minBounds.getArea());
                        dbDialect.safeSetDouble(sb, ps, col++, pos.minBounds.getSize());
                    } else {
                        dbDialect.safeSetShapeOptimization(sb, ps, col++, null);
                        dbDialect.safeSetPointOptimization(sb, ps, col++, null);
                        dbDialect.safeSetDouble(sb, ps, col++, null);
                        dbDialect.safeSetDouble(sb, ps, col++, null);
                    }
                    dbDialect.safeSetIntervalOptimization(sb, ps, col++, pos.maxRecoverableScale);
                    dbDialect.safeSetIntervalOptimization(sb, ps, col++, pos.resolutionBounds);
                } else {
                    dbDialect.safeSetShapeOptimization(sb, ps, col++, null);
                    dbDialect.safeSetPointOptimization(sb, ps, col++, null);
                    dbDialect.safeSetDouble(sb, ps, col++, null);
                    dbDialect.safeSetDouble(sb, ps, col++, null);
                    dbDialect.safeSetShapeOptimization(sb, ps, col++, null);
                    dbDialect.safeSetPointOptimization(sb, ps, col++, null);
                    dbDialect.safeSetDouble(sb, ps, col++, null);
                    dbDialect.safeSetDouble(sb, ps, col++, null);
                    dbDialect.safeSetIntervalOptimization(sb, ps, col++, null);
                    dbDialect.safeSetIntervalOptimization(sb, ps, col++, null);
                }
                
                if (plane.energy != null) {
                    Energy nrg = plane.energy;
                    dbDialect.safeSetIntervalOptimization(sb, ps, col++, nrg.getBounds());
                    dbDialect.safeSetIntervalListOptimization(sb, ps, col++, nrg.getSamples());
                    dbDialect.safeSetIntervalOptimization(sb, ps, col++, nrg.resolvingPowerBounds);
                    dbDialect.safeSetIntervalOptimization(sb, ps, col++, nrg.resolutionBounds);
                } else {
                    dbDialect.safeSetIntervalOptimization(sb, ps, col++, null);
                    dbDialect.safeSetIntervalListOptimization(sb, ps, col++, null);
                    dbDialect.safeSetIntervalOptimization(sb, ps, col++, null);
                    dbDialect.safeSetIntervalOptimization(sb, ps, col++, null);
                }
                
                if (plane.time != null) {
                    Time tim = plane.time;
                    dbDialect.safeSetIntervalOptimization(sb, ps, col++, tim.getBounds());
                    dbDialect.safeSetIntervalListOptimization(sb, ps, col++, tim.getSamples());
                    dbDialect.safeSetIntervalOptimization(sb, ps, col++, tim.exposureBounds);
                    dbDialect.safeSetIntervalOptimization(sb, ps, col++, tim.resolutionBounds);
                } else {
                    dbDialect.safeSetIntervalOptimization(sb, ps, col++, null);
                    dbDialect.safeSetIntervalListOptimization(sb, ps, col++, null);
                    dbDialect.safeSetIntervalOptimization(sb, ps, col++, null);
                    dbDialect.safeSetIntervalOptimization(sb, ps, col++, null);
                }
                
                // plane.polarization: no optimizations
                
                if (plane.custom != null) {
                    dbDialect.safeSetIntervalOptimization(sb, ps, col++, plane.custom.getBounds());
                    dbDialect.safeSetIntervalListOptimization(sb, ps, col++, plane.custom.getSamples());
                } else {
                    dbDialect.safeSetIntervalOptimization(sb, ps, col++, null);
                    dbDialect.safeSetIntervalListOptimization(sb, ps, col++, null);
                }
                
                if (plane.visibility != null) {
                    dbDialect.safeSetIntervalOptimization(sb, ps, col++, plane.visibility.getDistance());
                } else {
                    dbDialect.safeSetIntervalOptimization(sb, ps, col++, null);
                }
            }
            
            int optCols = col - mainCols - 1;
            log.debug("opt columns: " + PlaneMapper.OPT_COLUMNS.length + " filled: " + optCols);

            dbDialect.safeSetDate(sb, ps, col++, plane.getLastModified(), utcCalendar);
            dbDialect.safeSetDate(sb, ps, col++, plane.getMaxLastModified(), utcCalendar);
            dbDialect.safeSetURI(sb, ps, col++, plane.getMetaChecksum());
            dbDialect.safeSetURI(sb, ps, col++, plane.getAccMetaChecksum());
            dbDialect.safeSetURI(sb, ps, col++, plane.metaProducer);
            dbDialect.safeSetUUID(sb, ps, col++, plane.getID());

            if (sb != null) {
                log.debug(sb.toString());
            }
        }

        private void loadValuesInput(PreparedStatement ps)
                throws SQLException {
            StringBuilder sb = null;
            if (log.isDebugEnabled()) {
                sb = new StringBuilder();
            }

            int col = 1;
            dbDialect.safeSetUUID(sb, ps, col++, input.getOutputID());
            dbDialect.safeSetURI(sb, ps, col++, input.getInputID());
            if (sb != null) {
                log.debug(sb.toString());
            }
        }
        
        private void loadValuesForOpt(PreparedStatement ps)
                throws SQLException {
            StringBuilder sb = null;
            if (log.isDebugEnabled()) {
                sb = new StringBuilder();
            }

            int col = 1;
            dbDialect.safeSetDate(sb, ps, col++, plane.metaRelease, utcCalendar);
            dbDialect.safeSetUUID(sb, ps, col++, plane.getID());

            if (sb != null) {
                log.debug(sb.toString());
            }
        }
    }

    private class ArtifactPut implements EntityPut<Artifact>, PreparedStatementCreator {

        private boolean update;
        private Artifact artifact;
        private UUID parent;

        ArtifactPut(boolean update) {
            this.update = update;
        }

        public void execute(JdbcTemplate jdbc) {
            jdbc.update(this);
        }

        public void setValue(Artifact a, UUID parent) {
            this.artifact = a;
            this.parent = parent;
        }

        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
            String sql = null;
            if (update) {
                sql = getUpdateSQL(Artifact.class);
            } else {
                sql = getInsertSQL(Artifact.class);
            }
            PreparedStatement prep = conn.prepareStatement(sql);
            log.debug(sql);
            loadValues(prep);
            return prep;
        }

        private void loadValues(PreparedStatement ps)
                throws SQLException {
            if (artifact == null) {
                throw new IllegalStateException("null artifact");
            }

            StringBuilder sb = null;
            if (log.isDebugEnabled()) {
                sb = new StringBuilder();
            }

            int col = 1;
            dbDialect.safeSetUUID(sb, ps, col++, parent);

            dbDialect.safeSetString(sb, ps, col++, artifact.getURI().toASCIIString());
            dbDialect.safeSetString(sb, ps, col++, artifact.getUriBucket());
            dbDialect.safeSetString(sb, ps, col++, artifact.getProductType().getValue());
            dbDialect.safeSetString(sb, ps, col++, artifact.getReleaseType().getValue());
            dbDialect.safeSetString(sb, ps, col++, artifact.contentType);
            dbDialect.safeSetLong(sb, ps, col++, artifact.contentLength);
            dbDialect.safeSetURI(sb, ps, col++, artifact.contentChecksum);
            dbDialect.safeSetDate(sb, ps, col++, artifact.contentRelease, utcCalendar);
            dbDialect.safeSetArray(sb, ps, col++, artifact.getContentReadGroups());
            dbDialect.safeSetURI(sb, ps, col++, artifact.descriptionID);
            
            dbDialect.safeSetDate(sb, ps, col++, artifact.getLastModified(), utcCalendar);
            dbDialect.safeSetDate(sb, ps, col++, artifact.getMaxLastModified(), utcCalendar);
            dbDialect.safeSetURI(sb, ps, col++, artifact.getMetaChecksum());
            dbDialect.safeSetURI(sb, ps, col++, artifact.getAccMetaChecksum());
            dbDialect.safeSetURI(sb, ps, col++, artifact.metaProducer);
            dbDialect.safeSetUUID(sb, ps, col++, artifact.getID());

            if (sb != null) {
                log.debug(sb.toString());
            }
        }
    }

    private class PartPut implements EntityPut<Part>, PreparedStatementCreator {

        private boolean update;
        private Part part;
        private UUID parent;

        PartPut(boolean update) {
            this.update = update;
        }

        public void execute(JdbcTemplate jdbc) {
            jdbc.update(this);
        }

        public void setValue(Part p, UUID parent) {
            this.part = p;
            this.parent = parent;
        }

        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
            String sql = null;
            if (update) {
                sql = getUpdateSQL(Part.class);
            } else {
                sql = getInsertSQL(Part.class);
            }
            PreparedStatement prep = conn.prepareStatement(sql);
            log.debug(sql);
            loadValues(prep);
            return prep;
        }

        private void loadValues(PreparedStatement ps)
                throws SQLException {
            if (part == null) {
                throw new IllegalStateException("null part");
            }

            StringBuilder sb = null;
            if (log.isDebugEnabled()) {
                sb = new StringBuilder();
            }

            int col = 1;

            dbDialect.safeSetUUID(sb, ps, col++, parent);
            dbDialect.safeSetString(sb, ps, col++, part.getName());

            if (part.productType != null) {
                dbDialect.safeSetString(sb, ps, col++, part.productType.getValue());
            } else {
                dbDialect.safeSetString(sb, ps, col++, null);
            }

            //if (persistOptimisations) {
            //dbDialect.safeSetDate(sb, ps, col++, plane.metaRelease, utcCalendar);
            //dbDialect.safeSetMultiURI(sb, ps, col++, plane.getMetaReadGroups());
            //}

            dbDialect.safeSetDate(sb, ps, col++, part.getLastModified(), utcCalendar);
            dbDialect.safeSetDate(sb, ps, col++, part.getMaxLastModified(), utcCalendar);
            dbDialect.safeSetURI(sb, ps, col++, part.getMetaChecksum());
            dbDialect.safeSetURI(sb, ps, col++, part.getAccMetaChecksum());
            dbDialect.safeSetURI(sb, ps, col++, part.metaProducer);
            dbDialect.safeSetUUID(sb, ps, col++, part.getID());

            if (sb != null) {
                log.debug(sb.toString());
            }
        }
    }

    private class ChunkPut implements EntityPut<Chunk>, PreparedStatementCreator {

        private boolean update;
        private Chunk chunk;
        private UUID parent;

        ChunkPut(boolean update) {
            this.update = update;
        }

        public void execute(JdbcTemplate jdbc) {
            jdbc.update(this);
        }

        public void setValue(Chunk c, UUID parent) {
            this.chunk = c;
            this.parent = parent;
        }

        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
            String sql = null;
            if (update) {
                sql = getUpdateSQL(Chunk.class);
            } else {
                sql = getInsertSQL(Chunk.class);
            }
            PreparedStatement prep = conn.prepareStatement(sql);
            log.debug(sql);
            loadValues(prep);
            return prep;
        }

        private void loadValues(PreparedStatement ps)
                throws SQLException {
            if (chunk == null) {
                throw new IllegalStateException("null chunk");
            }

            StringBuilder sb = null;
            if (log.isDebugEnabled()) {
                sb = new StringBuilder();
            }

            int col = 1;
            dbDialect.safeSetUUID(sb, ps, col++, parent);

            if (chunk.productType != null) {
                dbDialect.safeSetString(sb, ps, col++, chunk.productType.getValue());
            } else {
                dbDialect.safeSetString(sb, ps, col++, null);
            }
            dbDialect.safeSetInteger(sb, ps, col++, chunk.naxis);
            dbDialect.safeSetInteger(sb, ps, col++, chunk.positionAxis1);
            dbDialect.safeSetInteger(sb, ps, col++, chunk.positionAxis2);
            dbDialect.safeSetInteger(sb, ps, col++, chunk.energyAxis);
            dbDialect.safeSetInteger(sb, ps, col++, chunk.timeAxis);
            dbDialect.safeSetInteger(sb, ps, col++, chunk.polarizationAxis);
            dbDialect.safeSetInteger(sb, ps, col++, chunk.customAxis);
            dbDialect.safeSetInteger(sb, ps, col++, chunk.observableAxis);

            if (chunk.position != null) {
                dbDialect.safeSetString(sb, ps, col++, chunk.position.getAxis().getAxis1().getCtype());
                dbDialect.safeSetString(sb, ps, col++, chunk.position.getAxis().getAxis1().getCunit());
                dbDialect.safeSetString(sb, ps, col++, chunk.position.getAxis().getAxis2().getCtype());
                dbDialect.safeSetString(sb, ps, col++, chunk.position.getAxis().getAxis2().getCunit());
                if (chunk.position.getAxis().error1 != null) {
                    dbDialect.safeSetDouble(sb, ps, col++, chunk.position.getAxis().error1.syser);
                    dbDialect.safeSetDouble(sb, ps, col++, chunk.position.getAxis().error1.rnder);
                } else {
                    dbDialect.safeSetDouble(sb, ps, col++, null);
                    dbDialect.safeSetDouble(sb, ps, col++, null);
                }
                if (chunk.position.getAxis().error2 != null) {
                    dbDialect.safeSetDouble(sb, ps, col++, chunk.position.getAxis().error2.syser);
                    dbDialect.safeSetDouble(sb, ps, col++, chunk.position.getAxis().error2.rnder);
                } else {
                    dbDialect.safeSetDouble(sb, ps, col++, null);
                    dbDialect.safeSetDouble(sb, ps, col++, null);
                }
                //range
                col += safeSet(sb, ps, col, chunk.position.getAxis().range);

                // bounds
                dbDialect.safeSetString(sb, ps, col++, CaomUtil.encodeCoordBounds2D(chunk.position.getAxis().bounds));

                // function
                col += safeSet(sb, ps, col, chunk.position.getAxis().function);

                // other fields
                dbDialect.safeSetString(sb, ps, col++, chunk.position.coordsys);
                dbDialect.safeSetDouble(sb, ps, col++, chunk.position.equinox);
                dbDialect.safeSetDouble(sb, ps, col++, chunk.position.resolution);
            } else {
                dbDialect.safeSetString(sb, ps, col++, null);
                dbDialect.safeSetString(sb, ps, col++, null);
                dbDialect.safeSetString(sb, ps, col++, null);
                dbDialect.safeSetString(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                //range
                col += safeSet(sb, ps, col, (CoordRange2D) null);
                // bounds
                dbDialect.safeSetString(sb, ps, col++, null);
                // function
                col += safeSet(sb, ps, col, (CoordFunction2D) null);
                // other fields
                dbDialect.safeSetString(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
            }

            if (chunk.energy != null) {
                dbDialect.safeSetString(sb, ps, col++, chunk.energy.getAxis().getAxis().getCtype());
                dbDialect.safeSetString(sb, ps, col++, chunk.energy.getAxis().getAxis().getCunit());
                if (chunk.energy.getAxis().error != null) {
                    dbDialect.safeSetDouble(sb, ps, col++, chunk.energy.getAxis().error.syser);
                    dbDialect.safeSetDouble(sb, ps, col++, chunk.energy.getAxis().error.rnder);
                } else {
                    dbDialect.safeSetDouble(sb, ps, col++, null);
                    dbDialect.safeSetDouble(sb, ps, col++, null);
                }
                //range
                col += safeSet(sb, ps, col, chunk.energy.getAxis().range);
                // bounds
                dbDialect.safeSetString(sb, ps, col++, CaomUtil.encodeCoordBounds1D(chunk.energy.getAxis().bounds));
                // function
                col += safeSet(sb, ps, col, chunk.energy.getAxis().function);

                // other fields
                dbDialect.safeSetString(sb, ps, col++, chunk.energy.getSpecsys());
                dbDialect.safeSetString(sb, ps, col++, chunk.energy.ssysobs);
                dbDialect.safeSetString(sb, ps, col++, chunk.energy.ssyssrc);
                dbDialect.safeSetDouble(sb, ps, col++, chunk.energy.restfrq);
                dbDialect.safeSetDouble(sb, ps, col++, chunk.energy.restwav);
                dbDialect.safeSetDouble(sb, ps, col++, chunk.energy.velosys);
                dbDialect.safeSetDouble(sb, ps, col++, chunk.energy.zsource);
                dbDialect.safeSetDouble(sb, ps, col++, chunk.energy.velang);
                dbDialect.safeSetString(sb, ps, col++, chunk.energy.bandpassName);
                dbDialect.safeSetDouble(sb, ps, col++, chunk.energy.resolvingPower);
                if (chunk.energy.transition != null) {
                    dbDialect.safeSetString(sb, ps, col++, chunk.energy.transition.getSpecies());
                    dbDialect.safeSetString(sb, ps, col++, chunk.energy.transition.getTransition());
                } else {
                    dbDialect.safeSetString(sb, ps, col++, null);
                    dbDialect.safeSetString(sb, ps, col++, null);
                }
            } else {
                dbDialect.safeSetString(sb, ps, col++, null);
                dbDialect.safeSetString(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                //range
                col += safeSet(sb, ps, col, (CoordRange1D) null);
                // bounds
                dbDialect.safeSetString(sb, ps, col++, null);
                // function
                col += safeSet(sb, ps, col, (CoordFunction1D) null);

                // other fields
                dbDialect.safeSetString(sb, ps, col++, null);
                dbDialect.safeSetString(sb, ps, col++, null);
                dbDialect.safeSetString(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetString(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetString(sb, ps, col++, null);
                dbDialect.safeSetString(sb, ps, col++, null);
            }

            if (chunk.time != null) {
                dbDialect.safeSetString(sb, ps, col++, chunk.time.getAxis().getAxis().getCtype());
                dbDialect.safeSetString(sb, ps, col++, chunk.time.getAxis().getAxis().getCunit());
                if (chunk.time.getAxis().error != null) {
                    dbDialect.safeSetDouble(sb, ps, col++, chunk.time.getAxis().error.syser);
                    dbDialect.safeSetDouble(sb, ps, col++, chunk.time.getAxis().error.rnder);
                } else {
                    dbDialect.safeSetDouble(sb, ps, col++, null);
                    dbDialect.safeSetDouble(sb, ps, col++, null);
                }
                //range
                col += safeSet(sb, ps, col, chunk.time.getAxis().range);
                // bounds
                dbDialect.safeSetString(sb, ps, col++, CaomUtil.encodeCoordBounds1D(chunk.time.getAxis().bounds));
                // function
                col += safeSet(sb, ps, col, chunk.time.getAxis().function);
                // other fields
                dbDialect.safeSetString(sb, ps, col++, chunk.time.timesys);
                dbDialect.safeSetString(sb, ps, col++, chunk.time.trefpos);
                dbDialect.safeSetDouble(sb, ps, col++, chunk.time.mjdref);
                dbDialect.safeSetDouble(sb, ps, col++, chunk.time.exposure);
                dbDialect.safeSetDouble(sb, ps, col++, chunk.time.resolution);
            } else {
                dbDialect.safeSetString(sb, ps, col++, null);
                dbDialect.safeSetString(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                //range
                col += safeSet(sb, ps, col, (CoordRange1D) null);
                // bounds
                dbDialect.safeSetString(sb, ps, col++, null);
                // function
                col += safeSet(sb, ps, col, (CoordFunction1D) null);
                // other fields
                dbDialect.safeSetString(sb, ps, col++, null);
                dbDialect.safeSetString(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
            }

            if (chunk.polarization != null) {
                dbDialect.safeSetString(sb, ps, col++, chunk.polarization.getAxis().getAxis().getCtype());
                dbDialect.safeSetString(sb, ps, col++, chunk.polarization.getAxis().getAxis().getCunit());
                if (chunk.polarization.getAxis().error != null) {
                    dbDialect.safeSetDouble(sb, ps, col++, chunk.polarization.getAxis().error.syser);
                    dbDialect.safeSetDouble(sb, ps, col++, chunk.polarization.getAxis().error.rnder);
                } else {
                    dbDialect.safeSetDouble(sb, ps, col++, null);
                    dbDialect.safeSetDouble(sb, ps, col++, null);
                }
                //range
                col += safeSet(sb, ps, col, chunk.polarization.getAxis().range);
                // bounds
                dbDialect.safeSetString(sb, ps, col++, CaomUtil.encodeCoordBounds1D(chunk.polarization.getAxis().bounds));
                // function
                col += safeSet(sb, ps, col, chunk.polarization.getAxis().function);
            } else {
                dbDialect.safeSetString(sb, ps, col++, null);
                dbDialect.safeSetString(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                //range
                col += safeSet(sb, ps, col, (CoordRange1D) null);
                // bounds
                dbDialect.safeSetString(sb, ps, col++, null);
                // function
                col += safeSet(sb, ps, col, (CoordFunction1D) null);
                // other fields
            }
            
            if (chunk.custom != null) {
                dbDialect.safeSetString(sb, ps, col++, chunk.custom.getAxis().getAxis().getCtype());
                dbDialect.safeSetString(sb, ps, col++, chunk.custom.getAxis().getAxis().getCunit());
                if (chunk.custom.getAxis().error != null) {
                    dbDialect.safeSetDouble(sb, ps, col++, chunk.custom.getAxis().error.syser);
                    dbDialect.safeSetDouble(sb, ps, col++, chunk.custom.getAxis().error.rnder);
                } else {
                    dbDialect.safeSetDouble(sb, ps, col++, null);
                    dbDialect.safeSetDouble(sb, ps, col++, null);
                }
                //range
                col += safeSet(sb, ps, col, chunk.custom.getAxis().range);
                // bounds
                dbDialect.safeSetString(sb, ps, col++, CaomUtil.encodeCoordBounds1D(chunk.custom.getAxis().bounds));
                // function
                col += safeSet(sb, ps, col, chunk.custom.getAxis().function);
            } else {
                dbDialect.safeSetString(sb, ps, col++, null);
                dbDialect.safeSetString(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                //range
                col += safeSet(sb, ps, col, (CoordRange1D) null);
                // bounds
                dbDialect.safeSetString(sb, ps, col++, null);
                // function
                col += safeSet(sb, ps, col, (CoordFunction1D) null);
                // other fields
            }

            if (chunk.observable != null) {
                dbDialect.safeSetString(sb, ps, col++, chunk.observable.getDependent().getAxis().getCtype());
                dbDialect.safeSetString(sb, ps, col++, chunk.observable.getDependent().getAxis().getCunit());
                dbDialect.safeSetLong(sb, ps, col++, chunk.observable.getDependent().getBin());
                if (chunk.observable.independent != null) {
                    dbDialect.safeSetString(sb, ps, col++, chunk.observable.independent.getAxis().getCtype());
                    dbDialect.safeSetString(sb, ps, col++, chunk.observable.independent.getAxis().getCunit());
                    dbDialect.safeSetLong(sb, ps, col++, chunk.observable.independent.getBin());
                } else {
                    dbDialect.safeSetString(sb, ps, col++, null);
                    dbDialect.safeSetString(sb, ps, col++, null);
                    dbDialect.safeSetLong(sb, ps, col++, null);
                }
            } else {
                dbDialect.safeSetString(sb, ps, col++, null);
                dbDialect.safeSetString(sb, ps, col++, null);
                dbDialect.safeSetLong(sb, ps, col++, null);
                dbDialect.safeSetString(sb, ps, col++, null);
                dbDialect.safeSetString(sb, ps, col++, null);
                dbDialect.safeSetLong(sb, ps, col++, null);
            }

            //if (persistOptimisations) {
            //dbDialect.safeSetDate(sb, ps, col++, plane.metaRelease, utcCalendar);
            //dbDialect.safeSetMultiURI(sb, ps, col++, plane.getMetaReadGroups());
            //}

            dbDialect.safeSetDate(sb, ps, col++, chunk.getLastModified(), utcCalendar);
            dbDialect.safeSetDate(sb, ps, col++, chunk.getMaxLastModified(), utcCalendar);
            dbDialect.safeSetURI(sb, ps, col++, chunk.getMetaChecksum());
            dbDialect.safeSetURI(sb, ps, col++, chunk.getAccMetaChecksum());
            dbDialect.safeSetURI(sb, ps, col++, chunk.metaProducer);
            dbDialect.safeSetUUID(sb, ps, col++, chunk.getID());

            if (sb != null) {
                log.debug(sb.toString());
            }
        }

        private int safeSet(StringBuilder sb, PreparedStatement ps, int col, CoordRange1D r)
                throws SQLException {
            if (r != null) {
                dbDialect.safeSetDouble(sb, ps, col++, r.getStart().pix);
                dbDialect.safeSetDouble(sb, ps, col++, r.getStart().val);
                dbDialect.safeSetDouble(sb, ps, col++, r.getEnd().pix);
                dbDialect.safeSetDouble(sb, ps, col++, r.getEnd().val);
            } else {
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
            }
            return 4;
        }

        private int safeSet(StringBuilder sb, PreparedStatement ps, int col, CoordFunction1D f)
                throws SQLException {
            if (f != null) {
                dbDialect.safeSetLong(sb, ps, col++, f.getNaxis());
                dbDialect.safeSetDouble(sb, ps, col++, f.getRefCoord().pix);
                dbDialect.safeSetDouble(sb, ps, col++, f.getRefCoord().val);
                dbDialect.safeSetDouble(sb, ps, col++, f.getDelta());
            } else {
                dbDialect.safeSetLong(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
            }
            return 4;
        }

        private int safeSet(StringBuilder sb, PreparedStatement ps, int col, CoordRange2D r)
                throws SQLException {
            if (r != null) {
                dbDialect.safeSetDouble(sb, ps, col++, r.getStart().getCoord1().pix);
                dbDialect.safeSetDouble(sb, ps, col++, r.getStart().getCoord1().val);
                dbDialect.safeSetDouble(sb, ps, col++, r.getStart().getCoord2().pix);
                dbDialect.safeSetDouble(sb, ps, col++, r.getStart().getCoord2().val);
                dbDialect.safeSetDouble(sb, ps, col++, r.getEnd().getCoord1().pix);
                dbDialect.safeSetDouble(sb, ps, col++, r.getEnd().getCoord1().val);
                dbDialect.safeSetDouble(sb, ps, col++, r.getEnd().getCoord2().pix);
                dbDialect.safeSetDouble(sb, ps, col++, r.getEnd().getCoord2().val);
            } else {
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
            }
            return 8;
        }

        private int safeSet(StringBuilder sb, PreparedStatement ps, int col, CoordFunction2D f)
                throws SQLException {
            if (f != null) {
                dbDialect.safeSetLong(sb, ps, col++, f.getDimension().naxis1);
                dbDialect.safeSetLong(sb, ps, col++, f.getDimension().naxis2);
                dbDialect.safeSetDouble(sb, ps, col++, f.getRefCoord().getCoord1().pix);
                dbDialect.safeSetDouble(sb, ps, col++, f.getRefCoord().getCoord1().val);
                dbDialect.safeSetDouble(sb, ps, col++, f.getRefCoord().getCoord2().pix);
                dbDialect.safeSetDouble(sb, ps, col++, f.getRefCoord().getCoord2().val);
                dbDialect.safeSetDouble(sb, ps, col++, f.getCd11());
                dbDialect.safeSetDouble(sb, ps, col++, f.getCd12());
                dbDialect.safeSetDouble(sb, ps, col++, f.getCd21());
                dbDialect.safeSetDouble(sb, ps, col++, f.getCd22());
            } else {
                dbDialect.safeSetLong(sb, ps, col++, null);
                dbDialect.safeSetLong(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
                dbDialect.safeSetDouble(sb, ps, col++, null);
            }
            return 10;
        }
    }

    String getColumns(Class c) {
        return getColumns(c, null);
    }

    String getColumns(Class c, String alias) {
        String[] cols = columnMap.get(c);
        if (cols != null) {
            StringBuilder sb = new StringBuilder();
            if (alias == null) {
                alias = getAlias(c);
            }
            for (int i = 0; i < cols.length; i++) {
                sb.append(alias);
                sb.append(".");
                sb.append(cols[i]);
                sb.append(",");
            }
            return sb.substring(0, sb.length() - 1); // strip trailing comma
        }
        return null;
    }

    public String getTable(Class c) {
        String tabName = (String) tableMap.get(c);
        StringBuilder sb = new StringBuilder();
        if (database != null) {
            sb.append(database);
            sb.append(".");
        }
        if (schema != null) {
            sb.append(schema);
            sb.append(".");
        }
        sb.append(tabName);
        return sb.toString();
    }

    public String getAlias(Class c) {
        return (String) aliasMap.get(c);
    }

    protected String getFrom(Class c) {
        String tab = getTable(c);
        StringBuilder sb = new StringBuilder();
        sb.append(tab);
        sb.append(" AS ");
        sb.append(getAlias(c));
        return sb.toString();
    }
    
    public String getFrom(Class c, int depth, boolean skeleton) {
        log.debug("getFrom: " + c + ", depth = " + depth);

        String a1 = getAlias(c);
        String f1 = getFrom(c);

        if (depth == 1) {
            return f1;
        }

        StringBuilder sb = new StringBuilder();
        sb.append(f1);
        Class child = null;
        if (Observation.class.isAssignableFrom(c) || ObservationSkeleton.class.isAssignableFrom(c)) {
            if (skeleton) {
                child = PlaneSkeleton.class;
            } else {
                child = Plane.class;
            }
            log.debug("getFrom: observation JOIN plane");
            // join to plane
            String a2 = getAlias(child);
            String f2 = getFrom(child, depth - 1, skeleton); // recursive
            sb.append(" LEFT OUTER JOIN ");
            sb.append(f2);
            sb.append(" ON ");
            sb.append(a1);
            sb.append(".obsID = ");
            sb.append(a2);
            sb.append(".obsID");
        } else if (Plane.class.equals(c) || PlaneSkeleton.class.equals(c)) {
            if (skeleton) {
                child = ArtifactSkeleton.class;
            } else {
                child = Artifact.class;
            }
            log.debug("getFrom: plane JOIN artifact");
            // join to artifact
            String a2 = getAlias(child);
            String f2 = getFrom(child, depth - 1, skeleton);
            sb.append(" LEFT OUTER JOIN ");
            sb.append(f2);
            sb.append(" ON ");
            sb.append(a1);
            sb.append(".planeID = ");
            sb.append(a2);
            sb.append(".planeID");
        } else if (Artifact.class.equals(c) || ArtifactSkeleton.class.equals(c)) {
            if (skeleton) {
                child = PartSkeleton.class;
            } else {
                child = Part.class;
            }
            log.debug("getFrom: artifact JOIN part");
            // join to artifact
            String a2 = getAlias(child);
            String f2 = getFrom(child, depth - 1, skeleton);
            sb.append(" LEFT OUTER JOIN ");
            sb.append(f2);
            sb.append(" ON ");
            sb.append(a1);
            sb.append(".artifactID = ");
            sb.append(a2);
            sb.append(".artifactID");
        } else if (Part.class.equals(c) || PartSkeleton.class.equals(c)) {
            if (skeleton) {
                child = ChunkSkeleton.class;
            } else {
                child = Chunk.class;
            }
            log.debug("getFrom: part JOIN chunk");
            // join to artifact
            String a2 = getAlias(child);
            String f2 = getFrom(child, depth - 1, skeleton);
            sb.append(" LEFT OUTER JOIN ");
            sb.append(f2);
            sb.append(" ON ");
            sb.append(a1);
            sb.append(".partID = ");
            sb.append(a2);
            sb.append(".partID");
        }
        return sb.toString();
    }

    private String getOrderColumns(int depth) {
        StringBuilder sb = new StringBuilder();
        if (depth > 1) {
            sb.append(getAlias(Observation.class));
            sb.append(".obsID");
        }
        if (depth > 2) {
            sb.append(",");
            sb.append(getAlias(Plane.class));
            sb.append(".planeID");
        }
        if (depth > 3) {
            sb.append(",");
            sb.append(getAlias(Artifact.class));
            sb.append(".artifactID");
        }
        if (depth > 4) {
            sb.append(",");
            sb.append(getAlias(Part.class));
            sb.append(".partID");
        }
        if (sb.length() > 0) {
            return sb.toString();
        }
        return null;
    }

    private String getSkeletonOrderColumns(int depth) {
        StringBuilder sb = new StringBuilder();
        if (depth > 1) {
            sb.append(getAlias(ObservationSkeleton.class));
            sb.append(".obsID");
        }
        if (depth > 2) {
            sb.append(",");
            sb.append(getAlias(PlaneSkeleton.class));
            sb.append(".planeID");
        }
        if (depth > 3) {
            sb.append(",");
            sb.append(getAlias(ArtifactSkeleton.class));
            sb.append(".artifactID");
        }
        if (depth > 4) {
            sb.append(",");
            sb.append(getAlias(PartSkeleton.class));
            sb.append(".partID");
        }
        if (sb.length() > 0) {
            return sb.toString();
        }
        return null;
    }

    protected StringBuilder getObservationSelect(int depth, boolean skeleton) {
        log.debug("getObservationSelect: " + depth + "," + skeleton);
        StringBuilder sb = getObservationSelectList(depth, skeleton);
        sb.append(" FROM ");
        if (skeleton) {
            sb.append(getFrom(ObservationSkeleton.class, depth, skeleton));
        } else {
            sb.append(getFrom(Observation.class, depth, skeleton));
        }
        return sb;
    }

    protected StringBuilder getObservationSelectList(int depth, boolean skeleton) {
        StringBuilder sb = new StringBuilder();
        if (skeleton) {
            sb.append(getColumns(ObservationSkeleton.class));
        } else {
            sb.append(getColumns(Observation.class));
        }

        // extra columns depend on depth: these sets of columns could be 
        // refactored into a recursive function, getColumnList(Class, int)
        if (depth > 1) {
            sb.append(",");
            if (skeleton) {
                sb.append(getColumns(PlaneSkeleton.class));
            } else {
                sb.append(getColumns(Plane.class));
            }
        }
        // Metric(s)?
        if (depth > 2) {
            sb.append(",");
            if (skeleton) {
                sb.append(getColumns(ArtifactSkeleton.class));
            } else {
                sb.append(getColumns(Artifact.class));
            }
        }
        if (depth > 3) {
            sb.append(",");
            if (skeleton) {
                sb.append(getColumns(PartSkeleton.class));
            } else {
                sb.append(getColumns(Part.class));
            }
        }
        if (depth > 4) {
            sb.append(",");
            if (skeleton) {
                sb.append(getColumns(ChunkSkeleton.class));
            } else {
                sb.append(getColumns(Chunk.class));
            }
        }
        return sb;
    }

    protected StringBuilder getEntitySelect(Class c) {
        StringBuilder sb = new StringBuilder();
        sb.append(getColumns(c));
        sb.append(" FROM ");
        sb.append(getFrom(c));
        return sb;
    }

    public ObservationExtractor getObservationExtractor() {
        return new ObservationExtractor(this);
    }

    public RowMapper getObservationStateMapper() {
        return new ObservationStateMapper();
    }
    
    public ResultSetExtractor<ObservationState> getObservationStateExtractor() {
        return new ObservationStateExtractor();
    }

    public ResultSetExtractor getSkeletonExtractor(Class<? extends Skeleton> c) {
        if (c.equals(ObservationSkeleton.class)) {
            return new ObservationSkeletonExtractor();
        }

        throw new UnsupportedOperationException("getSkeletonExtractor: " + c.getName());
    }

    public RowMapper getTimestampRowMapper() {
        return new TimestampRowMapper(utcCalendar);
    }

    private static class ClassComp implements Comparator<Class> {

        public int compare(Class o1, Class o2) {
            Class c1 = (Class) o1;
            Class c2 = (Class) o2;
            return c1.getSimpleName().compareTo(c2.getSimpleName());
        }
    }

    PartialRowMapper<Observation> getObservationMapper() {
        return new ObservationMapper(dbDialect, persistOptimisations);
    }

    PartialRowMapper<Plane> getPlaneMapper() {
        return new PlaneMapper(dbDialect, persistOptimisations);
    }

    public PartialRowMapper<Artifact> getArtifactMapper() {
        return new ArtifactMapper(dbDialect);
    }

    PartialRowMapper<Part> getPartMapper() {
        return new PartMapper();
    }

    PartialRowMapper<Chunk> getChunkMapper() {
        return new ChunkMapper();
    }

    class PartMapper implements PartialRowMapper<Part> {
        @Override
        public UUID getID(ResultSet rs, int row, int offset) throws SQLException {
            int n = getColumnCount() - 1;
            UUID id = Util.getUUID(rs, offset + n);
            log.debug("found: entity ID = " + id);
            return id;
        }
        
        @Override
        public int getColumnCount() {
            return columnMap.get(Part.class).length;
        }

        public Part mapRow(ResultSet rs, int row)
                throws SQLException {
            return mapRow(rs, row, 1);
        }

        /**
         * Map columns from the current row into an Artifact, starting at the
         * specified column offset.
         *
         * @param rs
         * @param row
         * @param col JDBC column offset where plane columns are located
         * @return
         * @throws java.sql.SQLException
         */
        public Part mapRow(ResultSet rs, int row, int col)
                throws SQLException {
            UUID artifactID = Util.getUUID(rs, col++); // FK
            if (artifactID == null) {
                return null;
            }

            //col += 2; // skip ancestor(s)

            String name = rs.getString(col++);
            Part p = new Part(name);
            log.debug("found part: " + p);

            String pt = rs.getString(col++);
            log.debug("found p.productType = " + pt);
            if (pt != null) {
                p.productType = DataLinkSemantics.toValue(pt);
            }

            if (persistOptimisations) {
                col += numOptPartColumns;
            }

            Date lastModified = Util.getDate(rs, col++, utcCalendar);
            Date maxLastModified = Util.getDate(rs, col++, utcCalendar);
            CaomUtil.assignLastModified(p, lastModified, "lastModified");
            CaomUtil.assignLastModified(p, maxLastModified, "maxLastModified");

            URI metaChecksum = Util.getURI(rs, col++);
            URI accMetaChecksum = Util.getURI(rs, col++);
            CaomUtil.assignMetaChecksum(p, metaChecksum, "metaChecksum");
            CaomUtil.assignMetaChecksum(p, accMetaChecksum, "accMetaChecksum");
            p.metaProducer = Util.getURI(rs, col++);
            
            UUID id = Util.getUUID(rs, col++);
            log.debug("found: part.id = " + id);
            CaomUtil.assignID(p, id);

            return p;
        }
    }

    class ChunkMapper implements PartialRowMapper<Chunk> {
        @Override
        public UUID getID(ResultSet rs, int row, int offset) throws SQLException {
            int n = getColumnCount() - 1;
            UUID id = Util.getUUID(rs, offset + n);
            log.debug("found: entity ID = " + id);
            return id;
        }
        
        @Override
        public int getColumnCount() {
            return columnMap.get(Chunk.class).length;
        }

        public Chunk mapRow(ResultSet rs, int row)
                throws SQLException {
            return mapRow(rs, row, 1);
        }

        /**
         * Map columns from the current row into an Artifact, starting at the
         * specified column offset.
         *
         * @param rs
         * @param row
         * @param col JDBC column offset where plane columns are located
         * @return
         * @throws java.sql.SQLException
         */
        public Chunk mapRow(ResultSet rs, int row, int col)
                throws SQLException {
            UUID partID = Util.getUUID(rs, col++); // FK
            if (partID == null) {
                return null;
            }

            //col += 3; // skip ancestor(s)

            Chunk c = new Chunk();

            String pt = rs.getString(col++);
            if (pt != null) {
                c.productType = DataLinkSemantics.toValue(pt);
            }
            c.naxis = Util.getInteger(rs, col++);
            c.positionAxis1 = Util.getInteger(rs, col++);
            c.positionAxis2 = Util.getInteger(rs, col++);
            c.energyAxis = Util.getInteger(rs, col++);
            c.timeAxis = Util.getInteger(rs, col++);
            c.polarizationAxis = Util.getInteger(rs, col++);
            c.customAxis = Util.getInteger(rs, col++);
            c.observableAxis = Util.getInteger(rs, col++);

            // position
            String posctype1 = rs.getString(col++);
            String poscunit1 = rs.getString(col++);
            String posctype2 = rs.getString(col++);
            String poscunit2 = rs.getString(col++);
            Double e1s = Util.getDouble(rs, col++);
            Double e1r = Util.getDouble(rs, col++);
            Double e2s = Util.getDouble(rs, col++);
            Double e2r = Util.getDouble(rs, col++);
            CoordRange2D posrange = null; //Util.decodeCoordRange2D(rs.getString(col++));
            Double start1pix = Util.getDouble(rs, col++);
            Double start1val = Util.getDouble(rs, col++);
            Double start2pix = Util.getDouble(rs, col++);
            Double start2val = Util.getDouble(rs, col++);
            Double end1pix = Util.getDouble(rs, col++);
            Double end1val = Util.getDouble(rs, col++);
            Double end2pix = Util.getDouble(rs, col++);
            Double end2val = Util.getDouble(rs, col++);
            if (start1pix != null) {
                posrange = new CoordRange2D(
                        new Coord2D(new RefCoord(start1pix, start1val), new RefCoord(start2pix, start2val)),
                        new Coord2D(new RefCoord(end1pix, end1val), new RefCoord(end2pix, end2val)));
            }
            CoordBounds2D posbounds = CaomUtil.decodeCoordBounds2D(rs.getString(col++));
            CoordFunction2D posfunction = null; //Util.decodeCoordFunction2D(rs.getString(col++));
            Long naxis1 = Util.getLong(rs, col++);
            Long naxis2 = Util.getLong(rs, col++);
            Double c1pix = Util.getDouble(rs, col++);
            Double c1val = Util.getDouble(rs, col++);
            Double c2pix = Util.getDouble(rs, col++);
            Double c2val = Util.getDouble(rs, col++);
            Double cd11 = Util.getDouble(rs, col++);
            Double cd12 = Util.getDouble(rs, col++);
            Double cd21 = Util.getDouble(rs, col++);
            Double cd22 = Util.getDouble(rs, col++);
            if (naxis1 != null) {
                posfunction = new CoordFunction2D(new Dimension2D(naxis1, naxis2),
                        new Coord2D(new RefCoord(c1pix, c1val), new RefCoord(c2pix, c2val)),
                        cd11, cd12, cd21, cd22);
            }

            String coordsys = rs.getString(col++);
            Double equinox = Util.getDouble(rs, col++);
            Double posres = Util.getDouble(rs, col++);
            if (posctype1 != null) {
                CoordAxis2D axis = new CoordAxis2D(new Axis(posctype1, poscunit1), new Axis(posctype2, poscunit2));
                if (e1s != null || e1r != null) {
                    axis.error1 = new CoordError(e1s, e1r);
                }
                if (e2s != null || e2r != null) {
                    axis.error2 = new CoordError(e2s, e2r);
                }
                axis.range = posrange;
                axis.bounds = posbounds;
                axis.function = posfunction;
                c.position = new SpatialWCS(axis);
                c.position.coordsys = coordsys;
                c.position.equinox = equinox;
                c.position.resolution = posres;
            }

            // energy
            String enctype = rs.getString(col++);
            String encunit = rs.getString(col++);
            Double enes = Util.getDouble(rs, col++);
            Double ener = Util.getDouble(rs, col++);
            CoordRange1D enrange = null; //Util.decodeCoordRange1D(rs.getString(col++));
            Double pix1 = Util.getDouble(rs, col++);
            Double val1 = Util.getDouble(rs, col++);
            Double pix2 = Util.getDouble(rs, col++);
            Double val2 = Util.getDouble(rs, col++);
            if (pix1 != null) {
                enrange = new CoordRange1D(new RefCoord(pix1, val1), new RefCoord(pix2, val2));
            }

            CoordBounds1D enbounds = CaomUtil.decodeCoordBounds1D(rs.getString(col++));
            CoordFunction1D enfunction = null; //Util.decodeCoordFunction1D(rs.getString(col++));
            Long naxis = Util.getLong(rs, col++);
            Double pix = Util.getDouble(rs, col++);
            Double val = Util.getDouble(rs, col++);
            Double delta = Util.getDouble(rs, col++);
            if (naxis != null) {
                enfunction = new CoordFunction1D(naxis, delta, new RefCoord(pix, val));
            }
            String specsys = rs.getString(col++);
            String ssysobs = rs.getString(col++);
            String ssyssrc = rs.getString(col++);
            Double restfrq = Util.getDouble(rs, col++);
            Double restwav = Util.getDouble(rs, col++);
            Double velosys = Util.getDouble(rs, col++);
            Double zsource = Util.getDouble(rs, col++);
            Double velang = Util.getDouble(rs, col++);
            String bandpassName = rs.getString(col++);
            Double enres = Util.getDouble(rs, col++);
            String species = rs.getString(col++);
            String trans = rs.getString(col++);
            if (enctype != null) {
                CoordAxis1D axis = new CoordAxis1D(new Axis(enctype, encunit));
                if (enes != null || ener != null) {
                    axis.error = new CoordError(enes, ener);
                }
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
                if (species != null) {
                    c.energy.transition = new EnergyTransition(species, trans);
                }
            }

            // time
            final String tctype = rs.getString(col++);
            final String tcunit = rs.getString(col++);
            Double tes = Util.getDouble(rs, col++);
            Double ter = Util.getDouble(rs, col++);
            CoordRange1D trange = null; // Util.decodeCoordRange1D(rs.getString(col++));
            pix1 = Util.getDouble(rs, col++);
            val1 = Util.getDouble(rs, col++);
            pix2 = Util.getDouble(rs, col++);
            val2 = Util.getDouble(rs, col++);
            if (pix1 != null) {
                trange = new CoordRange1D(new RefCoord(pix1, val1), new RefCoord(pix2, val2));
            }

            CoordBounds1D tbounds = CaomUtil.decodeCoordBounds1D(rs.getString(col++));
            CoordFunction1D tfunction = null; // Util.decodeCoordFunction1D(rs.getString(col++));
            naxis = Util.getLong(rs, col++);
            pix = Util.getDouble(rs, col++);
            val = Util.getDouble(rs, col++);
            delta = Util.getDouble(rs, col++);
            if (naxis != null) {
                tfunction = new CoordFunction1D(naxis, delta, new RefCoord(pix, val));
            }

            String timesys = rs.getString(col++);
            String trefpos = rs.getString(col++);
            Double mjdref = Util.getDouble(rs, col++);
            Double exposure = Util.getDouble(rs, col++);
            Double tres = Util.getDouble(rs, col++);
            if (tctype != null) {
                CoordAxis1D axis = new CoordAxis1D(new Axis(tctype, tcunit));
                if (tes != null || ter != null) {
                    axis.error = new CoordError(tes, ter);
                }
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
            final String pctype = rs.getString(col++);
            final String pcunit = rs.getString(col++);
            Double pes = Util.getDouble(rs, col++);
            Double per = Util.getDouble(rs, col++);
            CoordRange1D prange = null; // Util.decodeCoordRange1D(rs.getString(col++));
            pix1 = Util.getDouble(rs, col++);
            val1 = Util.getDouble(rs, col++);
            pix2 = Util.getDouble(rs, col++);
            val2 = Util.getDouble(rs, col++);
            if (pix1 != null) {
                prange = new CoordRange1D(new RefCoord(pix1, val1), new RefCoord(pix2, val2));
            }

            CoordBounds1D pbounds = CaomUtil.decodeCoordBounds1D(rs.getString(col++));
            CoordFunction1D pfunction = null; // Util.decodeCoordFunction1D(rs.getString(col++));
            naxis = Util.getLong(rs, col++);
            pix = Util.getDouble(rs, col++);
            val = Util.getDouble(rs, col++);
            delta = Util.getDouble(rs, col++);
            if (naxis != null) {
                pfunction = new CoordFunction1D(naxis, delta, new RefCoord(pix, val));
            }

            if (pctype != null) {
                CoordAxis1D axis = new CoordAxis1D(new Axis(pctype, pcunit));
                if (pes != null || per != null) {
                    axis.error = new CoordError(pes, per);
                }
                axis.range = prange;
                axis.bounds = pbounds;
                axis.function = pfunction;
                c.polarization = new PolarizationWCS(axis);
            }
            
            // custom
            final String cctype = rs.getString(col++);
            final String ccunit = rs.getString(col++);
            Double ces = Util.getDouble(rs, col++);
            Double cer = Util.getDouble(rs, col++);
            CoordRange1D crange = null; // Util.decodeCoordRange1D(rs.getString(col++));
            pix1 = Util.getDouble(rs, col++);
            val1 = Util.getDouble(rs, col++);
            pix2 = Util.getDouble(rs, col++);
            val2 = Util.getDouble(rs, col++);
            if (pix1 != null) {
                crange = new CoordRange1D(new RefCoord(pix1, val1), new RefCoord(pix2, val2));
            }

            CoordBounds1D cbounds = CaomUtil.decodeCoordBounds1D(rs.getString(col++));
            CoordFunction1D cfunction = null; // Util.decodeCoordFunction1D(rs.getString(col++));
            naxis = Util.getLong(rs, col++);
            pix = Util.getDouble(rs, col++);
            val = Util.getDouble(rs, col++);
            delta = Util.getDouble(rs, col++);
            if (naxis != null) {
                cfunction = new CoordFunction1D(naxis, delta, new RefCoord(pix, val));
            }

            if (cctype != null) {
                CoordAxis1D axis = new CoordAxis1D(new Axis(cctype, ccunit));
                if (ces != null || cer != null) {
                    axis.error = new CoordError(ces, cer);
                }
                axis.range = crange;
                axis.bounds = cbounds;
                axis.function = cfunction;
                c.custom = new CustomWCS(axis);
            }

            // observable
            String oda = rs.getString(col++);
            String odu = rs.getString(col++);
            Long odb = Util.getLong(rs, col++);
            String oia = rs.getString(col++);
            String oiu = rs.getString(col++);
            Long oib = Util.getLong(rs, col++);
            if (oda != null) {
                Slice dep = new Slice(new Axis(oda, odu), odb);
                c.observable = new ObservableAxis(dep);
                if (oia != null) {
                    c.observable.independent = new Slice(new Axis(oia, oiu), oib);
                }
            }

            if (persistOptimisations) {
                col += numOptChunkColumns;
            }

            Date lastModified = Util.getDate(rs, col++, utcCalendar);
            Date maxLastModified = Util.getDate(rs, col++, utcCalendar);
            CaomUtil.assignLastModified(c, lastModified, "lastModified");
            CaomUtil.assignLastModified(c, maxLastModified, "maxLastModified");

            URI metaChecksum = Util.getURI(rs, col++);
            URI accMetaChecksum = Util.getURI(rs, col++);
            CaomUtil.assignMetaChecksum(c, metaChecksum, "metaChecksum");
            CaomUtil.assignMetaChecksum(c, accMetaChecksum, "accMetaChecksum");
            c.metaProducer = Util.getURI(rs, col++);
            
            UUID id = Util.getUUID(rs, col++);
            log.debug("found: chunk.id = " + id);
            CaomUtil.assignID(c, id);

            return c;
        }
    }
}
