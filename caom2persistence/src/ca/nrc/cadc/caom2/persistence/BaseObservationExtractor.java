
package ca.nrc.cadc.caom2.persistence;

import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.Chunk;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.Part;
import ca.nrc.cadc.caom2.Plane;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;

/**
 * Class that uses PartialRowMapper(s) to construct a List<Observation> out of a ResultSet.
 * 
 * @author pdowler
 */
public class BaseObservationExtractor implements ObservationExtractor
{
    private static Logger log = Logger.getLogger(BaseObservationExtractor.class);
    
    private PartialRowMapper<Observation> obsMapper;
    private PartialRowMapper<Plane> planeMapper;
    private PartialRowMapper<Artifact> artifactMapper;
    private PartialRowMapper<Part> partMapper;
    private PartialRowMapper<Chunk> chunkMapper;
    
    public BaseObservationExtractor(BaseSQLGenerator gen)
    {
        this.obsMapper = gen.getObservationMapper();
        this.planeMapper = gen.getPlaneMapper();
        this.artifactMapper = gen.getArtifactMapper();
        this.partMapper = gen.getPartMapper();
        this.chunkMapper = gen.getChunkMapper();
    }

    public Object extractData(ResultSet rs) 
        throws SQLException
    {
        return extractObservations(rs);
    }

    public List<Observation> extractObservations(ResultSet rs)
        throws SQLException
    {
        int ncol = rs.getMetaData().getColumnCount();
        log.debug("extractData: ncol=" +  ncol);
        List<Observation> ret = new ArrayList<Observation>();
        Observation curObs = null;
        Plane curPlane = null;
        Artifact curArtifact = null;
        Part curPart = null;
        Chunk curChunk = null;
        int row = 0;
        while ( rs.next() )
        {
            row++;
            int col = 1;
            log.debug("mapping Observation at column " + col);
            Observation obs = obsMapper.mapRow(rs, row, col);
            col += obsMapper.getColumnCount();
            if ( curObs == null || !curObs.getID().equals(obs.getID()) )
            {
                if (curObs != null) // found first row of next observation
                    log.debug("END observation: " + curObs.getID());
                
                curObs = obs;
                ret.add(curObs);
                log.debug("START observation: " + curObs.getID());
            }
            // else: obs content repeated due to join -- ignore it
            
            if (ncol > col) // more columns==depth>1: planes
            {
                log.debug("mapping Plane at column " + col);
                Plane p = planeMapper.mapRow(rs, row, col);
                col += planeMapper.getColumnCount();
                if (p != null)
                {
                    if (curPlane == null || !curPlane.getID().equals(p.getID()))
                    {
                        if (curPlane != null) // found first row of next plane in same obs
                            log.debug("END plane: " + curPlane.getID());
                        curPlane = p;
                        curObs.getPlanes().add(p);
                        log.debug("START plane: " + curPlane.getID());
                    }
                    //else:  plane content repeated due to join -- ignore it
                }
                else
                {
                    log.debug("observation: " + curObs.getID() + ": no planes");
                    curPlane = null;
                }
            }
            if (curPlane != null && ncol > col) // more columns==depth>2: artifacts
            {
                log.debug("mapping Artifact at column " + col);
                Artifact a = artifactMapper.mapRow(rs, row, col);
                col += artifactMapper.getColumnCount();
                if (a != null)
                {
                    if (curArtifact == null || !curArtifact.getID().equals(a.getID()))
                    {
                        if (curArtifact != null) // found first row of next artifact in same plane
                            log.debug("END artifact: " + curArtifact.getID());
                        curArtifact = a;
                        curPlane.getArtifacts().add(curArtifact);
                        log.debug("START artifact: " + curArtifact.getID());
                    }
                    //else: artifact content repeated due to join -- ignore it
                }
                else
                {
                    log.debug("plane: " + curPlane.getID() + ": no artifacts");
                    curArtifact = null;
                }
            }
            if (curArtifact != null && ncol > col) // more columns==depth>3: parts
            {
                log.debug("mapping Part at column " + col);
                Part p = partMapper.mapRow(rs, row, col);
                col += partMapper.getColumnCount();
                if (p != null)
                {
                    if (curPart == null || !curPart.getID().equals(p.getID()))
                    {
                        if (curPart != null) // found first row of next artifact in same plane
                            log.debug("END part: " + curPart.getID());
                        curPart = p;
                        curArtifact.getParts().add(curPart);
                        log.debug("START part: " + curPart.getID());
                    }
                    //else: artifact content repeated due to join -- ignore it
                }
                else
                {
                    log.debug("artifact: " + curArtifact.getID() + ": no parts");
                    curPart = null;
                }
            }
            if (curPart != null && ncol > col) // more columns==depth>4: chunks
            {
                log.debug("mapping Chunk at column " + col);
                Chunk c = chunkMapper.mapRow(rs, row, col);
                col += chunkMapper.getColumnCount();
                if (c != null)
                {
                    if (curChunk == null || !curChunk.getID().equals(c.getID()))
                    {
                        if (curChunk != null)
                            log.debug("END part: " + curChunk.getID());
                        curChunk = c;
                        curPart.getChunks().add(curChunk);
                        log.debug("START chunk: " + curChunk.getID());
                    }
                    //else: artifact content repeated due to join -- ignore it
                }
                else
                {
                    log.debug("part: " + curPart.getID() + ": no chunks");
                    curChunk = null;
                }
            }
        }
       
        return ret;
    }
}
