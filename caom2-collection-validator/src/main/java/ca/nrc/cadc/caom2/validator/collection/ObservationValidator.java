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

package ca.nrc.cadc.caom2.validator.collection;

import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.ObservationResponse;
import ca.nrc.cadc.caom2.ObservationURI;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.compute.CaomWCSValidator;
import ca.nrc.cadc.caom2.compute.ComputeUtil;
import ca.nrc.cadc.caom2.harvester.HarvestResource;
import ca.nrc.cadc.caom2.harvester.state.HarvestState;
import ca.nrc.cadc.caom2.persistence.ObservationDAO;
import ca.nrc.cadc.caom2.persistence.PostgreSQLGenerator;
import ca.nrc.cadc.caom2.persistence.SQLGenerator;
import ca.nrc.cadc.caom2.repo.client.RepoClient;
import ca.nrc.cadc.caom2.util.CaomValidator;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBConfig;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.log4j.Logger;


/**
 *
 * @author hjeeves
 */
public class ObservationValidator implements Runnable {
    private static Logger log = Logger.getLogger(ObservationValidator.class);

    private static final String POSTGRESQL = "postgresql";
    private static String WCS_ERROR = "[wcs] ";
    private static String CORE_ERROR = "[core] ";
    private static String CHECKSUM_ERROR = "[checksum] ";
    private static String COMPUTE_ERROR = "[compute] ";
    private static String SOURCE_ERROR = "[source] ";

    private String srcURI;
    private RepoClient srcObservationService;
    private ObservationDAO srcObservationDAO;
    private HarvestResource src;

    // Progress file values
    protected File progressFile;
    private HarvestState progressRecord;

    // Store aggregate error count across batches
    private Aggregate runAggregate;

    // Parameter values
    protected Integer batchSize;
    private boolean computePlaneMetadata = false;
    protected Date minDate;
    protected Date maxDate;

    // Batch handling values
    private Date startDate;
    private Date endDate;
    private boolean firstIteration = true;
    private boolean ready = false;
    protected boolean full;

    public ObservationValidator(HarvestResource src, File progressFile, Integer batchSize, boolean compute, Integer nthreads) throws ObservationValidatorException {
        this.src = src;
        this.batchSize = batchSize;
        this.computePlaneMetadata = compute;

        this.progressFile = progressFile;

        try {
            init(nthreads);
        } catch (Exception e) {
            throw new ObservationValidatorException((e.getMessage()));
        }
    }

    public void setMinDate(Date d) {
        this.minDate = d;
    }

    public void setMaxDate(Date d) {
        this.maxDate = d;
    }

    private Map<String, Object> getConfigDAO(HarvestResource desc) throws IOException {
        if (desc.getDatabaseServer() == null) {
            throw new RuntimeException("BUG: getConfigDAO called with ObservationResource[service]");
        }

        Map<String, Object> ret = new HashMap<String, Object>();

        // HACK: detect RDBMS backend from JDBC driver
        DBConfig dbrc = new DBConfig();
        ConnectionConfig cc = dbrc.getConnectionConfig(desc.getDatabaseServer(), desc.getDatabase());
        String driver = cc.getDriver();
        if (driver == null) {
            throw new RuntimeException("failed to find JDBC driver for " + desc.getDatabaseServer() + " " + desc.getDatabase());
        }

        if (cc.getDriver().contains(POSTGRESQL)) {
            ret.put(SQLGenerator.class.getName(), PostgreSQLGenerator.class);
            ret.put("disableHashJoin", Boolean.TRUE);
        } else {
            throw new IllegalArgumentException("unknown SQL dialect: " + desc.getDatabaseServer());
        }

        ret.put("server", desc.getDatabaseServer());
        ret.put("database", desc.getDatabase());
        ret.put("schema", desc.getSchema());
        return ret;
    }

    private void init(int nthreads) throws IOException, ParseException, URISyntaxException {
        if (src.getResourceType() == HarvestResource.SOURCE_DB && src.getDatabaseServer() != null) {
            Map<String, Object> config1 = getConfigDAO(src);
            this.srcObservationDAO = new ObservationDAO();
            srcObservationDAO.setConfig(config1);
            ready = true;
        } else if (src.getResourceType() == HarvestResource.SOURCE_URI) {
            this.srcObservationService = new RepoClient(src.getResourceID(), nthreads);
            this.srcURI = src.getResourceID().toString();
        } else if (src.getResourceType() == HarvestResource.SOURCE_CAP_URL) {
            this.srcObservationService = new RepoClient(src.getCapabilitiesURL(), nthreads);
            this.srcURI = src.getCapabilitiesURL().toString();
        } else {
            throw new IllegalStateException("BUG: unexpected HarvestResource resource type: " + src);
        }

        initProgressFile(src.getIdentifier());

        // This is to capture the totals of each batch aggregate
        runAggregate = new Aggregate();
        
        if (srcObservationService != null) {
            if (srcObservationService.isObsAvailable()) {
                ready = true;
            } else {
                log.error("Not available obs endpoint in " + srcObservationService.toString());
            }
        }

        // make sure wcslib can be loaded
        try {
            log.info("loading ca.nrc.cadc.wcs.WCSLib");
            Class.forName("ca.nrc.cadc.wcs.WCSLib");
        } catch (Throwable t) {
            throw new RuntimeException("FATAL - failed to load WCSLib JNI binding", t);
        }
    }

    // Used for printing UUIDs to logs
    private String format(UUID id) {
        if (id == null) {
            return "null";
        }
        return id.toString();
    }

    // Used for printing and reading dates
//    DateFormat df = DateUtil.getDateFormat(DateUtil.ISO_DATE_FORMAT, DateUtil.UTC);
    DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);

    public String format(Date d) {
        if (d == null) {
            return "null";
        }
        return df.format(d);
    }

    public void run() {
        log.info("START: Observation Validation");
        System.out.println("Observation Validation for:"
            + "\n\tcollection source: " + this.srcURI
            + "\n\tcollection: " + src.getCollection());

        boolean go = true;
        while (go) {
            Aggregate agg = doit();

            if (agg.found > 0) {
                log.debug("***************** finished batch: " + agg + " *******************");
            }

            if (agg.abort) {
                log.error("batched aborted");
            }
            go = (agg.found > 0 && !agg.abort && !agg.done);
            log.debug("agg.found, abort, done: " + agg.found + ": " + agg.abort + ": " + agg.done);
            if (batchSize != null && agg.found < batchSize.intValue() / 2) {
                go = false;
            }
            full = true; // do not start at beginning again
            runAggregate.addAggregate(agg);
        }

        log.info("about to clean up");
//        cleanupProgressFile();
    }

    private Aggregate doit() {
        log.info("Starting batch. batchsize: " + batchSize);
        Aggregate ret = new Aggregate();

        if (!ready) {
            log.error("Observation Validator not ready");
            ret.abort = true;
            return ret;
        }
        long t = System.currentTimeMillis();
        long timeQuery = -1;
        long timeValidation = -1;

        int expectedNum = Integer.MAX_VALUE;
        if (batchSize != null) {
            expectedNum = batchSize.intValue();
        }
        log.debug("expectedNum: " + expectedNum);
        try {
            System.gc(); // hint
            t = System.currentTimeMillis();

            progressRecord = readProgressFile();
            startDate = progressRecord.curLastModified;

            if (firstIteration) {
                if (full) {
                    startDate = null;
                } else if (this.minDate != null) {
                    startDate = this.minDate;
                }
                endDate = this.maxDate;
            }
            firstIteration = false;

            log.info("...getting collection list");
            log.info("Validation window: " + format(startDate) + " :: " + format(endDate) + " [" + batchSize + "]");
            List<ObservationResponse> obsList;
            if (srcObservationDAO != null) {
                obsList = srcObservationDAO.getList(src.getCollection(), startDate, endDate, batchSize + 1);
            } else {
                obsList = srcObservationService.getList(src.getCollection(), startDate, endDate, batchSize + 1);
            }

            // avoid re-processing the last successful one stored in progressRecord (normal case because query: >= startDate)
            if (obsList.isEmpty()) {
                ListIterator<ObservationResponse> iter = obsList.listIterator();
                Observation curBatchLeader = iter.next().observation;
                if (curBatchLeader != null) {
                    log.debug("currentBatch: " + curBatchLeader.getURI() + " " + format(curBatchLeader.getMaxLastModified()));
                    log.debug("currentBatch: " + curBatchLeader.getURI() + " <need a date here for format asking for URI?>");
                    log.debug("progressRecord: " + format(progressRecord.curID) + " " + format(progressRecord.curLastModified));
                    if (curBatchLeader.getID().equals(progressRecord.curID)
                        && curBatchLeader.getMaxLastModified().equals(progressRecord.curLastModified)) {
                        iter.remove();
                        expectedNum--;
                    }
                }
            }

            ret.found = obsList.size();
            log.debug("observation count: " + obsList.size());

            timeQuery = System.currentTimeMillis() - t;
            t = System.currentTimeMillis();

            ListIterator<ObservationResponse> iter1 = obsList.listIterator();
            log.info("...validating observations\n");
            while (iter1.hasNext()) {
                boolean clean = true;
                log.debug("next iteration...");

                ObservationResponse ow = iter1.next();
                ObservationURI observationURI = ow.observationState.getURI();
                Observation o =  ow.observation;
                iter1.remove(); // allow garbage collection during loop

                if (o != null) {
                    progressRecord.curLastModified = o.getMaxLastModified();
                    log.debug("max last modified: " + o.getMaxLastModified());

                    // core validator
                    try {
                        log.debug("core validation");
                        CaomValidator.validate(o);
                    } catch (IllegalArgumentException coreOops) {
                        clean = false;
                        String str = coreOops.toString();
                        log.error(CORE_ERROR + "CONTENT PROBLEM - invalid observation: " + observationURI + " - " + coreOops.getMessage());
                        if (coreOops.getCause() != null) {
                            log.error("cause: " + coreOops.getCause());
                        }
                        ret.coreErr++;
                    }

                    // WCS validation
                    try {
                        log.debug("wcs validation");
                        for (Plane p : o.getPlanes()) {
                            for (Artifact a : p.getArtifacts()) {
                                CaomWCSValidator.validate(a);
                            }
                        }
                    } catch (IllegalArgumentException wcsOops) {
                        clean = false;
                        log.error(CORE_ERROR + "CONTENT PROBLEM - invalid observation: " + observationURI + " - " + wcsOops.getMessage());
                        if (wcsOops.getCause() != null) {
                            log.error("cause: " + wcsOops.getCause());
                        }
                        ret.wcsErr++;
                    }

                    try {
                        log.debug("checksum validation");
                        validateChecksum(o);
                    } catch (MismatchedChecksumException checksumOops) {
                        clean = false;
                        log.error(CHECKSUM_ERROR + "CONTENT PROBLEM - mismatching checksums: " + observationURI);
                        ret.checksumErr++;
                    }

                    // Optional, via --compute on command line
                    // default is false
                    if (computePlaneMetadata) {
                        try {
                            log.debug("computePlaneMetadata: " + o.getURI());
                            for (Plane p : o.getPlanes()) {
                                ComputeUtil.computeTransientState(o, p);
                            }
                        } catch (IllegalArgumentException otherOoops) {
                            clean = false;
                            log.error("Compute error: " + observationURI);
                            ret.computeErr++;
                        }
                    }

                } else if (ow.error != null) {
                    clean = false;
                    // unwrap intervening RuntimeException(s)
                    Throwable oops = ow.error.getCause();
                    while (oops.getCause() != null && oops instanceof RuntimeException) {
                        oops = oops.getCause();
                    }
                    log.error("PROBLEM - failed to read observation: " + observationURI + " - " + oops.getMessage());
                    ret.srcErr++;
                }

                writeProgressFile(progressRecord.curLastModified, src.getIdentifier());

                if (clean == true) {
                    ret.passed++;
                } else {
                    ret.failed++;
                }
                timeValidation += System.currentTimeMillis() - t;
            }

            if (ret.found < expectedNum) {
                ret.done = true;
            }

        } catch (InterruptedException | ExecutionException e) {
            log.error("SEVERE PROBLEM - ThreadPool harvesting Observations failed: " + e.getMessage());
            ret.abort = true;
            ret.runtime++;
        } catch (IOException | ParseException iope) {
            log.error("SEVERE PROBLEM - error reading from progress file" + iope.toString());
            ret.runtime++;
        } catch (URISyntaxException ue) {
            log.error("SEVERE PROBLEM - error reading observation URI from progress file: " + ue.toString());
            ret.runtime++;
        } finally {
            timeValidation = System.currentTimeMillis() - t;
            log.info("...done batch.");
            log.info("batch stats: " + ret.toString());
            log.info("\nTime to run ObservationListQuery: " + timeQuery + "ms");
            log.info("\nTime to run validations for batch: " + timeValidation + "ms\n");
            ret.processTime = timeQuery + timeValidation;
        }
        return ret;
    }

    private void validateChecksum(Observation o) throws MismatchedChecksumException {
        if (o.getAccMetaChecksum() == null) {
            return; // no check
        }
        try {
            URI calculatedChecksum = o.computeAccMetaChecksum(MessageDigest.getInstance("MD5"));

            log.debug("validateChecksum: " + o.getURI() + " -- " + o.getAccMetaChecksum() + " vs " + calculatedChecksum);
            if (!calculatedChecksum.equals(o.getAccMetaChecksum())) {
                throw new MismatchedChecksumException("Observation.accMetaChecksum mismatch");
            }
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 digest algorithm not available");
        }
    }

    // Progress file management functions
    protected void initProgressFile(String src) throws IOException, ParseException, URISyntaxException {
        progressRecord = new HarvestState();

        if (this.progressFile.exists()) {
            // read in the file and set the minDate to the last record in progress
            // processing will start from that date
            HarvestState lastProgressRec = readProgressFile();
            this.minDate = lastProgressRec.curLastModified;
        } else {
            // Initialize the file
            HarvestState blankProgress = new HarvestState();
            writeProgressFile(blankProgress.curLastModified, src);
        }
    }

    private void writeProgressFile(Date curLastModified, String source) throws IOException {
        String sourceStr;

        if (source == null || source.length() == 0) {
            sourceStr = "null";
        } else {
            sourceStr = source;
        }

        String progressString = "source: " + sourceStr + "\n"
                + "curLastModified: " + format(curLastModified) + "\n";

        log.debug("writing progress file: " + progressRecord + ": " + progressString);
        FileOutputStream fos = new FileOutputStream(this.progressFile);
        Writer w =
            new BufferedWriter(new OutputStreamWriter(fos));
        w.write(progressString);
        w.flush();
        w.close();
    }

    private HarvestState readProgressFile() throws IOException, ParseException, URISyntaxException {
        log.debug("reading progress file");
        FileInputStream fis =  new FileInputStream(this.progressFile);
        BufferedReader r =
            new BufferedReader(new InputStreamReader(fis));

        HarvestState hs = new HarvestState();

        // read past the source statement.
        String tmpVal = r.readLine();

        tmpVal = r.readLine().split(": ")[1];
        if (!tmpVal.equals("null")) {
            hs.curLastModified = DateUtil.flexToDate(tmpVal, df);
        }

        r.close();
        log.debug("PROGRESS FILE OBJECT: " + hs);
        return hs;
    }

    // Called from Shutdown hook function
    public void printAggregateReport() {
        String aggReport =
            "\n---------------------------"
            + "\n\nFinal Report: " + "\n---------------"
            + "\ncollection source: " + this.srcURI
            + "\ncollection: " + src.getCollection()
            + "\ntotal observations: " + runAggregate.found
            + "\npassed:  " + runAggregate.passed
            + "\nfailed: " + runAggregate.failed
            + runAggregate.getDetails()
            + "\ntotal time: " + runAggregate.processTime
            + "\n---------------------------" + "\nDone! " + "\n";

        System.out.print(aggReport);
    }

    private void cleanupProgressFile() {
        // Leave the file in place if it's not a clean exit
        try {
            boolean result = Files.deleteIfExists(this.progressFile.toPath());

            if (result == false) {
                log.error("progress file not found: " + this.progressFile.getName());
            }
        } catch (Exception e) {
            log.error("Unable to delete progress file " + this.progressFile.getName());
        }
    }

    private static class Aggregate {
        boolean done = false;
        boolean abort = false;
        int found = 0;
        int passed = 0;
        int failed = 0;
        int wcsErr = 0;
        int coreErr = 0;
        int checksumErr = 0;
        int computeErr = 0;
        int srcErr = 0;
        int runtime = 0;
        long processTime;

        @Override
        public String toString() {
            return "found: " + found + " passed:  " + passed + " failed: " + failed + " time: " + this.processTime;
        }

        public String getDetails() {
            return "\nDetailed error counts (total may be higher than # failed):" + "\n---------------\n"
                + ObservationValidator.WCS_ERROR + ": " + wcsErr + "\n"
                + ObservationValidator.CORE_ERROR + ": " + coreErr + "\n"
                + ObservationValidator.CHECKSUM_ERROR + ": " + checksumErr + "\n"
                + ObservationValidator.COMPUTE_ERROR + ": " + computeErr + "\n"
                + ObservationValidator.SOURCE_ERROR + ": " + srcErr + "\n"
                + "For more information use caom2-validator of caom2ui for individual observations.";
        }

        public void addAggregate(Aggregate ag) {
            this.found += ag.found;
            this.passed += ag.passed;
            this.failed += ag.failed;
            this.wcsErr += ag.wcsErr;
            this.coreErr += ag.coreErr;
            this.checksumErr += ag.checksumErr;
            this.processTime += ag.processTime;
            this.computeErr += ag.computeErr;
        }
    }

}
