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

package ca.nrc.cadc.caom2.soda;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.dali.Circle;
import ca.nrc.cadc.dali.Interval;
import ca.nrc.cadc.dali.ParamExtractor;
import ca.nrc.cadc.dali.Polygon;
import ca.nrc.cadc.dali.util.CircleFormat;
import ca.nrc.cadc.dali.util.DoubleIntervalFormat;
import ca.nrc.cadc.dali.util.PolygonFormat;
import ca.nrc.cadc.log.WebServiceLogInfo;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.rest.SyncOutput;
import ca.nrc.cadc.util.Base64;
import ca.nrc.cadc.uws.ErrorSummary;
import ca.nrc.cadc.uws.ErrorType;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.Result;
import ca.nrc.cadc.uws.server.JobNotFoundException;
import ca.nrc.cadc.uws.server.JobPersistenceException;
import ca.nrc.cadc.uws.server.JobRunner;
import ca.nrc.cadc.uws.server.JobUpdater;
import ca.nrc.cadc.uws.util.JobLogInfo;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;

/**
 * This JobRunner implements IVOA WD-SODA-1.0 job semantics.
 *
 * @author pdowler
 */
public abstract class AbstractSodaJobRunner implements JobRunner {

    private static Logger log = Logger.getLogger(AbstractSodaJobRunner.class);

    //private static final EnergyConverter conv = new EnergyConverter();
    static final String PARAM_ID = "ID";
    static final String PARAM_POS = "POS";
    static final String PARAM_CIRC = "CIRCLE";
    static final String PARAM_POLY = "POLYGON";
    static final String PARAM_BAND = "BAND";
    static final String PARAM_TIME = "TIME";
    static final String PARAM_POL = "POL";
    static final String PARAM_RUNID = "RUNID";

    static final String RESULT_OK = "ok";
    static final String RESULT_WARN = "warn";
    static final String RESULT_FAIL = "fail";

    static final List<String> SODA_PARAMS = Arrays.asList(
            PARAM_ID, PARAM_POS, PARAM_CIRC, PARAM_POLY, PARAM_BAND, PARAM_TIME, PARAM_POL
    );

    private JobUpdater jobUpdater;
    private SyncOutput syncOutput;
    protected Job job;

    private WebServiceLogInfo logInfo;

    public AbstractSodaJobRunner() {
    }

    public abstract SodaPlugin getSodaPlugin();

    public void setJobUpdater(JobUpdater jobUpdater) {
        this.jobUpdater = jobUpdater;
    }

    public void setSyncOutput(SyncOutput syncOutput) {
        this.syncOutput = syncOutput;
    }

    public void setJob(Job job) {
        this.job = job;
    }

    public void run() {
        logInfo = new JobLogInfo(job);
        log.info(logInfo.start());
        long start = System.currentTimeMillis();

        try {
            doit();
        } catch (Exception ex) {
            log.error("unexpected exception", ex);
        }

        logInfo.setElapsedTime(System.currentTimeMillis() - start);
        log.info(logInfo.end());
    }

    void doit()
            throws IOException // failed to write to SyncOutput
    {
        List<Long> tList = new ArrayList<Long>();
        List<String> sList = new ArrayList<String>();

        tList.add(System.currentTimeMillis());
        sList.add("start");

        try {
            // phase->EXECUTING
            ExecutionPhase ep = jobUpdater.setPhase(job.getID(), ExecutionPhase.QUEUED, ExecutionPhase.EXECUTING, new Date());
            if (!ExecutionPhase.EXECUTING.equals(ep)) {
                ep = jobUpdater.getPhase(job.getID());
                log.debug(job.getID() + ": QUEUED -> EXECUTING [FAILED] -- phase is " + ep);
                logInfo.setSuccess(false);
                logInfo.setMessage("Could not set job phase to EXECUTING.");
                return;
            }
            log.debug(job.getID() + ": QUEUED -> EXECUTING [OK]");
            tList.add(System.currentTimeMillis());
            sList.add("QUEUED -> EXECUTING: ");

            // validate params
            ParamExtractor pex = new ParamExtractor(SODA_PARAMS);
            Map<String, List<String>> params = pex.getParameters(job.getParameterList());
            log.debug("soda params: " + SODA_PARAMS.size() + " map params: " + params.size());
            List<String> idList = params.get(PARAM_ID);

            List<Cutout<Object>> posCut = getSpatialCuts(params);
            List<Cutout<Interval>> bandCut = getEnergyCuts(params);
            List<Cutout<Interval>> timeCut = getTimeCuts(params);
            Cutout<List<String>> polCut = getPolarizationCuts(params);

            // single-valued for sync execution
            StringBuilder esb = new StringBuilder();
            if (syncOutput != null) {
                if (idList.size() != 1) {
                    esb.append("found ").append(idList.size()).append(" ID values, expected 1\n");
                }
                if (posCut.size() > 1) {
                    esb.append("found ").append(posCut.size()).append(" POS/CIRCLE/POLY values, expected 0-1\n");
                }
                if (bandCut.size() > 1) {
                    esb.append("found ").append(bandCut.size()).append(" BAND values, expected 0-1\n");
                }
                if (timeCut.size() > 1) {
                    esb.append("found ").append(timeCut.size()).append(" TIME values, expected 0-1\n");
                }

                if (esb.length() > 0) {
                    throw new IllegalArgumentException("sync: " + esb.toString());
                }
            }

            if (idList.isEmpty()) {
                throw new IllegalArgumentException("missing required param ID");
            }

            List<URI> ids = new ArrayList<URI>();
            esb = new StringBuilder();
            for (String i : idList) {
                try {
                    ids.add(new URI(i));
                } catch (URISyntaxException ex) {
                    esb.append("invalid URI: " + i + "\n");
                }
            }
            if (esb.length() > 0) {
                throw new IllegalArgumentException("invalid ID(s) found\n" + esb.toString());
            }

            // add  single null element to make subsequent loops easier
            if (posCut.isEmpty()) {
                posCut.add(new Cutout<Object>());
            }
            if (bandCut.isEmpty()) {
                bandCut.add(new Cutout<Interval>());
            }
            if (timeCut.isEmpty()) {
                timeCut.add(new Cutout<Interval>());
            }

            tList.add(System.currentTimeMillis());
            sList.add("parse parameters");

            String runID = job.getRunID();
            if (runID == null) {
                runID = job.getID();
            }

            SodaPlugin doit = getSodaPlugin();
            List<Result> jobResults = new ArrayList<>();
            int serialNum = 1;
            for (URI id : ids) {
                for (Cutout<Object> pos : posCut) {
                    for (Cutout<Interval> band : bandCut) {
                        for (Cutout<Interval> time : timeCut) {
                            URL url = doit.toURL(serialNum, id, pos, band, time, polCut);
                            log.debug("cutout URL: " + url.toExternalForm());
                            try {
                                jobResults.add(new Result(RESULT_OK + "-" + serialNum++, url.toURI()));
                            } catch (URISyntaxException ex) {
                                throw new RuntimeException("BUG: result URL is invalid URI: " + url.toExternalForm(), ex);
                            }
                        }
                    }
                }
            }

            // sync: redirect
            if (syncOutput != null) {
                Result r0 = jobResults.get(0);
                syncOutput.setHeader("Location", r0.getURI().toASCIIString());
                syncOutput.setCode(303);
            }

            // phase -> COMPLETED
            ExecutionPhase fep = ExecutionPhase.COMPLETED;
            log.debug("setting ExecutionPhase = " + fep + " with results");
            jobUpdater.setPhase(job.getID(), ExecutionPhase.EXECUTING, fep, jobResults, new Date());
        } catch (IllegalArgumentException ex) {
            handleError(400, ex.getMessage());
        } catch (JobNotFoundException ex) {
            handleError(400, ex.getMessage());
        } catch (IllegalStateException ex) {
            handleError(500, ex.getMessage());
        } catch (JobPersistenceException ex) {
            handleError(500, ex.getMessage());
        } catch (TransientException ex) {
            handleError(503, ex.getMessage());
        } finally {
            tList.add(System.currentTimeMillis());
            sList.add("set final job state: ");

            for (int i = 1; i < tList.size(); i++) {
                long dt = tList.get(i) - tList.get(i - 1);
                log.debug(job.getID() + " -- " + sList.get(i) + " " + dt + "ms");
            }
        }
    }

    List<Cutout<Interval>> getEnergyCuts(Map<String, List<String>> params) {
        List<String> vals = params.get(PARAM_BAND);
        DoubleIntervalFormat fmt = new DoubleIntervalFormat();
        List<Cutout<Interval>> ret = new ArrayList<Cutout<Interval>>();
        for (String s : vals) {
            try {
                Interval i = fmt.parse(s);
                ret.add(new Cutout(PARAM_BAND, s, i));
            } catch (IllegalArgumentException ex) {
                throw new IllegalArgumentException("invalid " + PARAM_BAND + ": " + s);
            }
        }

        return ret;
    }

    List<Cutout<Interval>> getTimeCuts(Map<String, List<String>> params) {
        List<String> vals = params.get(PARAM_TIME);
        DoubleIntervalFormat fmt = new DoubleIntervalFormat();
        List<Cutout<Interval>> ret = new ArrayList<Cutout<Interval>>();
        for (String s : vals) {
            try {
                Interval i = fmt.parse(s);
                ret.add(new Cutout(PARAM_TIME, s, i));
            } catch (IllegalArgumentException ex) {
                throw new IllegalArgumentException("invalid " + PARAM_TIME + ": " + s);
            }
        }

        return ret;
    }

    Cutout<List<String>> getPolarizationCuts(Map<String, List<String>> params) {
        List<String> vals = params.get(PARAM_POL);
        StringBuilder sb = new StringBuilder();
        for (String s : vals) {
            sb.append(s).append("|");
        }
        String sval = null;
        if (sb.length() > 0) {
            sval = sb.substring(0, sb.length() - 2); // drop trailing |
        }
        return new Cutout(PARAM_POL, sval, vals);
    }

    List<Cutout<Object>> getSpatialCuts(Map<String, List<String>> params) {
        List<String> posList = params.get(PARAM_POS);
        List<String> circList = params.get(PARAM_CIRC);
        List<String> polyList = params.get(PARAM_POLY);
        List<Cutout<Object>> posCut = new ArrayList<>();
        for (String s : posList) {
            s = s.trim().toLowerCase();
            if (s.startsWith("circle")) {
                s = s.substring(7); // remove keyword
                circList.add(s);
            } else if (s.startsWith("polygon")) {
                s = s.substring(8); // remove keyword
                posList.add(s);
            } else {
                // TODO: support range?
                throw new IllegalArgumentException("unexpected shape type in: " + s);
            }
        }
        CircleFormat cf = new CircleFormat();
        for (String s : circList) {
            try {
                Circle c = cf.parse(s);
                posCut.add(new Cutout(PARAM_CIRC, s, c));
            } catch (IllegalArgumentException ex) {
                throw new IllegalArgumentException("invalid " + PARAM_CIRC + ": " + s);
            }
        }
        PolygonFormat pf = new PolygonFormat();
        for (String s : polyList) {
            try {
                Polygon p = pf.parse(s);
                posCut.add(new Cutout(PARAM_POLY, s, p));
            } catch (IndexOutOfBoundsException ex) {
                throw new IllegalArgumentException("invalid " + PARAM_POLY + ": " + s);
            }
        }

        return posCut;
    }

    private void handleError(int code, String msg)
            throws IOException {
        logInfo.setMessage(msg);
        if (syncOutput != null) {
            syncOutput.setResponseCode(code);
            syncOutput.setHeader("Content-Type", "text/plain");
            PrintWriter w = new PrintWriter(syncOutput.getOutputStream());
            w.println(msg);
            w.flush();
        }

        ExecutionPhase fep = ExecutionPhase.ERROR;
        ErrorSummary es = new ErrorSummary(msg, ErrorType.FATAL);
        log.debug("setting ExecutionPhase = " + fep + " with results");
        try {
            jobUpdater.setPhase(job.getID(), ExecutionPhase.EXECUTING, fep, es, new Date());
        } catch (JobNotFoundException ex) {
            log.error("oops", ex);
        } catch (JobPersistenceException ex) {
            log.error("oops", ex);
        } catch (TransientException ex) {
            log.error("oops", ex);
        }
    }
}
