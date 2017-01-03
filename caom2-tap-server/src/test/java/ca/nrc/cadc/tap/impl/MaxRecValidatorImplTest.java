/*
 ************************************************************************
 ****  C A N A D I A N   A S T R O N O M Y   D A T A   C E N T R E  *****
 *
 * (c) 2012.                         (c) 2012.
 * National Research Council            Conseil national de recherches
 * Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
 * All rights reserved                  Tous droits reserves
 *
 * NRC disclaims any warranties         Le CNRC denie toute garantie
 * expressed, implied, or statu-        enoncee, implicite ou legale,
 * tory, of any kind with respect       de quelque nature que se soit,
 * to the software, including           concernant le logiciel, y com-
 * without limitation any war-          pris sans restriction toute
 * ranty of merchantability or          garantie de valeur marchande
 * fitness for a particular pur-        ou de pertinence pour un usage
 * pose.  NRC shall not be liable       particulier.  Le CNRC ne
 * in any event for any damages,        pourra en aucun cas etre tenu
 * whether direct or indirect,          responsable de tout dommage,
 * special or general, consequen-       direct ou indirect, particul-
 * tial or incidental, arising          ier ou general, accessoire ou
 * from the use of the software.        fortuit, resultant de l'utili-
 *                                      sation du logiciel.
 *
 *
 * @author jenkinsd
 * 7/3/12 - 2:45 PM
 *
 *
 *
 ****  C A N A D I A N   A S T R O N O M Y   D A T A   C E N T R E  *****
 ************************************************************************
 */
package ca.nrc.cadc.tap.impl;

import ca.nrc.cadc.tap.MaxRecValidator;
import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.SchemaDesc;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.schema.TapSchema;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.Parameter;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import static org.junit.Assert.*;
import org.junit.Test;

public class MaxRecValidatorImplTest
{
    private static final Logger log = Logger.getLogger(MaxRecValidatorImplTest.class);
    
    static final TapSchema tapSchema = new TapSchema();
    
    static
    {
        Log4jInit.setLevel("ca.nrc.cadc.tap", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.dali", Level.INFO);
        
        
        ColumnDesc c1 = new ColumnDesc("foo.bar", "a", "adql:INTEGER", null);
        ColumnDesc c2 = new ColumnDesc("foo.bar", "b", "adql:DOUBLE", null);
        ColumnDesc c3 = new ColumnDesc("foo.bar", "c", "adql:VARCHAR", 64);
        
        TableDesc td = new TableDesc("foo", "foo.bar");
        td.getColumnDescs().add(c1);
        td.getColumnDescs().add(c2);
        td.getColumnDescs().add(c3);
        
        SchemaDesc sd = new SchemaDesc("foo");
        sd.getTableDescs().add(td);
        
        tapSchema.getSchemaDescs().add(sd);
    }
    
    Job job = new Job()
    {
        @Override
        public String getID() { return "internal-test-jobID"; }
    };
    
    @Test
    public void testSync() throws Exception
    {
        try
        {
            log.debug("*** testSync ***");
            job.getParameterList().add(new Parameter("QUERY", "SELECT a, b, c FROM foo.bar"));

            final MaxRecValidator testSubject = new MaxRecValidatorImpl();
            testSubject.setJob(job);
            testSubject.setTapSchema(tapSchema);
            testSubject.setSynchronousMode(true);

            // no limit
            final Integer result1 = testSubject.validate();
            log.debug("no limit: " + result1);
            assertNull("no limit", result1);

            // large user limit
            job.getParameterList().add(new Parameter("MAXREC", "123456"));
            final int result2 = testSubject.validate();
            assertEquals("user limit", 123456, result2);

            // sync -> vospace: user limit only
            job.getParameterList().add(new Parameter("DEST", "vos://cadc.nrc.ca!vospace/myvospace"));
            final int result3 = testSubject.validate();
            assertEquals("vospace dest with user limit", 123456, result3);
        }
        finally
        {
            job.getParameterList().clear();
        }
    }
    
    @Test
    public void testASync() throws Exception
    {
        try
        {
            log.debug("*** testASync ***");
            job.getParameterList().add(new Parameter("QUERY", "SELECT a, b, c FROM foo.bar"));

            final MaxRecValidator testSubject = new MaxRecValidatorImpl();
            testSubject.setJob(job);
            testSubject.setTapSchema(tapSchema);
            testSubject.setSynchronousMode(false);

            // imposed limit
            final Integer defaultLimit = testSubject.validate();
            log.debug("async limit: " + defaultLimit);
            assertNotNull("async limit", defaultLimit);

            // large user limit
            Integer largeLimit = defaultLimit * 100;
            job.getParameterList().add(new Parameter("MAXREC", largeLimit.toString()));
            final Integer result2 = testSubject.validate();
            log.debug("large limit: " + defaultLimit);
            assertNotNull("async limit", result2);
            assertEquals("larger == dynamic", defaultLimit, result2);

            // sync -> vospace: user limit only
            job.getParameterList().add(new Parameter("DEST", "vos://cadc.nrc.ca!vospace/myvospace"));
            final Integer result3 = testSubject.validate();
            assertNotNull("user limit", result3);
            assertEquals("vospace dest with user limit", largeLimit, result3);
        }
        finally
        {
            job.getParameterList().clear();
        }
    }
}
