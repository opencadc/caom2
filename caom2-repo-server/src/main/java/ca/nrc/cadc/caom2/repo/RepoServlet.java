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

package ca.nrc.cadc.caom2.repo;

import java.io.IOException;
import java.io.PrintWriter;
import java.security.PrivilegedActionException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;

import javax.security.auth.Subject;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.caom2.repo.action.DeleteAction;
import ca.nrc.cadc.caom2.repo.action.GetAction;
import ca.nrc.cadc.caom2.repo.action.ListAction;
import ca.nrc.cadc.caom2.repo.action.PostAction;
import ca.nrc.cadc.caom2.repo.action.PutAction;
import ca.nrc.cadc.caom2.repo.action.RepoAction;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.log.ServletLogInfo;
import java.security.AccessControlException;

/**
 *
 * @author pdowler
 */
public class RepoServlet extends HttpServlet
{
    private static final long serialVersionUID = 201208141300L;
    
    private static final Logger log = Logger.getLogger(RepoServlet.class);
	private DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);

    private void doit(HttpServletRequest request, HttpServletResponse response, RepoAction action)
        throws IOException, ServletException
    {
        ServletLogInfo logInfo = new ServletLogInfo(request);
        log.info(logInfo.start());

        long start = System.currentTimeMillis();

        SyncInput in = new SyncInput(request);
        SyncOutput out = new SyncOutput(response);

        try
        {
            Subject subject = null;
            try { subject = AuthenticationUtil.getSubject(request); }
            catch(AccessControlException ex)
            {
                log.debug("caught: " + ex);
                logInfo.setSuccess(true);
                response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                return;
            }
            logInfo.setSubject(subject);

            action.setPath(request.getPathInfo());
            action.setLogInfo(logInfo);
            action.setSyncInput(in);
            action.setSyncOutput(out);

            if (subject == null)
                action.run();
            else
            {
                try
                {
                    Subject.doAs(subject, action);
                }
                catch(PrivilegedActionException pex)
                {
                    if (pex.getCause() instanceof ServletException)
                        throw (ServletException) pex.getCause();
                    else if (pex.getCause() instanceof IOException)
                        throw (IOException) pex.getCause();
                    else if (pex.getCause() instanceof RuntimeException)
                        throw (RuntimeException) pex.getCause();
                    else
                        throw new RuntimeException(pex.getCause());
                }
            }
        }
        catch(Throwable t)
        {
            handleUnexpected(out, t, logInfo);
        }
        finally
        {
            Long dt = new Long(System.currentTimeMillis() - start);
            logInfo.setElapsedTime(dt);
            log.info(logInfo.end());
        }
    }
    
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
        throws IOException, ServletException
    {
    	String path = request.getPathInfo();
    	if (path.charAt(0) == '/')
    		path = path.substring(1);
    	log.info("PATH: " + path);
    	String[] cop = path.split("/");
    	if (cop.length == 2)
    		doit(request, response, new GetAction());
    	else if (cop.length == 1)
    	{
            // maxRec == null means list all 
    		String maxRecString = request.getParameter("maxRec");
    		Integer maxRec = null;
    		if (maxRecString != null)	
    		{
    		    maxRec = Integer.valueOf(maxRecString);
    		}
    		
    		try 
    		{
                // start date is optional
                Date start = null;
                String startString = request.getParameter("start");
                if (startString != null)
				   start = df.parse(startString);

                // end date is optional
		    	Date end = null;
		    	String endString = request.getParameter("end");
		    	if (endString != null)
				    end = df.parse(endString);
		    	
				doit(request, response, 
				        new ListAction(maxRec, start, end));
			} 
    		catch (ParseException e) 
    		{
				throw new IllegalArgumentException("wrong date format", e);
			}
    	}    	
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
        throws IOException, ServletException
    {
        doit(request, response, new PostAction());
    }

    @Override
    protected void doPut(HttpServletRequest request, HttpServletResponse response)
        throws IOException, ServletException
    {
        doit(request, response, new PutAction());
    }

    @Override
    protected void doDelete(HttpServletRequest request, HttpServletResponse response)
        throws IOException, ServletException
    {
        doit(request, response, new DeleteAction());
    }

    private void handleUnexpected(SyncOutput out, Throwable t, ServletLogInfo logInfo)
        throws IOException
    {
        logInfo.setSuccess(false);
        logInfo.setMessage(t.getMessage());
        log.error("unexpected exception (SyncOutput.isOpen: " + out.isOpen() + ")", t);

        if (out.isOpen())
            return;

        out.setCode(500); // internal server error
        out.setHeader("Content-Type", "text/plain");
        PrintWriter w = out.getWriter();
        w.println("unexpected exception: " + t);
        w.flush();
    }
}
