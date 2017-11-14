/*
 ************************************************************************
 ****  C A N A D I A N   A S T R O N O M Y   D A T A   C E N T R E  *****
 *
 * (c) 2014.                            (c) 2014.
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
 ****  C A N A D I A N   A S T R O N O M Y   D A T A   C E N T R E  *****
 ************************************************************************
 */

package ca.nrc.cadc.caom2.repo;

import ca.nrc.cadc.ac.GroupURI;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.util.PropertiesReader;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ArchiveReadGroupsTest
{
	
	private static final Logger log = Logger.getLogger(ArchiveReadGroupsTest.class);
	
	public ArchiveReadGroupsTest()
	{
		Log4jInit.setLevel("ca.nrc.cadc.caom", Level.INFO);
		Log4jInit.setLevel("ca.nrc.cadc.util", Level.INFO);
	}
	
	@Test
	public void testArchiveReadGroups()
	{
		ArrayList<GroupURI> expectedGroupsTest1 = new ArrayList<GroupURI>();
		expectedGroupsTest1.add(new GroupURI("ivo://cadc.nrc.ca/gms?testGroup1"));

		try
		{
			System.setProperty(PropertiesReader.class.getName() + ".dir", "src/test/resources");
			
			ArchiveReadGroups config = new ArchiveReadGroups("A1", "archive-read-groups.properties");
			List<GroupURI> testGroups = config.getReadGroups();
			Assert.assertEquals("wrong number of groups", 1, testGroups.size());
			Assert.assertEquals("wrong group", expectedGroupsTest1.get(0), testGroups.get(0));

			ArrayList<GroupURI> expectedGroupsTest2 = new ArrayList<GroupURI>();
			expectedGroupsTest2.add(new GroupURI("ivo://cadc.nrc.ca/gms?testGroup2"));
			expectedGroupsTest2.add(new GroupURI("ivo://cadc.nrc.ca/gms?testGroup3"));
			expectedGroupsTest2.add(new GroupURI("ivo://cadc.nrc.ca/gms?testGroup4"));

			config = new ArchiveReadGroups("A2", "archive-read-groups.properties");
			testGroups = config.getReadGroups();
			Assert.assertEquals("wrong number of groups", 3, testGroups.size());

			Assert.assertEquals("wrong group", expectedGroupsTest2.get(0), testGroups.get(0));
			Assert.assertEquals("wrong group", expectedGroupsTest2.get(1), testGroups.get(1));
			Assert.assertEquals("wrong group", expectedGroupsTest2.get(2), testGroups.get(2));
		}
		catch (Exception e)
		{
			String msg = "Unexpected exception: " + e;
			log.error(msg, e);
			Assert.fail(msg);
		}
		finally
		{
			System.clearProperty(PropertiesReader.class.getName());
		}
	}

}
