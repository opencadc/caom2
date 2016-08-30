<!--
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2009.                            (c) 2009.
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
*  $Revision: 4 $
*
************************************************************************
-->

<%@ page language="java" %>

<%@ taglib uri="WEB-INF/c.tld" prefix="c"%>

<%
    String skin = "http://localhost/cadc/skin/";
    String htmlHead = skin + "htmlHead";
    String bodyHeader = skin + "quickHeader";
    String bodyFooter = skin + "bodyFooter";
%>

<head>
    <title>CAOM-2.0 Repository Service</title>
    <c:catch><c:import url="<%= htmlHead %>" /></c:catch>
</head>
<body>
<c:catch><c:import url="<%= bodyHeader %>" /></c:catch>

<div class="main">

<h1>The Common Archive Observation Model (CAOM) Metadata Repository</h1>

<p>
This service provides a metadata repository for CAOM-2.0 observations.
</p>

<p>
Resources:
</p>

<div class="table">
<table class="content">
<tr>
<th>resource</th><th>description</th>
</tr>
<tr>
<td>
    /caom2repo/pub
</td>
<td>
    anonymous (http), client-certificate authentication (https)
</td>
</tr>
<tr>
<td>
    /caom2repo/auth
</td>
<td>
    username/password authentication (http)
</td>
</tr>
<tr>
<td>
    <a href="availability">/caom2repo/availability</a>
</td>
<td>
    VOSI availability
</td>
</tr>

</table>
</div>

<h1>Usage</h1>

<p>
All HTTP actions are supported: GET, POST, PUT, DELETE. In all cases, the path <em>must</em> be composed
by appending the path component of a CAOM-2.0 ObservationURI onto one of the above (pub or auth) resources,
(e.g. <b>/caom2repo/pub/IRIS/f001h000</b>).
</p>

<p>Standard response codes:<br/>
- 400 (bad request) with the text "invalid ObservationURI" if a valid ObservationURI cannot be constructed from the requested path,<br/>
- 403 (forbidden) if the caller does not have read permission,<br/>
- 404 (not found) if the specified collection does not exist or the observation is not in the repository,<br/>
</p>

<h2>GET</h2>

<p>
GET retrieves the observation as a CAOM-2.0 xml document.
</p>

<h2>POST</h2>
<p>
POST updates (replaces) an existing observation and will fail (with a 404 "not found")
if it does not exist.
The delivered content must be a CAOM-2.0 xml document.
</p><p>
Response codes:<br/>
- 400 (bad request) with the text "invalid XML" if the submitted document is not
valid (well-formedness and schema validation),<br/>
- 400 (bad request) with the text "invalid observation" if the document was valid but the
data structure did not pass extended validation (e.g. a failure to compute extended
metadata due to invalid WCS),<br/>
- 400 (bad request) with the text "request path does not match ObservationURI"
if the path of URI in the document submitted does not match the path in the URL to which
the document was posted,<br/> 
- 413 (too large) if the size of the document exceeds the 500Kb maximum imposed in this web
service.
</p>

<h2>PUT</h2>
<p>
PUT stores a new observation and will fail if it already exists.
The delivered content must be a CAOM-2.0 xml document.
</p><p>
Response codes:<br/>
- 404 (not found) only if the collection does not exist,
- 400 (bad request) with the text "invalid XML" if the submitted document is not
valid (well-formedness and schema validation),<br/>
- 400 (bad request) with the text "invalid observation" if the document was valid but the
data structure did not pass extended validation (e.g. a failure to compute extended
metadata due to invalid WCS),<br/>
- 400 (bad request) with the text "request path does not match ObservationURI"
if the path of URI in the document submitted does not match the path in the URL to which
the document was posted,<br/> 
- 409 (conflict) with the text "already exists" if the observation already exists,<br/>
- 413 (too large) if the size of the document exceeds the 20MB maximum imposed in this web
service.
</p>

<h2>DELETE</h2>
<p>
DELETE removes an existing observation from the repository and will fail (with a 404 "not found")
if it does not exist.
</p>

<c:catch><c:import url="<%= bodyFooter %>" /></c:catch>
</body>

</html>

