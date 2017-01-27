<?xml version="1.0" encoding="UTF-8"?>
<!-- 
This XSLT script transforms a data model from our
intermediate representation to a GraphViz dot file.
 -->

<!DOCTYPE stylesheet [
<!ENTITY cr  "<xsl:text>
</xsl:text>">
<!ENTITY bl  "<xsl:text> </xsl:text>">
<!ENTITY rem "<xsl:text>-- </xsl:text>">
]>

<xsl:stylesheet version="2.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
								xmlns:exsl="http://exslt.org/common"
                extension-element-prefixes="exsl"
                xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                xmlns:vo-dml="http://www.ivoa.net/xml/VODML/v1.0">
  
  <xsl:import href="common.xsl"/>
  
  <xsl:output method="text" encoding="UTF-8" indent="no" />
  
  <xsl:strip-space elements="*" />
  
  <xsl:key name="element" match="*//vodml-id" use="."/>
  <xsl:key name="package" match="*//package/vodml-id" use="."/>

  <xsl:param name="project.name"/>
  <xsl:param name="usesubgraph" select="'F'"/>
  
  <xsl:variable name="packages" select="//package/vodml-id"/>

  <xsl:template match="/">
  <xsl:message>Starting GVD</xsl:message>
    <xsl:apply-templates select="vo-dml:model"/>
  </xsl:template>
  
  <xsl:template match="vo-dml:model">  
  <xsl:message>Found model</xsl:message>
digraph GVmap {  <!-- name must not be too long. the cmap that is generated uses this name and that must not be too long to work apparently -->
	label = "\n\n<xsl:value-of select="name"/> data model"
	rankdir=TB
	
	node [ 
	  shape=tab
	  style=filled
	]
	subgraph cluster_packages {
	  label="Model"
	  rankdir=TB
      style=filled
      fillcolor="<xsl:apply-templates select="." mode="color"/>"
	  <xsl:apply-templates select="package" />
	  <xsl:if test="//package[depends]">
	      edge [color="black", arrowhead="open", arrowtail="none", style="dashed"]
	    <xsl:apply-templates select="//package[depends]" mode="depends"/>
	  </xsl:if>
	}
	
	node [
	shape=record
	fontsize=8
	style=filled] 
	<xsl:apply-templates select="." mode="types"/>
<!--   <xsl:apply-templates select="//objectType"/>  -->
  <xsl:if test="//extends">
<!--    edge [color="red", arrowhead="empty", headport="s", tailport="n"] --> 
    edge [color="red", arrowtail="none", arrowhead="empty"]
    <xsl:apply-templates select="//extends"/>
  </xsl:if>
  <xsl:if test="//collection">
<!--    edge [color="blue", arrowhead="open", arrowtail="diamond", headport="n", tailport="s"] --> 
    edge [color="blue", arrowhead="open", arrowtail="diamond",dir="both",fontsize="10"]
    <xsl:apply-templates select="//objectType/collection"/>
  </xsl:if>
  <xsl:if test="//reference">
<!--    edge [color="green", arrowhead="open", headport="w", tailport="e"]   --> 
    edge [color="green", arrowhead="open", arrowtail="none"]
    <xsl:apply-templates select="//reference"/>
  </xsl:if>  
}
  </xsl:template>
  
  

  <xsl:template match="package">
    "<xsl:value-of select="vodml-id"/>" [
    URL="#<xsl:value-of select="vodml-id"/>"
    label = "<xsl:value-of select="name"/>"
    fillcolor="<xsl:apply-templates select="." mode="color"/>"
    ] ;
    <xsl:if test="package">
    subgraph cluster_<xsl:value-of select="name"/> {
      label="Package: <xsl:value-of select="name"/>"
      style=filled
      fillcolor="<xsl:apply-templates select="." mode="color"/>"
      <xsl:apply-templates select="package"/>
    }
    </xsl:if>
  </xsl:template>

  <!-- If the name starts with "cluster" than the nodes inside the package will be  -->
  <xsl:template match="vo-dml:model|package" mode="types">
    <xsl:choose>
    <xsl:when test="$usesubgraph = 'T'">
    subgraph cluster_<xsl:value-of select="name"/> {
      label="<xsl:value-of select="name"/>"
      <xsl:apply-templates select="objectType|dataType|enumeration|primitiveType"/>
    }
    </xsl:when>
    <xsl:otherwise>
      <xsl:apply-templates select="objectType|dataType|enumeration|primitiveType"/>
    </xsl:otherwise>
    </xsl:choose>
    <xsl:apply-templates select="package" mode="types"/>
  </xsl:template>
  
  
  <!--  TBD deal with types directly under model -->
  <xsl:template match="objectType">
    <xsl:variable name="nodename">
        <xsl:apply-templates select="." mode="nodename"/>
    </xsl:variable>
    <xsl:variable name="label">
        <xsl:apply-templates select="." mode="nodelabel"/>
    </xsl:variable>
	<xsl:value-of select="$nodename"/> [
    URL="#<xsl:value-of select="vodml-id"/>"
    label = "{<xsl:value-of select="$label"/><xsl:if test="attribute">|<xsl:apply-templates select="attribute"/></xsl:if>}"
    fillcolor="<xsl:apply-templates select="." mode="color"/>"
    ] ;
  </xsl:template>


  <xsl:template match="objectType|dataType|enumeration|primitiveType" mode="color">
    <xsl:apply-templates select=".." mode="color"/>
  </xsl:template>

  <xsl:template match="vo-dml:model" mode="color">
    <xsl:value-of select="'/set312/1'"/>
  </xsl:template>

  <xsl:template match="package" mode="color">
    <xsl:value-of select="concat('/set312/',index-of($packages,./vodml-id)+1)"/> 
  </xsl:template>

  <xsl:template match="dataType">
    <xsl:variable name="nodename">
        <xsl:apply-templates select="." mode="nodename"/>
    </xsl:variable>
    <xsl:variable name="label">
        <xsl:apply-templates select="." mode="nodelabel"/>
    </xsl:variable>
    <xsl:value-of select="$nodename"/> [
    URL="#<xsl:value-of select="vodml-id"/>"
    label = "{&amp;lt;&amp;lt;datatype&amp;gt;&amp;gt;\n<xsl:value-of select="$label"/><xsl:if test="attribute">|<xsl:apply-templates select="attribute"/></xsl:if>}"
    fillcolor="<xsl:apply-templates select="." mode="color"/>"
    ] ;
  </xsl:template>



  <xsl:template match="enumeration">
    <xsl:variable name="nodename">
        <xsl:apply-templates select="." mode="nodename"/>
    </xsl:variable>
    <xsl:variable name="label">
        <xsl:apply-templates select="." mode="nodelabel"/>
    </xsl:variable>
    <xsl:value-of select="$nodename"/> [
    URL="#<xsl:value-of select="vodml-id"/>"
    label = "{&amp;lt;&amp;lt;enumeration&amp;gt;&amp;gt;\l<xsl:value-of select="$label"/><xsl:if test="literal">|<xsl:apply-templates select="literal"/></xsl:if>}"
    fillcolor="<xsl:apply-templates select="." mode="color"/>"
    ] ;
  </xsl:template>


  <xsl:template match="primitiveType">
     <xsl:variable name="nodename">
        <xsl:apply-templates select="." mode="nodename"/>
    </xsl:variable>
    <xsl:variable name="label">
        <xsl:apply-templates select="." mode="nodelabel"/>
    </xsl:variable>
    <xsl:value-of select="$nodename"/>[
    URL="#<xsl:value-of select="vodml-id"/>"
    label = "{&amp;lt;&amp;lt;primitive type&amp;gt;&amp;gt;\n<xsl:value-of select="$label"/>}"
    fillcolor="<xsl:apply-templates select="." mode="color"/>"
    ] ;
  </xsl:template>

<!--  NOTE keep starting an ending <xsl:text> elements -->
  <xsl:template match="attribute">
  <xsl:text>+</xsl:text> <xsl:value-of select="name"/> : <xsl:value-of select="datatype/vodml-ref"/><xsl:text>\l</xsl:text>
  </xsl:template>




  <xsl:template match="literal">
  <xsl:text>+</xsl:text> <xsl:value-of select="name"/><xsl:text>\l</xsl:text>
  </xsl:template>



  <xsl:template match="extends">
    <xsl:variable name="fromnode">
        <xsl:apply-templates select=".." mode="nodename">
    </xsl:apply-templates>
    </xsl:variable>
    <xsl:variable name="tonode">
        <xsl:apply-templates select="vodml-ref" mode="nodename">
    </xsl:apply-templates>
    </xsl:variable>
    <xsl:value-of select="$fromnode"/> -> <xsl:value-of select="$tonode"/> ;
  </xsl:template>


  <xsl:template match="collection|reference">
    <xsl:variable name="fromnode">
        <xsl:apply-templates select=".." mode="nodename">
    </xsl:apply-templates>
    </xsl:variable>
    <xsl:variable name="tonode">
        <xsl:apply-templates select="datatype/vodml-ref" mode="nodename"/>
    </xsl:variable>

    <xsl:value-of select="$fromnode"/> -> <xsl:value-of select="$tonode"/> [headlabel="<xsl:apply-templates select="multiplicity" mode="tostring"/>",label="<xsl:value-of select="name"/>",labelfontsize=10] ;
  </xsl:template>

<!-- 
  <xsl:template match="reference">
    <xsl:value-of select="../name"/> -> <xsl:value-of select="datatype/@name"/> ;
  </xsl:template>
 -->

  <xsl:template match="objectType|dataType|primitiveType|enumeration" mode="nodename">
  <xsl:variable name="vodml-ref"><xsl:apply-templates select="vodml-id" mode="asvodml-ref"/> </xsl:variable>
    <xsl:value-of select="concat('&quot;',$vodml-ref,'&quot;')"/>
   </xsl:template>

  <!--  name of a node for a certain vodml-ref.   -->
  <xsl:template match="vodml-ref" mode="nodename">
      <xsl:value-of select="concat('&quot;',.,'&quot;')"/>
<!-- 
    <xsl:variable name="prefix" select="substring-before(.,':' )"/>
    <xsl:variable name="id" select="substring-after(.,':' )"/>
    <xsl:choose>
    <xsl:when test="$prefix=/vo-dml:model/name">
       <xsl:value-of select="concat('&quot;',$id,'&quot;')"/>
    </xsl:when>
    <xsl:otherwise>
      <xsl:value-of select="concat('&quot;',.,'&quot;')"/>
    </xsl:otherwise>
    </xsl:choose>
 -->
   </xsl:template>



  <xsl:template match="objectType|dataType|primitiveType|enumeration" mode="nodelabel">
<!--       <xsl:value-of select="concat('&quot;',/vo-dml:model/name,':',./vodml-id,'&quot;')"/>   -->
<!--       <xsl:value-of select="concat(//vo-dml:model/name,':',./vodml-id)"/>  -->
    <xsl:call-template name="package-path">
    <xsl:with-param name="model" select="/vo-dml:model"/>
    <xsl:with-param name="packageid" select="../vodml-id"/>
    <xsl:with-param name="delimiter" select="'/'"/>
    <xsl:with-param name="suffix" select="name"/>
    </xsl:call-template>
   </xsl:template>

</xsl:stylesheet>