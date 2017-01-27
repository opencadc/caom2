<?xml version="1.0" encoding="UTF-8"?>

<!DOCTYPE stylesheet [
<!ENTITY cr "<xsl:text>
</xsl:text>">
<!ENTITY bl "<xsl:text> </xsl:text>">
<!ENTITY dotsep "<xsl:text>.</xsl:text>">
<!ENTITY colonsep "<xsl:text>:</xsl:text>">
<!ENTITY slashsep "<xsl:text>/</xsl:text>">
<!ENTITY modelsep "<xsl:text>:/</xsl:text>"> <!-- separator between model and child -->
<!ENTITY ppsep "<xsl:text></xsl:text>"> <!-- separator between packages -->
<!ENTITY packagesuffix "<xsl:text>/</xsl:text>"> <!-- separator between packages -->
<!ENTITY pcsep "<xsl:text></xsl:text>"> <!-- separator between package and class -->
<!ENTITY casep "<xsl:text>.</xsl:text>"> <!-- separator between class and attribute -->
<!ENTITY aasep "<xsl:text>.</xsl:text>"> <!-- separator between attributes -->
]>

<xsl:stylesheet version="2.0" 
                xmlns:xmi="http://schema.omg.org/spec/XMI/2.1"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                xmlns:uml="http://schema.omg.org/spec/UML/2.0"
               	xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
               	xmlns:IVOA_UML_Profile='http://www.magicdraw.com/schemas/IVOA_UML_Profile.xmi'>
  
  <!--
    Templates used by XSLT scripts to derive vodml-ref-s for attributes, references and collections
  -->
  
  
  
  <xsl:template match="model|package|objectType|primitiveType|dataType|enumeration" mode="vodml-ref">
    <xsl:value-of select="vodml-ref"/>
  </xsl:template>

  
  <xsl:template match="attribute|collection|reference" mode="vodml-ref">
    <xsl:value-of select="vodml-ref"/>
  </xsl:template>



<!--  vodml-ref templates for xmi2intermediate -->
    
  <!--
  GL 2008-10-04:
   somewhat ugly way of calling vodml-ref, but did not quickly find more elegant solution
   -->
  <xsl:template name="intermediate_vodml-ref">
    <xsl:param name="member"/>

    <xsl:choose>
      <xsl:when test="$member/name() = 'uml:Model'">
        <xsl:value-of select="$member/@name"/>
      </xsl:when>
      <!-- 
      <xsl:when test="$member/@xmi:type='uml:DataType'">
        <xsl:value-of select="''"/>
      </xsl:when>
       -->
      <xsl:otherwise> <!--  test="$member/@xmi:type='uml:Package' or $member/@xmi:type='uml:Class'">   -->
        <xsl:variable name="prefix">
          <xsl:call-template name="intermediate_vodml-ref">
            <xsl:with-param name="member" select="$member/.."/>  
          </xsl:call-template>
        </xsl:variable>
        <xsl:variable name="sep">
          <xsl:choose>
            <xsl:when test="$member/../name()='uml:Model'">&modelsep;</xsl:when>
            <xsl:when test="$member/../@xmi:type='uml:Package' and $member/@xmi:type='uml:Package'">&ppsep;</xsl:when>
            <xsl:when test="$member/../@xmi:type='uml:Package' and ($member/@xmi:type='uml:Class' or $member/@xmi:type='uml:DataType' or $member/@xmi:type='uml:Enumeration')">&pcsep;</xsl:when>
            <xsl:when test="$member/../@xmi:type='uml:Class'">&casep;</xsl:when>
            <xsl:when test="$member/../@xmi:type='uml:DataType'">&casep;</xsl:when>
            <xsl:when test="$member/../@xmi:type='uml:Enumeration'">&casep;</xsl:when>
            <xsl:when test="$member/../@xmi:type='uml:Property'">&casep;</xsl:when>
            <xsl:otherwise></xsl:otherwise>
          </xsl:choose>
        </xsl:variable>
        <xsl:variable name="suffix">
          <xsl:choose>
            <xsl:when test="$member/@xmi:type='uml:Package'">&packagesuffix;</xsl:when>
            <xsl:otherwise></xsl:otherwise>
          </xsl:choose>
        </xsl:variable>
        <xsl:value-of select="concat($prefix,$sep,$member/@name, $suffix)"/>
      </xsl:otherwise>
    </xsl:choose>

  </xsl:template>



  <xsl:template match="objectType" name="table_ID_vodml-ref">
    <xsl:value-of select="vodml-ref"/>&casep;<xsl:text>ID</xsl:text>
  </xsl:template>




  <xsl:template name="intermediate_container_vodml-ref">
    <xsl:param name="prefix"/>
    <xsl:value-of select="$prefix"/>&casep;<xsl:text>CONTAINER</xsl:text>
  </xsl:template>



  <xsl:template match="uml:Model" mode="intermediate_vodml-ref">
        <xsl:value-of select="@name"/>
  </xsl:template>


  <xsl:template match="*[@xmi:type='uml:Package']" mode="intermediate_vodml-ref">
   <xsl:param name="prefix"/>
    <xsl:choose>
      <xsl:when test="prefix">
        <xsl:value-of select="$prefix"/><xsl:value-of select="name"/>
      </xsl:when>
      <xsl:otherwise>
        <xsl:variable name="sep">
          <xsl:choose>
            <xsl:when test="../name() = 'package'">&ppsep;</xsl:when>
            <xsl:otherwise>&modelsep;</xsl:otherwise>
          </xsl:choose>
        </xsl:variable>
        <xsl:apply-templates select=".." mode="vodml-ref"/><xsl:value-of select="$sep"/><xsl:value-of select="name"/>
      </xsl:otherwise>
    </xsl:choose> 
  </xsl:template>



</xsl:stylesheet>