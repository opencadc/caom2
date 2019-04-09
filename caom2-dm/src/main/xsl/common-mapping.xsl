<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE stylesheet [
<!ENTITY cr "<xsl:text>
</xsl:text>">
<!ENTITY bl "<xsl:text> </xsl:text>">
]>
<xsl:stylesheet version="2.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:map="http://volute.g-vo.org/dm/vo-dml-mapping/v0.9"
                xmlns:vo-dml="http://www.ivoa.net/xml/VODML/v1.0">


  
  <xsl:import href="common.xsl"/>

  <xsl:param name="targetnamespace_root"/>


  <xsl:template match="vo-dml:model" mode="xsd-path">
    <xsl:param name="delimiter"/>
    <xsl:param name="suffix" select="''"/>
    <xsl:value-of select="concat(vodml-id,$suffix)"/>
  </xsl:template>

  <xsl:template match="package" mode="xsd-path">
    <xsl:param name="delimiter"/>
    <xsl:param name="suffix" select="''"/>
    <xsl:variable name="newsuffix">
      <xsl:value-of select="concat($delimiter,./name,$suffix)"/>
    </xsl:variable>
    <xsl:apply-templates select=".." mode="xsd-path">
       <xsl:with-param name="suffix" select="$newsuffix"/>
       <xsl:with-param name="delimiter" select="$delimiter"/>
    </xsl:apply-templates>
  </xsl:template>

  <!-- return the targetnamespace for the schema document for the package with the given id -->
  <xsl:template name="namespace-for-package">
    <xsl:param name="model"/>
    <xsl:param name="packageid"/>
    <xsl:variable name="path">
      <xsl:call-template name="package-path">
        <xsl:with-param name="model" select="$model"/>
        <xsl:with-param name="packageid" select="$packageid"/>
        <xsl:with-param name="delimiter" select="'/'"/>
      </xsl:call-template>
    </xsl:variable>    
    <xsl:value-of select="concat($targetnamespace_root,'/',$path)"/>
  </xsl:template>
  


  <!-- calculate a prefix for the package with the given id -->
  <xsl:template name="package-prefix">
    <xsl:param name="packageid"/>
    <xsl:variable name="rank">
      <xsl:value-of select="count(/*//package[@xmiid &lt; $packageid])+1"/>
    </xsl:variable>
    <xsl:value-of select="concat('p',$rank)"/>
  </xsl:template>



  <!-- calculate a prefix for the given object -->
  <xsl:template match="objectType" mode="package-prefix">
    <xsl:call-template name="package-prefix">
      <xsl:with-param name="packageid" select="./ancestor::package[1]/@xmiid"/>
    </xsl:call-template>
  </xsl:template>


  
  <!-- Does not really properly count number of contained types in hierarchy, 
  but at least will provide 0 if there are none. -->
  <xsl:template match="objectType" mode="testrootelements">
    <xsl:param name="count" select="0"/>
    <xsl:variable name="xmiid" select="@xmiid"/>
    <xsl:choose>
      <xsl:when test="container">
        <xsl:value-of select="number($count)+1"/>
      </xsl:when>
      <xsl:otherwise>
        <xsl:variable name="childcount" >
          <xsl:choose>
            <xsl:when test="/model//objectType[extends/@xmiidref = $xmiid]">
              <xsl:apply-templates select="/model//objectType[extends/@xmiidref = $xmiid]" mode="testrootelements">
                <xsl:with-param name="count" select="$count"/>
              </xsl:apply-templates>
            </xsl:when>
            <xsl:otherwise>
              <xsl:value-of select="0"/>
            </xsl:otherwise>
          </xsl:choose>
        </xsl:variable>
        <xsl:value-of select="number($count)+number($childcount)"/>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>
  
<!--  Do we potentially want to generate root elements even for dadaType-s?
See similar comment in jaxb.xsl:  <xsl:template match="objectType|dataType" mode="JAXBAnnotation">
 -->
  <xsl:template match="objectType|dataType" mode="root-element-name">
      <xsl:variable name="firstletterisvowel" select="translate(substring(name,1,1),'AEIOU','11111')"/>
      <xsl:variable name="article">
        <xsl:choose>
          <xsl:when test="$firstletterisvowel = '1'" >
            <xsl:text>an</xsl:text>
          </xsl:when>
          <xsl:otherwise>
            <xsl:text>a</xsl:text>
          </xsl:otherwise>
        </xsl:choose>
      </xsl:variable>
      <xsl:value-of select="concat($article,name)"/>
  </xsl:template>
  
  
    <!-- find JavaType for given vodml-ref, starting from provided model element -->
  <xsl:template name="JavaType">
    <xsl:param name="model" />
    <xsl:param name="vodml-ref"/> <!-- assumed to be fully qualified! i.e. also for elements in local model, the prefix is included! -->
    <xsl:param name="length" select="''"/> 
    <xsl:param name="fullpath" /> 

    <xsl:if test="not($model)">
      <xsl:message>JavaType: No model supplied for <xsl:value-of select="vodml-ref"/></xsl:message>
    </xsl:if>

    <xsl:variable name="mappedtype">
      <xsl:call-template name="findmapping">
        <xsl:with-param name="model" select="$model"/>
        <xsl:with-param name="vodml-ref" select="$vodml-ref"/>
      </xsl:call-template>
    </xsl:variable>
    <xsl:choose>
      <xsl:when test="$mappedtype != ''">
          <xsl:value-of select="$mappedtype"/>  
      </xsl:when>
      <xsl:otherwise>
      <xsl:choose>
        <xsl:when test="$fullpath='true'">
<!--         <xsl:message >Finding full path for <xsl:value-of select="$vodml-ref"/></xsl:message>   -->
          <xsl:call-template  name="fullpath">
            <xsl:with-param name="model" select="$model"/>
            <xsl:with-param name="vodml-ref" select="$vodml-ref"/>
          </xsl:call-template>
        </xsl:when>
        <xsl:otherwise>
        
        <xsl:variable name="type" as="element()">
          <xsl:call-template name="Element4vodml-ref">
            <xsl:with-param name="model" select="$model"/>
            <xsl:with-param name="vodml-ref" select="$vodml-ref"/>
          </xsl:call-template>
        </xsl:variable> 
        
          <xsl:value-of select="$type/name"/>
        </xsl:otherwise>
      </xsl:choose>
     </xsl:otherwise>
    </xsl:choose>
  </xsl:template>
  
  
  <xsl:template name="Element4vodml-ref" as="element()">
    <xsl:param name="model" as="element()"/>
    <xsl:param name="vodml-ref"/>
    
    <xsl:if test="not($model)">
      <xsl:message>Element4vodml-ref: No model provided for vodml-ref <xsl:value-of select="$vodml-ref"/></xsl:message>
    </xsl:if>
    <xsl:variable name="prefix" select="substring-before($vodml-ref,':')"/>
    <xsl:if test="not($prefix) or $prefix=''">
    <xsl:message>!!!!!!! ERROR No prefix found in Element4vodml-ref for <xsl:value-of select="$vodml-ref"/></xsl:message>
    </xsl:if>
    <xsl:variable name="vodml-id" select="substring-after($vodml-ref,':')"/>
    <xsl:choose>
      <xsl:when test="$model/name = $prefix">
        <xsl:copy-of select="$model//*[vodml-id=$vodml-id]"/>
      </xsl:when>
      <xsl:otherwise>
 
    <xsl:choose>
      <xsl:when test="$mapping/mappedModels/model[name=$prefix]/file">
        <xsl:variable name="file" select="$mapping/mappedModels/model[name=$prefix]/file"/>
        <xsl:copy-of select="document($file)/vo-dml:model//*[vodml-id=$vodml-id]"/>
      </xsl:when>
      <xsl:otherwise>
      <xsl:message >Looking online for model '<xsl:value-of select="$prefix"/>'</xsl:message>
        <xsl:variable name="import" select="$model/import[prefix = $prefix]/url"/>
        <xsl:copy-of select="document($import)/vo-dml:model//*[vodml-id=$vodml-id]"/>
      </xsl:otherwise>
    </xsl:choose>
    </xsl:otherwise>
    </xsl:choose>
  </xsl:template>

  <xsl:template name="Model4vodml-ref" ><!-- as="element()">  -->
    <xsl:param name="model"/>
    <xsl:param name="vodml-ref"/>
    <xsl:message>Model4vodml-ref: vodml-ref=<xsl:value-of select="$vodml-ref"/></xsl:message>

<xsl:if test="not($model)">
    <xsl:message>Model4vodml-ref: No model supplied for </xsl:message>
</xsl:if>

    <xsl:variable name="prefix" select="substring-before($vodml-ref,':')"/>
    <xsl:message>Model4vodml-ref: prefix=<xsl:value-of select="$prefix"/></xsl:message>
    <xsl:variable name="vodml-id" select="substring-after($vodml-ref,':')"/>
    <xsl:if test="not($prefix) or $prefix=''">
    <xsl:message>!!!!!!! ERROR No prefix found in Model4vodml-ref for <xsl:value-of select="$vodml-ref"/></xsl:message>
    </xsl:if>
    <xsl:choose>
      <xsl:when test="not($prefix) or $prefix = '' or $model/name = $prefix">
        <xsl:message>Model4vodml-ref : <xsl:value-of select="$model/name"/></xsl:message>
        <xsl:copy-of select="$model"/>
      </xsl:when>
      <xsl:otherwise>
    <xsl:choose>
      <xsl:when test="$mapping/mappedModels/model[name=$prefix]/file">
        <xsl:variable name="file" select="$mapping/mappedModels/model[name=$prefix]/file"/>
        <xsl:message>Model4vodml-ref : <xsl:value-of select="$file"/></xsl:message>
        <xsl:copy-of select="document($file)/vo-dml:model"/>
      </xsl:when>
      <xsl:otherwise>
        <xsl:variable name="import" select="$model/import[name = $prefix]/url"/>
        <xsl:copy-of select="document($import)/vo-dml:model"/>
      </xsl:otherwise>
    </xsl:choose>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>
  
  
  
</xsl:stylesheet>