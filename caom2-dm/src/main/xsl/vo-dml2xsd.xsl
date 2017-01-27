<?xml version="1.0" encoding="UTF-8"?>
<!-- 
- map each model to single schema document defining ONLY types.
- map types to either complex (objectType, dataType) or simpleTypes.
Use fully qualified type names, i.e. <package-path-with-dots>.typename.
Could do '_' as well, less safe.  Should disallow '.' in names in VO-DML!
'.' is allowed in NCName in XSD. [TBD check what JAXB does with these names]
This makes import much easier as well as prefix: use model prefix!!!
Note, XML files will not need type namess, apart from in xsi:type (but there again nice!)

- No root elements defined, root element definitions up to user.
- Create a single document with element definitions for all complexTypes. 
Idea is that this schema document can be used to validate for example the result of an interpretation of a VOTable. 
Note, this also will allow one to create elements representing an abstract type, using an anoymous type definition extending it.
This is because sometimes in VOTable one might just want to declare a certain type to be an instance of an abstract type, without 
being able to choose a more specific sub-type.
 -->

<!DOCTYPE stylesheet [
<!ENTITY cr "<xsl:text>
</xsl:text>">
<!ENTITY bl "<xsl:text> </xsl:text>">
]>

<xsl:stylesheet version="2.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                xmlns:map="http://volute.g-vo.org/dm/vo-dml-mapping/v0.9"
                xmlns:vo-dml="http://www.ivoa.net/xml/VODML/v1.0">
  
  <!-- <xsl:import href="common.xsl"/> -->
  <xsl:import href="common-mapping.xsl"/>
  
  <xsl:output method="xml" version="1.0" encoding="UTF-8" indent="yes" />
  
  <xsl:strip-space elements="*" />
  
  <!-- xml index on vodml-id -->
  <xsl:key name="element" match="*//vodml-id" use="."/>
  <xsl:key name="package" match="*//package/vodml-id" use="."/>

  
  <!-- Input parameters -->
  <xsl:param name="lastModifiedText"/>

  <xsl:param name="mapping_file"/> <!-- file containing mapping info for java generation such as root packages for all models -->
  <xsl:param name="schemalocation_root" select="'https://volute.googlecode.com/svn/trunk/projects/dm/vo-dml/models-xsd/'"/>
  
  <xsl:variable name="mapping" select="."/>
  <xsl:variable name="xsd-ns">http://www.w3.org/2001/XMLSchema</xsl:variable>
  <xsl:variable name="xsd-prefix">xsd</xsl:variable>
  <xsl:variable name="base-prefix">vodml-base</xsl:variable>
  <xsl:variable name="base-schemanamespace" select="'http://www.ivoa.net/xml/vo-dml/xsd/base/v0.1'"/>
  <xsl:variable name="base-schemalocation" select="'https://volute.googlecode.com/svn/trunk/projects/dm/vo-dml/xsd/vodml-base.xsd'"/>

  <xsl:variable name="typeSchemanameSuffix" select="'.types.xsd'"/>
  <xsl:variable name="elementSchemanameSuffix" select="'.elements.xsd'"/>
  
  <xsl:variable name="referenceType" select="concat($base-prefix,':Reference')"/>

  <xsl:variable name="metadataObjectType" select="concat($base-prefix,':MetadataObject')"/>
  <xsl:variable name="targetnamespace_root" select="concat(namespace-uri(/*),'/xsd')"/> 
  
  <!-- main pattern : processes for root node model -->
  <xsl:template match="/">
    <xsl:for-each select="map:mappedModels/todo/model">
      <xsl:variable name="prefix" select="." />
      <xsl:for-each select="/map:mappedModels/model[name=$prefix]">
        <xsl:choose>
          <xsl:when test="file">
            <xsl:apply-templates select="document(file)/vo-dml:model" />
            <xsl:apply-templates select="document(file)/vo-dml:model" mode="votable-elements"/>
          </xsl:when>
          <xsl:when test="url">
            <xsl:apply-templates select="document(url)/vo-dml:model" />
            <xsl:apply-templates select="document(url)/vo-dml:model" mode="votable-elements"/>
          </xsl:when>
          <xsl:otherwise>
            <xsl:message>
              Model <xsl:value-of select="vodml-id" /> has neither url nor file, hence no XML schemas are generated.
            </xsl:message>
          </xsl:otherwise>
        </xsl:choose>
      </xsl:for-each>
    </xsl:for-each>
  </xsl:template>

  <xsl:template match="vo-dml:model">
    <xsl:message>Model = <xsl:value-of select="vodml-id"/></xsl:message>

    <xsl:variable name="path">
       <xsl:apply-templates select="." mode="xsd-path">
          <xsl:with-param name="delimiter" select="'/'"/>
       </xsl:apply-templates>
    </xsl:variable>
    
    <xsl:variable name="file" select="concat(vodml-id,$typeSchemanameSuffix)"/>
    <!-- open file for the schema file corresponding to this package -->
    <xsl:message >Opening file <xsl:value-of select="$file"/></xsl:message>
    <xsl:result-document href="{$file}">
      <xsl:variable name="targetNamespace">
        <xsl:value-of select="concat($targetnamespace_root,'/',vodml-id)"/>
      </xsl:variable>
      <xsd:schema>
        <xsl:namespace name="{vodml-id}">
          <xsl:value-of select="$targetNamespace"/>
        </xsl:namespace>
        <xsl:attribute name="targetNamespace">
          <xsl:value-of select="$targetNamespace"/>
        </xsl:attribute>
        <xsl:call-template name="ns-rootnamespaces" />
        <xsl:apply-templates select="import" mode="xmlns"/>
        <xsl:call-template name="import-rootnamespaces"/>
        <xsl:apply-templates select="import" mode="ns-import"/>
        
        <xsl:apply-templates select="objectType" mode="test"/>
        <xsl:apply-templates select="dataType" mode="test"/>
        <xsl:apply-templates select="primitiveType" mode="test"/>
        <xsl:apply-templates select="enumeration" mode="test"/>
        
        <xsl:apply-templates select="package"/>
    
      </xsd:schema>
    </xsl:result-document>
  </xsl:template>
  
  <xsl:template match="vo-dml:model" mode="votable-elements">
    <xsl:message>Model = <xsl:value-of select="vodml-id"/></xsl:message>

    <xsl:variable name="path">
       <xsl:apply-templates select="." mode="xsd-path">
          <xsl:with-param name="delimiter" select="'/'"/>
       </xsl:apply-templates>
    </xsl:variable>
    
    <xsl:variable name="file" select="concat(vodml-id,$elementSchemanameSuffix)"/>
    <!-- open file for the schema file corresponding to this package -->
    <xsl:message >Opening file <xsl:value-of select="$file"/></xsl:message>
    <xsl:result-document href="{$file}">
      <xsl:variable name="targetNamespace">
        <xsl:value-of select="concat($targetnamespace_root,'/',vodml-id)"/>
      </xsl:variable>
      <xsd:schema>
        <xsl:namespace name="{vodml-id}">
          <xsl:value-of select="$targetNamespace"/>
        </xsl:namespace>
        <xsl:attribute name="targetNamespace">
          <xsl:value-of select="$targetNamespace"/>
        </xsl:attribute>
    <xsl:element name="xsd:include">
      <xsl:attribute name="schemaLocation">
        <xsl:value-of select="concat($schemalocation_root,vodml-id,$typeSchemanameSuffix)"/>
      </xsl:attribute>
    </xsl:element>
        <xsd:element name="instance">
          <xsd:complexType>
            <xsd:choice maxOccurs="unbounded">
              <xsl:apply-templates select="//objectType" mode="element"/>
              <xsl:apply-templates select="//dataType" mode="element"/>
            </xsd:choice>
          </xsd:complexType>
        </xsd:element>
        
      </xsd:schema>
    </xsl:result-document>
    
    
  </xsl:template>
  
  <xsl:template match="import" mode="xmlns" >
    <xsl:namespace name="{prefix}">
      <xsl:value-of select="concat($targetnamespace_root,'/',prefix)"/>
    </xsl:namespace>
  </xsl:template>
  
  <xsl:template match="import" mode="ns-import">
    <xsl:element name="xsd:import">
      <xsl:attribute name="namespace">
        <xsl:value-of select="concat($targetnamespace_root,'/',prefix)"/>
      </xsl:attribute>
      <xsl:attribute name="schemaLocation">
        <xsl:value-of select="concat($schemalocation_root,vodml-id,$typeSchemanameSuffix)"/>
      </xsl:attribute>
    </xsl:element>
  </xsl:template>

  <xsl:template name="ns-rootnamespaces">
    <xsl:namespace name="{$base-prefix}">
      <xsl:value-of select="$base-schemanamespace"/>
    </xsl:namespace>
  </xsl:template>
  
  <xsl:template name="import-rootnamespaces">
    <xsl:element name="xsd:import">
      <xsl:attribute name="namespace">
        <xsl:value-of select="$base-schemanamespace"/>
      </xsl:attribute>
      <xsl:attribute name="schemaLocation">
        <xsl:value-of select="$base-schemalocation"/>
      </xsl:attribute>
    </xsl:element>
  </xsl:template>
  
  
  <xsl:template match="package">  
        <xsl:apply-templates select="objectType" mode="test"/>
        <xsl:apply-templates select="dataType" mode="test"/>
        <xsl:apply-templates select="primitiveType" mode="test"/>
        <xsl:apply-templates select="enumeration" mode="test"/>
  </xsl:template>  
  
  <xsl:template match="objectType|dataType|enumeration|primitiveType" mode="test">
      <xsl:variable name="mappedtype">
      <xsl:call-template name="findmappingInThisModel">
        <xsl:with-param name="modelname" select="./ancestor::vo-dml:model/name"/>
        <xsl:with-param name="vodml-id" select="vodml-id"/>
      </xsl:call-template>
    </xsl:variable>
    <xsl:if test="not($mappedtype) or $mappedtype = ''" >
      <xsl:apply-templates select="." mode="declare"/>
    </xsl:if>
  </xsl:template>  
  
  <xsl:template match="objectType|dataType" mode="element">
    <xsl:variable name="typename">
      <xsl:apply-templates select="." mode="type-name"/>
    </xsl:variable>
    <xsd:element>
      <xsl:attribute name="name">
        <xsl:value-of select="concat('a_',$typename)"/>
      </xsl:attribute>
      <xsl:choose>
        <xsl:when test="@abstract='true'">
         <xsd:complexType>
          <xsd:complexContent>
            <xsd:extension>
              <xsl:attribute name="base">
                <xsl:value-of select="concat(./ancestor::vo-dml:model/name,':',$typename)"/>
              </xsl:attribute>
            </xsd:extension>
          </xsd:complexContent>
          </xsd:complexType>
        </xsl:when>
        <xsl:otherwise>
          <xsl:attribute name="type">
            <xsl:value-of select="concat(./ancestor::vo-dml:model/name,':',$typename)"/>
          </xsl:attribute>
        </xsl:otherwise>
      </xsl:choose>
    </xsd:element>
  </xsl:template>

  <xsl:template match="objectType" mode="declare">
  
    <xsl:variable name="typename">
      <xsl:apply-templates select="." mode="type-name"/>
    </xsl:variable>
    <xsd:complexType>
      <xsl:attribute name="name">
        <xsl:value-of select="$typename"/>
      </xsl:attribute>
      <xsl:if test="@abstract = 'true'">
        <xsl:attribute name="abstract">
          <xsl:value-of select="'true'"/>
        </xsl:attribute>
      </xsl:if>
      <xsl:call-template name="add_annotation"/>
      <xsl:choose>
        <xsl:when test="extends">
          <xsd:complexContent>
            <xsd:extension>
              <xsl:attribute name="base">
                <xsl:call-template name="XSDType">
                  <xsl:with-param name="model" select="ancestor::vo-dml:model"/>
                  <xsl:with-param name="vodml-ref" select="extends/vodml-ref"/>
                </xsl:call-template>
              </xsl:attribute>
              <xsl:apply-templates select="." mode="content"/>
            </xsd:extension>
          </xsd:complexContent>
        </xsl:when>
        <xsl:otherwise>

          <!-- fix : MetaDataObject -->
          <xsd:complexContent>
            <xsd:extension>
              <xsl:attribute name="base">
                    <xsl:value-of select="$metadataObjectType"/>
              </xsl:attribute>
              <xsl:apply-templates select="." mode="content"/>
            </xsd:extension>
          </xsd:complexContent>
          
        </xsl:otherwise>
      </xsl:choose>
    </xsd:complexType>&cr;&cr;
  </xsl:template>
  
  
  
  <!--   !!!! IMPORTANT !!!!
 The order of the elements MUST be the same as the order in which the properties appear in the jaxb.xsl propOrder.
 This order is {attribute,collection[not(subsets)],reference[not(subsets)]}.
 NB Might we prefer attribute,referemce, collection???
 -->
  <xsl:template match="objectType" mode="content">
    <xsl:variable name="numprops" select="count(attribute|reference[not(subsets)]|collection)"/>
    <xsl:if test="number($numprops) > 0">
      <xsd:sequence>
        <xsl:apply-templates select="attribute"/>
        <xsl:apply-templates select="collection[not(subsets)]"/>
        <xsl:apply-templates select="reference[not(subsets)]"/>
      </xsd:sequence>
    </xsl:if>
  </xsl:template>
  
    
  
  
  <xsl:template match="dataType" mode="declare">
    <xsl:variable name="typename">
      <xsl:apply-templates select="." mode="type-name"/>
    </xsl:variable>
    <xsd:complexType>
      <xsl:attribute name="name">
        <xsl:value-of select="$typename"/>
      </xsl:attribute>
      <xsl:if test="@abstract = 'true'">
        <xsl:attribute name="abstract">
          <xsl:value-of select="'true'"/>
        </xsl:attribute>
      </xsl:if>
      <xsl:call-template name="add_annotation"/>
      <xsl:choose>
        <xsl:when test="extends">
          <xsd:complexContent>
            <xsd:extension>
              <xsl:attribute name="base">
                <xsl:call-template name="XSDType">
                  <xsl:with-param name="model" select="ancestor::vo-dml:model"/>
                  <xsl:with-param name="vodml-ref" select="extends/vodml-ref"/>
                </xsl:call-template>

              </xsl:attribute>
              <xsl:apply-templates select="." mode="content"/>
            </xsd:extension>
          </xsd:complexContent>
        </xsl:when>
        <xsl:otherwise>
          <xsl:apply-templates select="." mode="content"/>
        </xsl:otherwise>
      </xsl:choose>
    </xsd:complexType>&cr;&cr;
  </xsl:template>
  
  
  
  
  <xsl:template match="dataType" mode="content">
    <xsl:variable name="numprops" select="count(attribute)"/>
    <xsl:if test="number($numprops) > 0">
      <xsd:sequence>
        <xsl:apply-templates select="attribute"/>
      </xsd:sequence>
    </xsl:if>
  </xsl:template>
  
  
  
  
  <xsl:template match="enumeration" mode="declare">
    <xsl:variable name="typename">
      <xsl:apply-templates select="." mode="type-name"/>
    </xsl:variable>
    <xsd:simpleType>
      <xsl:attribute name="name">
        <xsl:value-of select="$typename"/>
      </xsl:attribute>
      <xsl:call-template name="add_annotation"/>
       <xsd:restriction base="xsd:string">
        <xsl:apply-templates select="literal"/>
      </xsd:restriction>
    </xsd:simpleType>&cr;&cr;
  </xsl:template>
  
  
  
  
  <xsl:template match="enumeration/literal">
    <xsd:enumeration>
      <xsl:attribute name="value"><xsl:value-of select="name"/></xsl:attribute>
      <xsl:call-template name="add_annotation"/>

    </xsd:enumeration>
  </xsl:template>
  


  <xsl:template match="primitiveType" mode="declare">
    <xsl:variable name="typename">
      <xsl:apply-templates select="." mode="type-name"/>
    </xsl:variable>
    <xsd:simpleType>
      <xsl:attribute name="name">
        <xsl:value-of select="$typename"/>
      </xsl:attribute>
      <xsl:call-template name="add_annotation"/>
       <xsd:restriction base="xsd:string">
      </xsd:restriction>
    </xsd:simpleType>&cr;&cr;
  </xsl:template>
  
  
  
  <xsl:template match="attribute" >
    <xsl:variable name="type">
      <xsl:call-template name="XSDType">
        <xsl:with-param name="model" select="ancestor::vo-dml:model"/>
        <xsl:with-param name="vodml-ref" select="datatype/vodml-ref"/>
      </xsl:call-template>
    </xsl:variable>
    <xsd:element>
      <xsl:attribute name="name" >
        <xsl:value-of select="name"/>
      </xsl:attribute>
      <xsl:attribute name="type" >
        <xsl:value-of select="$type"/>
      </xsl:attribute>
      <xsl:apply-templates select="multiplicity"/>
      <xsl:call-template name="add_annotation"/>
    </xsd:element>
  </xsl:template>
  
  <xsl:template match="multiplicity">
        <!--  only legal values: 0..1   1   0..*   1..* -->      
      <xsl:if test="minOccurs">
        <xsl:attribute name="minOccurs"><xsl:value-of select="minOccurs"/></xsl:attribute>
      </xsl:if>
      <xsl:if test="maxOccurs">
        <xsl:attribute name="maxOccurs">
        <xsl:choose>
          <xsl:when test="maxOccurs &lt;= 0">
            <xsl:value-of select="'unbounded'"/>
          </xsl:when>
          <xsl:otherwise>
            <xsl:value-of select="maxOccurs"/>
          </xsl:otherwise>
        </xsl:choose>
        </xsl:attribute>
        </xsl:if>
  </xsl:template>  
  
  
  <xsl:template match="collection" >
    <xsl:variable name="type">
      <xsl:call-template name="XSDType">
        <xsl:with-param name="model" select="ancestor::vo-dml:model"/>
        <xsl:with-param name="vodml-ref" select="datatype/vodml-ref"/>
      </xsl:call-template>
    </xsl:variable>
    <xsd:element>
      <xsl:attribute name="name" >
        <xsl:value-of select="name"/>
      </xsl:attribute>
      <xsl:attribute name="type" >
        <xsl:value-of select="$type"/>
      </xsl:attribute>
      <xsl:apply-templates select="multiplicity"/>
    </xsd:element>
  </xsl:template>
  
  
  
  
  <xsl:template match="reference" >
    <xsl:variable name="type">
      <xsl:call-template name="XSDType">
        <xsl:with-param name="model" select="ancestor::vo-dml:model"/>
        <xsl:with-param name="vodml-ref" select="datatype/vodml-ref"/>
      </xsl:call-template>
    </xsl:variable>
    <xsd:element>
      <xsl:attribute name="name" >
        <xsl:value-of select="name"/>
      </xsl:attribute>
      <xsl:attribute name="type" >
        <xsl:value-of select="$referenceType"/>
      </xsl:attribute>
      <xsl:apply-templates select="multiplicity"/>
      <xsl:call-template name="add_annotation"/>
    </xsd:element>
  </xsl:template>
  
  
  <xsl:template match="attribute" mode="declare">
    <xsl:variable name="type">
      <xsl:call-template name="XSDType">
        <xsl:with-param name="model" select="ancestor::vo-dml:model"/>
        <xsl:with-param name="vodml-ref" select="datatype/vodml-ref"/>
      </xsl:call-template>
    </xsl:variable>
    <xsd:element>
      <xsl:attribute name="name">
        <xsl:value-of select="name"/>
      </xsl:attribute>
      <xsl:attribute name="type">
        <xsl:value-of select="$type"/>
      </xsl:attribute>
    </xsd:element>
  </xsl:template>
  
  
 
  
  
  
  <!--    named util templates    -->  
  <!-- return the schema location for the schema document for the package with the given id -->
  <xsl:template name="schemalocation-for-package">
    <xsl:param name="themodel"/>
    <xsl:param name="packageid"/>
    <xsl:variable name="path">
      <xsl:call-template name="package-path">
        <xsl:with-param name="model" select="$themodel"/>
        <xsl:with-param name="packageid" select="$packageid"/>
        <xsl:with-param name="delimiter" select="'/'"/>
      </xsl:call-template>
    </xsl:variable>    
    <xsl:value-of select="concat($schemalocation_root,$path,'.xsd')"/>
  </xsl:template>
  
  
  
  
  <!-- Add appinfo element -->
  <xsl:template name="add_annotation">
    <xsl:if test="description or vodml-id">
    <xsd:annotation>
      <xsl:if test="description">
        <xsd:documentation>
          <xsl:value-of select="description"/>
        </xsd:documentation>
      </xsl:if>
      <xsl:if test="vodml-id">
        <xsl:call-template name="add_appinfo"/>
      </xsl:if>
    </xsd:annotation>
    </xsl:if>
  </xsl:template>
  

  <xsl:template name="add_appinfo">
        <xsd:appinfo>
          <vo-dml:vodml-id>
            <xsl:value-of select="./vodml-id"/>
          </vo-dml:vodml-id>
        </xsd:appinfo>
  </xsl:template>

<!-- copied from vo-dml2pojo -->
    <xsl:template name="Model4vodml-ref" as="element()">
    <xsl:param name="model"/>
    <xsl:param name="vodml-ref"/>
<xsl:if test="not($model)">
    <xsl:message>Model4vodml-ref: No model supplied for </xsl:message>
</xsl:if>
    <xsl:variable name="prefix" select="substring-before($vodml-ref,':')"/>
    <xsl:variable name="vodml-id" select="substring-after($vodml-ref,':')"/>
    <xsl:if test="not($prefix) or $prefix=''">
    <xsl:message>!!!!!!! ERROR No prefix found in Model4vodml-ref for <xsl:value-of select="$vodml-ref"/></xsl:message>
    </xsl:if>
    <xsl:choose>
      <xsl:when test="not($prefix) or $prefix = '' or $model/name = $prefix">
        <xsl:copy-of select="$model"/>
      </xsl:when>
      <xsl:otherwise>
    <xsl:choose>
      <xsl:when test="$mapping/map:mappedModels/model[Model4vodml-ref=$prefix]/file">
        <xsl:variable name="file" select="$mapping/map:mappedModels/model[name=$prefix]/file"/>
        <xsl:copy-of select="document($file)/vo-dml:model"/>
      </xsl:when>
      <xsl:otherwise>
        <xsl:variable name="import" select="$model/import[prefix = $prefix]/url"/>
        <xsl:copy-of select="document($import)/vo-dml:model"/>
      </xsl:otherwise>
    </xsl:choose>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>
  

  <xsl:template name="XSDType">
    <xsl:param name="model" />
    <xsl:param name="vodml-ref"/> <!-- assumed to be fully qualified! i.e. also for elements in local model, the prefix is included! -->
    <xsl:param name="length" select="''"/> 
    <xsl:param name="fullpath" select="'true'"/> 

    <xsl:if test="not($model)">
      <xsl:message>XSDType: No model supplied for <xsl:value-of select="$vodml-ref"/></xsl:message>
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
            <xsl:with-param name="utyvodml-refpe" select="$vodml-ref"/>
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

  <!-- Find a mapping for the given vodml-id, in the provided model -->
  <xsl:template name="findmappingInThisModel">
    <xsl:param name="modelname"/>
    <xsl:param name="vodml-id"/>
        <xsl:value-of select="$mapping/map:mappedModels/model[name=$modelname]/type-mapping[vodml-id=$vodml-id]/xsd-type"/>
  </xsl:template>

  <xsl:template name="findmapping">
    <xsl:param name="model" as="element()"/>
    <xsl:param name="vodml-ref"/>
    
    <xsl:variable name="modelname" select="substring-before($vodml-ref,':')"/>
    <xsl:if test="not($modelname) or $modelname=''">
      <xsl:message>!!!!!!! ERROR No prefix found in findmaping for <xsl:value-of select="$vodml-ref"/></xsl:message>
    </xsl:if>
    <xsl:value-of select="$mapping/map:mappedModels/model[name=$modelname]/type-mapping[vodml-id=substring-after($vodml-ref,':')]/xsd-type"/>
  </xsl:template>
  
    <!-- find a java package path towards the type identified with the name -->
  <xsl:template name="fullpath">
    <xsl:param name="vodml-ref"/>
    <xsl:param name="model"/>

    <xsl:variable name="themodel" as="element()">
      <xsl:call-template name="Model4vodml-ref">
        <xsl:with-param name="model" select="$model"/>
        <xsl:with-param name="vodml-ref" select="$vodml-ref"/>
      </xsl:call-template>
    </xsl:variable>

    <xsl:variable name="root" select="$themodel/name"/>

    <xsl:variable name="vodmlid" select="substring-after($vodml-ref,':' )"/>    
    <xsl:variable name="path">
    <xsl:for-each select="$themodel//*[vodml-id=$vodmlid]/ancestor-or-self::*[name() != 'vo-dml:model']">
       <xsl:value-of select="./name"/>
       <xsl:if test="position() != last()">
       <xsl:text>.</xsl:text>
       </xsl:if>
    </xsl:for-each>
    </xsl:variable>
    <xsl:value-of select="concat($root,':',$path)"/>

  </xsl:template>

  <xsl:template match="objectType|dataType|enumeration|primitiveType" mode="type-name">
    <xsl:variable name="path">
    <xsl:for-each select="./ancestor-or-self::*[name() != 'vo-dml:model']">
       <xsl:value-of select="./name"/>
       <xsl:if test="position() != last()">
       <xsl:text>.</xsl:text>
       </xsl:if>
    </xsl:for-each>
    </xsl:variable>
    <xsl:value-of select="$path"/>
  </xsl:template>  
  
</xsl:stylesheet>
