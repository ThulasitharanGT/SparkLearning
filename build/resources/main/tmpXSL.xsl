<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="tmpXml.xsd"> <!-- xsi:schemaLocation="http://java.sun.com/xml/ns/jaxb/tmpXml.xsd"> -->
    <xsl:output method="text"/>
    <xsl:strip-space elements="*"/>
    <xsl:template match="updateEvent"> <!-- discount calclation wont be done for upd event -->
        <xsl:text>UPD||</xsl:text>
        <xsl:text>sessionID::</xsl:text>
        <xsl:value-of select="@sessionID"/>
        <xsl:text>+</xsl:text>
        <xsl:text>accountID::</xsl:text>
        <xsl:value-of select="accountID"/>
        <xsl:text>+incomingTS::</xsl:text>
        <xsl:value-of select="incomingTS"/>
        <xsl:text>+orderId::</xsl:text>
        <xsl:value-of select="orderId"/><xsl:text>+</xsl:text>
        <xsl:text>orderItems::</xsl:text>
        <xsl:for-each select="orderItems">
            <xsl:for-each select="itemsInfo">
                <xsl:text>(</xsl:text>
                <xsl:value-of select="itemId"/><xsl:text>~</xsl:text>
                <xsl:value-of select="price"/><xsl:text>~</xsl:text>
                <xsl:value-of select="quantity"/><xsl:text>~</xsl:text>
                <xsl:variable name="discountCode"> <!-- creating custom variable -->
                    <xsl:value-of select="@discountApplied"/>
                </xsl:variable>
                <xsl:if test="$discountCode=''">
                    <xsl:text>NA~NA</xsl:text>
                </xsl:if>
                <xsl:if test="$discountCode!=''">
                    <xsl:value-of select="@discountApplied"/><xsl:text>~</xsl:text>
                    <xsl:value-of select="//discountEligibility[@discountId=$discountCode]/discountAmount"/> <!-- taking
matching value from outside using custom variable --></xsl:if>
                <xsl:text>),</xsl:text>
            </xsl:for-each>
        </xsl:for-each>
        <xsl:text>+userId::</xsl:text>
        <xsl:value-of select="userId"/>
        <xsl:text>+</xsl:text>
        <xsl:text>Address::</xsl:text><xsl:value-of select="addressInfo/line1"/><xsl:text>,</xsl:text>
        <xsl:value-of select="addressInfo/line2"/><xsl:text>,</xsl:text>
        <xsl:value-of select="addressInfo/line3"/><xsl:text>,</xsl:text>
        <xsl:value-of select="addressInfo/line4"/><xsl:text>,</xsl:text>
        <xsl:value-of select="addressInfo/pinCode"/><xsl:text>,</xsl:text>
    </xsl:template>
    <xsl:template match="insertEvent"> <!-- discount calclation wont be done for INS event , to check -->
        <xsl:text>INS||</xsl:text>
        <xsl:text>sessionID::</xsl:text>
        <xsl:value-of select="@sessionID"/>
        <xsl:text>+incomingTS::</xsl:text>
        <xsl:value-of select="incomingTS"/>
        <xsl:text>+</xsl:text>
        <xsl:text>accountID::</xsl:text>
        <xsl:value-of select="accountID"/>
        <xsl:text>+orderId::</xsl:text>
        <xsl:value-of select="orderId"/><xsl:text>+</xsl:text>
        <xsl:text>orderItems::</xsl:text>
        <xsl:for-each select="orderItems">
            <xsl:for-each select="itemsInfo">
                <xsl:text>(</xsl:text>
                <xsl:value-of select="itemId"/><xsl:text>~</xsl:text>
                <xsl:variable name="discountCheck">
                    <xsl:value-of select="@discountApplied"/>
                </xsl:variable>
                <xsl:if test="$discountCheck=''">
                    <xsl:value-of select="price"/><xsl:text>~NA~NA~NA</xsl:text>
                </xsl:if>
                <xsl:if test="$discountCheck!=''">
                    <xsl:variable name="discountAmountApp">
                        <xsl:value-of select="//discountEligibility[@discountId=$discountCheck]/discountAmount"/>
                    </xsl:variable>
                    <xsl:variable name="totalPrice">
                        <xsl:value-of select="price"/>
                    </xsl:variable>
                    <xsl:if test="$discountAmountApp &gt; $totalPrice">
                        <xsl:value-of select="price"/><xsl:text>~N~</xsl:text>
                        <xsl:value-of select="@discountApplied"/> <!-- [$discountCheck] -->
                        <xsl:text>~</xsl:text>
                        <xsl:value-of select="//discountEligibility[@discountId=$discountCheck]/discountAmount"/> <!-- [$discountAmountApp]-->
                    </xsl:if>
                    <xsl:if test="$discountAmountApp = $totalPrice">
                        <xsl:text>0~Y~</xsl:text>
                        <xsl:value-of select="@discountApplied"/> <!-- [$discountCheck] -->
                        <xsl:text>~</xsl:text>
                        <xsl:value-of select="//discountEligibility[@discountId=$discountCheck]/discountAmount"/> <!-- [$discountAmountApp]-->
                    </xsl:if>
                    <xsl:if test="$discountAmountApp &lt; $totalPrice">
                        <xsl:variable name="testSub" select="$totalPrice - $discountAmountApp"/>
                        <xsl:value-of select="$testSub"/><xsl:text>~Y~</xsl:text>
                        <xsl:value-of select="@discountApplied"/> <!-- [$discountCheck] -->
                        <xsl:text>~</xsl:text>
                        <xsl:value-of select="//discountEligibility[@discountId=$discountCheck]/discountAmount"/> <!-- [$discountAmountApp]-->
                    </xsl:if>
                </xsl:if>
                <xsl:text>~</xsl:text>
                <xsl:value-of select="quantity"/>
                <xsl:text>),</xsl:text>
            </xsl:for-each>
        </xsl:for-each>
        <xsl:text>+userId::</xsl:text><xsl:value-of select="userId"/>
        <xsl:text>+Address::</xsl:text><xsl:value-of select="addressInfo/line1"/><xsl:text>,</xsl:text>
        <xsl:value-of select="addressInfo/line2"/><xsl:text>,</xsl:text>
        <xsl:value-of select="addressInfo/line3"/><xsl:text>,</xsl:text>
        <xsl:value-of select="addressInfo/line4"/><xsl:text>,</xsl:text>
        <xsl:value-of select="addressInfo/pinCode"/><xsl:text>,</xsl:text>
    </xsl:template>
</xsl:stylesheet>