package org.broadleafcommerce.common.sitemap.service;

import org.broadleafcommerce.common.sitemap.domain.SiteMapGeneratorConfiguration;


/**
 * Responsible for generating site map entries.   
 * 
 * Each SiteMapGenerator can generate 
 * 
 * @author bpolster
 *
 */
public interface SiteMapGenerator {
    
    /**
     * Returns true if this SiteMapGenerator is able to process the passed in siteMapGeneratorConfiguration.   
     * 
     * @param siteMapGeneratorConfiguration
     * @return
     */
    public boolean canHandleSiteMapConfiguration(SiteMapGeneratorConfiguration siteMapGeneratorConfiguration);
    
    /**
     * Adds the site map entries to the passed in SiteMap.
     * 
     * @return The number of URLs generated by this generator.
     */
    public int generateSiteMapEntries(SiteMapUtility utility, int currentFileURLCount);
}
