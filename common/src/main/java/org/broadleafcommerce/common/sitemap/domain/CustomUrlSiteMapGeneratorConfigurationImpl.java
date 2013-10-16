/*
 * Copyright 2008-2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.broadleafcommerce.common.sitemap.domain;

import org.broadleafcommerce.common.presentation.AdminPresentationClass;
import org.broadleafcommerce.common.presentation.AdminPresentationCollection;
import org.broadleafcommerce.common.sitemap.service.type.SiteMapGeneratorType;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.OneToMany;
import javax.persistence.Table;

/**
 * CustomSiteMapGenerator is controlled by this configuration.
 * 
 * @author bpolster
 */
@Entity
@Table(name = "BLC_CUSTOM_URL_SITE_MAP_GEN_CONFIG")
@AdminPresentationClass(friendlyName = "CustomUrlSiteMapGeneratorConfiguration")
public class CustomUrlSiteMapGeneratorConfigurationImpl extends SiteMapGeneratorConfigurationImpl implements CustomUrlSiteMapGeneratorConfiguration {

    private static final long serialVersionUID = 1L;

    @Column(name = "CUSTOM_URL_ENTRIES")
    @OneToMany(mappedBy = "customUrlSiteMapGeneratorConfiguration", targetEntity = SiteMapURLEntryImpl.class, cascade = { CascadeType.ALL }, orphanRemoval = true)
    @AdminPresentationCollection(friendlyName = "CustomSiteMapConfiguration_Custom_URL_Entries")
    protected List<SiteMapURLEntry> customURLEntries = new ArrayList<SiteMapURLEntry>();
    
    @Override
    public SiteMapGeneratorType getSiteMapGeneratorType() {
        return SiteMapGeneratorType.CUSTOM;
    }

    @Override
    public List<SiteMapURLEntry> getCustomURLEntries() {
        return customURLEntries;
    }

    @Override
    public void setCustomURLEntries(List<SiteMapURLEntry> customURLEntries) {
        this.customURLEntries = customURLEntries;
    }

}
