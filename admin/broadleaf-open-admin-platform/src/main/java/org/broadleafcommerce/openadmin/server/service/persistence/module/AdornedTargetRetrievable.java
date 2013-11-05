/*
 * #%L
 * BroadleafCommerce Open Admin Platform
 * %%
 * Copyright (C) 2009 - 2013 Broadleaf Commerce
 * %%
 * NOTICE:  All information contained herein is, and remains
 * the property of Broadleaf Commerce, LLC
 * The intellectual and technical concepts contained
 * herein are proprietary to Broadleaf Commerce, LLC
 * and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Broadleaf Commerce, LLC.
 * #L%
 */
package org.broadleafcommerce.openadmin.server.service.persistence.module;

import org.broadleafcommerce.openadmin.dto.AdornedTargetList;
import org.broadleafcommerce.openadmin.dto.CriteriaTransferObject;
import org.broadleafcommerce.openadmin.dto.Entity;
import org.broadleafcommerce.openadmin.dto.PersistencePackage;

/**
 * @author Jeff Fischer
 */
public interface AdornedTargetRetrievable {

    public AdornedTargetListPersistenceModule.AdornedTargetRetrieval getAdornedTargetRetrieval(PersistencePackage persistencePackage, Entity entity, AdornedTargetList adornedTargetList);

    public AdornedTargetListPersistenceModule.AdornedTargetRetrieval getAdornedTargetRetrieval(PersistencePackage persistencePackage, AdornedTargetList adornedTargetList, CriteriaTransferObject cto);
}
