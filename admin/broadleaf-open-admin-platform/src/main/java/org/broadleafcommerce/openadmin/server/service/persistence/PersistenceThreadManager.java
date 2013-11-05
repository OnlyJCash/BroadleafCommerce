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
package org.broadleafcommerce.openadmin.server.service.persistence;

import org.springframework.stereotype.Service;

/**
 * @author Jeff Fischer
 */
@Service("blPersistenceThreadManager")
public class PersistenceThreadManager {

    public <T, G extends Throwable> T operation(TargetModeType targetModeType, Persistable<T, G> persistable) throws G {
        try {
            PersistenceManagerFactory.startPersistenceManager(targetModeType);
            return persistable.execute();
        } finally {
            PersistenceManagerFactory.endPersistenceManager();
        }
    }
}
