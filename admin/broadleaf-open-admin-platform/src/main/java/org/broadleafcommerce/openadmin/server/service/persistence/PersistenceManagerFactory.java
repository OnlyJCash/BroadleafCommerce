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

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Service;

/**
 * @author Jeff Fischer
 */
@Service("blPersistenceManagerFactory")
public class PersistenceManagerFactory implements ApplicationContextAware {

    private static ApplicationContext applicationContext;
    public static final String DEFAULTPERSISTENCEMANAGERREF = "blPersistenceManager";
    protected static String persistenceManagerRef = DEFAULTPERSISTENCEMANAGERREF;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        PersistenceManagerFactory.applicationContext = applicationContext;
    }

    public static PersistenceManager getPersistenceManager() {
        if (PersistenceManagerContext.getPersistenceManagerContext() != null) {
            return PersistenceManagerContext.getPersistenceManagerContext().getPersistenceManager();
        }
        throw new IllegalStateException("PersistenceManagerContext is not set on ThreadLocal. If you want to use the " +
                "non-cached version, try getPersistenceManager(TargetModeType)");
    }

    public static PersistenceManager getPersistenceManager(TargetModeType targetModeType) {
        PersistenceManager persistenceManager = (PersistenceManager) applicationContext.getBean(persistenceManagerRef);
        persistenceManager.setTargetMode(targetModeType);

        return persistenceManager;
    }

    public static boolean isPersistenceManagerActive() {
        return applicationContext.containsBean(getPersistenceManagerRef());
    }

    public static void startPersistenceManager(TargetModeType targetModeType) {
        PersistenceManagerContext context = new PersistenceManagerContext();
        context.addPersistenceManager(getPersistenceManager(targetModeType));
        PersistenceManagerContext.addPersistenceManagerContext(context);
    }

    public static void endPersistenceManager() {
        PersistenceManagerContext context = PersistenceManagerContext.getPersistenceManagerContext();
        if (context != null) {
            context.remove();
        }
    }

    public static String getPersistenceManagerRef() {
        return persistenceManagerRef;
    }

    public static void setPersistenceManagerRef(String persistenceManagerRef) {
        PersistenceManagerFactory.persistenceManagerRef = persistenceManagerRef;
    }
}
