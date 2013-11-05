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
package org.broadleafcommerce.openadmin.server.service.persistence.rebalance;

import org.hibernate.dialect.Dialect;

import java.math.BigDecimal;

/**
 * @author Jeff Fischer
 */
public interface RebalanceDialect {

    public boolean canHandle(Dialect hibernateDialect);

    public String[] createRebalanceQuery(String tableName, String idColumn, String sortColumn, String whereClause, BigDecimal startValue, BigDecimal increment);

}
