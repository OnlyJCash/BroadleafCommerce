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

import org.apache.commons.lang3.StringUtils;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.MySQLDialect;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

/**
 * @author Jeff Fischer
 */
@Component("blMySqlRebalanceDialect")
public class MySqlRebalanceDialect implements RebalanceDialect {

    @Override
    public boolean canHandle(Dialect hibernateDialect) {
        return hibernateDialect instanceof MySQLDialect;
    }

    @Override
    public String[] createRebalanceQuery(String tableName, String idColumn, String sortColumn, String whereClause, BigDecimal startValue, BigDecimal increment) {
        /*
        This is basic gist of the sql:
            SET @i=1;
            UPDATE BLC_CATEGORY_PRODUCT_XREF SET DISPLAY_ORDER = (@i:=@i+0.00001) WHERE CATEGORY_ID IN (9950) AND
            SNDBX_ID IN (950) AND SNDBX_DELETED_FLAG='N' AND SNDBX_ARCHIVED_FLAG='N' AND DISPLAY_ORDER > 1 AND
            DISPLAY_ORDER < 2 ORDER BY DISPLAY_ORDER;
         */
        String[] response = new String[2];
        StringBuilder sb = new StringBuilder();
        sb.append("SET @i=");
        sb.append(startValue.toString());
        sb.append(";");
        response[0] = sb.toString();
        sb = new StringBuilder();
        sb.append("UPDATE ");
        sb.append(tableName);
        sb.append(" SET ");
        sb.append(sortColumn);
        sb.append(" = (@i:=@i+");
        sb.append(increment);
        sb.append(")");
        if (!StringUtils.isEmpty(whereClause)) {
            if (!whereClause.toUpperCase().trim().startsWith("WHERE")) {
                sb.append(" WHERE");
            }
            sb.append(" ");
            sb.append(whereClause.trim());
        }
        sb.append(" ORDER BY ");
        sb.append(sortColumn);
        sb.append(";");
        response[1] = sb.toString();
        return response;
    }
}
