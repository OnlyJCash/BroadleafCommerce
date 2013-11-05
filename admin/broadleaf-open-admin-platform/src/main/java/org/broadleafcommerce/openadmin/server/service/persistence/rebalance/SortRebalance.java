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

import org.hibernate.Session;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.jdbc.ReturningWork;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Jeff Fischer
 */
@Repository("blSortRebalance")
public class SortRebalance {

    @PersistenceContext(unitName="blPU")
    protected EntityManager em;

    @Resource(name="blRebalanceDialects")
    protected List<RebalanceDialect> dialects = new ArrayList<RebalanceDialect>();

    public List<RebalanceDialect> getDialects() {
        return dialects;
    }

    public void setDialects(List<RebalanceDialect> dialects) {
        this.dialects = dialects;
    }

    public int rebalanceRows(String tableName, String idColumn, String sortColumn, String whereClause, BigDecimal startValue, BigDecimal increment) {
        SessionFactoryImplementor factory = (SessionFactoryImplementor) em.unwrap(Session.class).getSessionFactory();
        Dialect hibernateDialect = factory.getDialect();
        for (RebalanceDialect dialect : dialects) {
            if (dialect.canHandle(hibernateDialect)) {
                final String[] queries = dialect.createRebalanceQuery(tableName, idColumn, sortColumn, whereClause, startValue, increment);
                return em.unwrap(Session.class).doReturningWork(new ReturningWork<Integer>() {
                    @Override
                    public Integer execute(Connection connection) throws SQLException {
                        Statement statement = connection.createStatement();
                        for (final String query : queries) {
                            statement.addBatch(query);
                        }
                        int count = 0;
                        int[] updateCounts = statement.executeBatch();
                        for (int updateCount : updateCounts) {
                            count += updateCount;
                        }
                        return count;
                    }
                });
            }
        }
        throw new UnsupportedOperationException("Unable to find a compatible RebalanceDialect instance for the current " +
                "Hibernate dialect ("+hibernateDialect.getClass().getName()+"). If you wish to add sort column rebalance " +
                "support for a database type, create an implementation of RebalanceDialect and add it to the " +
                "blRebalanceDialects list in your Spring app context.");
    }
}
