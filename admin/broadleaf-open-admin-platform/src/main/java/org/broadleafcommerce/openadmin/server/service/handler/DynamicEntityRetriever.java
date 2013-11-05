package org.broadleafcommerce.openadmin.server.service.handler;

import java.io.Serializable;
import java.util.List;

import org.broadleafcommerce.openadmin.dto.Entity;

/**
 * @author Jeff Fischer
 */
public interface DynamicEntityRetriever {

    Entity fetchDynamicEntity(Serializable root, List<String> dirtyFields, boolean includeId) throws Exception;

}
