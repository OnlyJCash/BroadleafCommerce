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

package org.broadleafcommerce.openadmin.server.service.persistence.module;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;
import javax.persistence.EntityManager;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.From;
import javax.persistence.criteria.Path;
import javax.persistence.criteria.Predicate;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.broadleafcommerce.common.exception.SecurityServiceException;
import org.broadleafcommerce.common.exception.ServiceException;
import org.broadleafcommerce.common.presentation.client.OperationType;
import org.broadleafcommerce.common.presentation.client.PersistencePerspectiveItemType;
import org.broadleafcommerce.common.presentation.client.SupportedFieldType;
import org.broadleafcommerce.common.sandbox.SandBoxHelper;
import org.broadleafcommerce.common.web.BroadleafRequestContext;
import org.broadleafcommerce.openadmin.dto.AdornedTargetList;
import org.broadleafcommerce.openadmin.dto.BasicFieldMetadata;
import org.broadleafcommerce.openadmin.dto.CriteriaTransferObject;
import org.broadleafcommerce.openadmin.dto.DynamicResultSet;
import org.broadleafcommerce.openadmin.dto.Entity;
import org.broadleafcommerce.openadmin.dto.FieldMetadata;
import org.broadleafcommerce.openadmin.dto.FilterAndSortCriteria;
import org.broadleafcommerce.openadmin.dto.ForeignKey;
import org.broadleafcommerce.openadmin.dto.MergedPropertyType;
import org.broadleafcommerce.openadmin.dto.PersistencePackage;
import org.broadleafcommerce.openadmin.dto.PersistencePerspective;
import org.broadleafcommerce.openadmin.dto.Property;
import org.broadleafcommerce.openadmin.server.service.persistence.PersistenceManager;
import org.broadleafcommerce.openadmin.server.service.persistence.module.criteria.FieldPath;
import org.broadleafcommerce.openadmin.server.service.persistence.module.criteria.FieldPathBuilder;
import org.broadleafcommerce.openadmin.server.service.persistence.module.criteria.FilterMapping;
import org.broadleafcommerce.openadmin.server.service.persistence.module.criteria.Restriction;
import org.broadleafcommerce.openadmin.server.service.persistence.module.criteria.predicate.PredicateProvider;
import org.broadleafcommerce.openadmin.server.service.persistence.rebalance.SortRebalance;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.metadata.ClassMetadata;
import org.hibernate.persister.entity.AbstractEntityPersister;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

/**
 * @author jfischer
 */
@Component("blAdornedTargetListPersistenceModule")
@Scope("prototype")
public class AdornedTargetListPersistenceModule extends BasicPersistenceModule implements AdornedTargetRetrievable {

    private static final Log LOG = LogFactory.getLog(AdornedTargetListPersistenceModule.class);

    @Resource(name = "blSortRebalance")
    protected SortRebalance sortRebalance;

    @Resource(name="blSandBoxHelper")
    protected SandBoxHelper sandBoxHelper;

    @Override
    public boolean isCompatible(OperationType operationType) {
        return OperationType.ADORNEDTARGETLIST.equals(operationType);
    }

    @Override
    public void extractProperties(Class<?>[] inheritanceLine, Map<MergedPropertyType, Map<String, FieldMetadata>> mergedProperties, List<Property> properties) throws NumberFormatException {
        if (mergedProperties.get(MergedPropertyType.ADORNEDTARGETLIST) != null) {
            extractPropertiesFromMetadata(inheritanceLine, mergedProperties.get(MergedPropertyType.ADORNEDTARGETLIST), properties, true, MergedPropertyType.ADORNEDTARGETLIST);
        }
    }
    
    public List<FilterMapping> getBasicFilterMappings(PersistencePerspective persistencePerspective,
                    CriteriaTransferObject cto, Map<String, FieldMetadata> mergedProperties,
                    String cefqcn) {
        return getFilterMappings(persistencePerspective, cto, cefqcn, mergedProperties);
    }

    public List<FilterMapping> getAdornedTargetFilterMappings(PersistencePerspective persistencePerspective,
                    CriteriaTransferObject cto, Map<String, FieldMetadata> mergedProperties,
                    AdornedTargetList adornedTargetList) throws ClassNotFoundException {
        List<FilterMapping> filterMappings = getFilterMappings(persistencePerspective, cto, adornedTargetList.
                getAdornedTargetEntityClassname(), mergedProperties);
        FilterMapping filterMapping = new FilterMapping()
            .withFieldPath(new FieldPath()
                    .withTargetProperty(adornedTargetList.getLinkedObjectPath() + "." + adornedTargetList.getLinkedIdProperty()))
            .withFilterValues(cto.get(adornedTargetList.getCollectionFieldName()).getFilterValues())
            .withRestriction(new Restriction()
                .withPredicateProvider(new PredicateProvider<Serializable, String>() {
                    @Override
                    public Predicate buildPredicate(CriteriaBuilder builder, FieldPathBuilder fieldPathBuilder, From root,
                                                    String ceilingEntity, String fullPropertyName, Path<Serializable> explicitPath,
                                                    List<String> directValues) {
                        if (String.class.isAssignableFrom(explicitPath.getJavaType())) {
                            return explicitPath.in(directValues);
                        } else {
                            List<Long> converted = new ArrayList<Long>();
                            for (String directValue : directValues) {
                                converted.add(Long.parseLong(directValue));
                            }
                            return explicitPath.in(converted);
                        }
                    }
                })
            );
        filterMappings.add(filterMapping);
        FilterMapping filterMapping2 = new FilterMapping()
            .withFieldPath(new FieldPath()
                    .withTargetProperty(adornedTargetList.getTargetObjectPath() + "." +
                                        adornedTargetList.getTargetIdProperty()))
            .withFilterValues(cto.get(adornedTargetList.getCollectionFieldName() + "Target").getFilterValues())
            .withRestriction(new Restriction()
                .withPredicateProvider(new PredicateProvider<Serializable, String>() {
                    @Override
                    public Predicate buildPredicate(CriteriaBuilder builder, FieldPathBuilder fieldPathBuilder, From root,
                                                    String ceilingEntity, String fullPropertyName, Path<Serializable> explicitPath,
                                                    List<String> directValues) {
                        if (String.class.isAssignableFrom(explicitPath.getJavaType())) {
                            return explicitPath.in(directValues);
                        } else {
                            List<Long> converted = new ArrayList<Long>();
                            for (String directValue : directValues) {
                                converted.add(Long.parseLong(directValue));
                            }
                            return explicitPath.in(converted);
                        }
                    }
                })
            );
        filterMappings.add(filterMapping2);

        return filterMappings;
    }

    @Override
    public void updateMergedProperties(PersistencePackage persistencePackage, Map<MergedPropertyType, Map<String, FieldMetadata>> allMergedProperties) throws ServiceException {
        String ceilingEntityFullyQualifiedClassname = persistencePackage.getCeilingEntityFullyQualifiedClassname();
        try {
            PersistencePerspective persistencePerspective = persistencePackage.getPersistencePerspective();
            AdornedTargetList adornedTargetList = (AdornedTargetList) persistencePerspective.getPersistencePerspectiveItems().get(PersistencePerspectiveItemType.ADORNEDTARGETLIST);
            if (adornedTargetList != null) {
                Class<?>[] entities = persistenceManager.getPolymorphicEntities(adornedTargetList.getAdornedTargetEntityClassname());
                Map<String, FieldMetadata> joinMergedProperties = persistenceManager.getDynamicEntityDao().getMergedProperties(
                        adornedTargetList.getAdornedTargetEntityClassname(),
                        entities,
                        null,
                        new String[]{},
                        new ForeignKey[]{},
                        MergedPropertyType.ADORNEDTARGETLIST,
                        persistencePerspective.getPopulateToOneFields(),
                        persistencePerspective.getIncludeFields(),
                        persistencePerspective.getExcludeFields(),
                        persistencePerspective.getConfigurationKey(),
                        ""
                );
                String idProp = null;
                for (String key : joinMergedProperties.keySet()) {
                    if (joinMergedProperties.get(key) instanceof BasicFieldMetadata && ((BasicFieldMetadata) joinMergedProperties.get(key)).getFieldType() == SupportedFieldType.ID) {
                        idProp = key;
                        break;
                    }
                }
                if (idProp != null) {
                    joinMergedProperties.remove(idProp);
                }
                allMergedProperties.put(MergedPropertyType.ADORNEDTARGETLIST, joinMergedProperties);
            }
        } catch (Exception e) {
            throw new ServiceException("Unable to fetch results for " + ceilingEntityFullyQualifiedClassname, e);
        }
    }

    @Override
    public Entity add(PersistencePackage persistencePackage) throws ServiceException {
        String[] customCriteria = persistencePackage.getCustomCriteria();
        if (customCriteria != null && customCriteria.length > 0) {
            LOG.warn("custom persistence handlers and custom criteria not supported for add types other than BASIC");
        }
        PersistencePerspective persistencePerspective = persistencePackage.getPersistencePerspective();
        String ceilingEntityFullyQualifiedClassname = persistencePackage.getCeilingEntityFullyQualifiedClassname();
        Entity entity = persistencePackage.getEntity();
        AdornedTargetList adornedTargetList = (AdornedTargetList) persistencePerspective.getPersistencePerspectiveItems().get(PersistencePerspectiveItemType.ADORNEDTARGETLIST);
        if (!adornedTargetList.getMutable()) {
            throw new SecurityServiceException("Field is not mutable");
        }
        Entity payload;
        try {
            Class<?>[] entities = persistenceManager.getPolymorphicEntities(ceilingEntityFullyQualifiedClassname);
            Map<String, FieldMetadata> mergedPropertiesTarget = persistenceManager.getDynamicEntityDao().getMergedProperties(
                    ceilingEntityFullyQualifiedClassname,
                    entities,
                    null,
                    persistencePerspective.getAdditionalNonPersistentProperties(),
                    persistencePerspective.getAdditionalForeignKeys(),
                    MergedPropertyType.PRIMARY,
                    persistencePerspective.getPopulateToOneFields(),
                    persistencePerspective.getIncludeFields(),
                    persistencePerspective.getExcludeFields(),
                    persistencePerspective.getConfigurationKey(),
                    ""
            );
            Class<?>[] entities2 = persistenceManager.getPolymorphicEntities(adornedTargetList.getAdornedTargetEntityClassname());
            Map<String, FieldMetadata> mergedProperties = persistenceManager.getDynamicEntityDao().getMergedProperties(
                    adornedTargetList.getAdornedTargetEntityClassname(),
                    entities2,
                    null,
                    new String[]{},
                    new ForeignKey[]{},
                    MergedPropertyType.ADORNEDTARGETLIST,
                    false,
                    new String[]{},
                    new String[]{},
                    null,
                    ""
            );

            CriteriaTransferObject ctoInserted = new CriteriaTransferObject();
            FilterAndSortCriteria filterCriteriaInsertedLinked = ctoInserted.get(adornedTargetList.getCollectionFieldName());
            String linkedPath;
            String targetPath;
            if (adornedTargetList.getInverse()) {
                linkedPath = adornedTargetList.getTargetObjectPath() + "." + adornedTargetList.getTargetIdProperty();
                targetPath = adornedTargetList.getLinkedObjectPath() + "." + adornedTargetList.getLinkedIdProperty();
            } else {
                targetPath = adornedTargetList.getTargetObjectPath() + "." + adornedTargetList.getTargetIdProperty();
                linkedPath = adornedTargetList.getLinkedObjectPath() + "." + adornedTargetList.getLinkedIdProperty();
            }
            filterCriteriaInsertedLinked.setFilterValue(entity.findProperty(adornedTargetList.getInverse() ? targetPath : linkedPath).getValue());
            FilterAndSortCriteria filterCriteriaInsertedTarget = ctoInserted.get(adornedTargetList.getCollectionFieldName() + "Target");
            filterCriteriaInsertedTarget.setFilterValue(entity.findProperty(adornedTargetList.getInverse() ? linkedPath : targetPath).getValue());
            List<FilterMapping> filterMappingsInserted = getAdornedTargetFilterMappings(persistencePerspective, ctoInserted, mergedProperties, adornedTargetList);
            List<Serializable> recordsInserted = getPersistentRecords(adornedTargetList.getAdornedTargetEntityClassname(), filterMappingsInserted, ctoInserted.getFirstResult(), ctoInserted.getMaxResults());
            if (recordsInserted.size() > 0) {
                payload = getRecords(mergedPropertiesTarget, recordsInserted, mergedProperties, adornedTargetList.getTargetObjectPath())[0];
            } else {
                Serializable instance = createPopulatedAdornedTargetInstance(adornedTargetList, entity);
                FieldManager fieldManager = getFieldManager();
                if (fieldManager.getField(instance.getClass(), "id") != null) {
                    fieldManager.setFieldValue(instance, "id", null);
                }
                if (adornedTargetList.getSortField() != null) {
                    CriteriaTransferObject cto = new CriteriaTransferObject();
                    FilterAndSortCriteria filterCriteria = cto.get(adornedTargetList.getCollectionFieldName());
                    List<String> filterValues = new ArrayList<String>();
                    filterValues.add(entity.findProperty(adornedTargetList.getInverse() ? targetPath : linkedPath).getValue());
                    if (entity.findProperty("__originalLinkedId") != null) {
                        filterValues.add(entity.findProperty("__originalLinkedId").getValue());
                    }
                    filterCriteria.setFilterValues(filterValues);
                    FilterAndSortCriteria sortCriteria = cto.get(adornedTargetList.getSortField());
                    sortCriteria.setSortAscending(adornedTargetList.getSortAscending());
                    List<FilterMapping> filterMappings = getAdornedTargetFilterMappings(persistencePerspective, cto,
                                                    mergedProperties, adornedTargetList);
                    if (entity.findProperty(adornedTargetList.getSortField()) == null ||
                            StringUtils.isEmpty(entity.findProperty(adornedTargetList.getSortField()).getValue())) {
                        BigDecimal position = new BigDecimal("0.00000");
                        Integer total = getTotalRecords(adornedTargetList.getAdornedTargetEntityClassname(), filterMappings);
                        total++;
                        position = position.add(new BigDecimal(total));
                        Property sequence = new Property();
                        sequence.setValue(position.toString());
                        sequence.setName(adornedTargetList.getSortField());
                        sequence.setIsDirty(true);
                        entity.addProperty(sequence);
                    }
                    BigDecimal max = (BigDecimal) getMaxValue(adornedTargetList.getAdornedTargetEntityClassname(), filterMappings, adornedTargetList.getSortField());
                    if (max == null) {
                        max = new BigDecimal("0");
                    }
                    fieldManager.setFieldValue(instance, adornedTargetList.getSortField(), max.add(new BigDecimal("1")));
                }
                if (!ceilingEntityFullyQualifiedClassname.equals(adornedTargetList.getAdornedTargetEntityClassname())) {
                    instance = createPopulatedInstance(instance, entity, mergedPropertiesTarget, false);
                }
                instance = getPersistenceManager().getDynamicEntityDao().merge(instance);
                Object temp = getParentInstance(persistencePackage, adornedTargetList);
                instance = updateSequence(persistencePackage, entity, adornedTargetList,
                                                mergedProperties, instance, temp);
                persistenceManager.getDynamicEntityDao().clear();

                List<Serializable> recordsInserted2 = getPersistentRecords(adornedTargetList.getAdornedTargetEntityClassname(), filterMappingsInserted, ctoInserted.getFirstResult(), ctoInserted.getMaxResults());

                payload = getRecords(mergedPropertiesTarget, recordsInserted2, mergedProperties, adornedTargetList.getTargetObjectPath())[0];
            }
        } catch (Exception e) {
            throw new ServiceException("Problem adding new entity : " + e.getMessage(), e);
        }

        return payload;
    }

    @Override
    public Entity update(final PersistencePackage persistencePackage) throws ServiceException {
        final PersistencePerspective persistencePerspective = persistencePackage.getPersistencePerspective();
        final Entity[] entity = new Entity[1];
        entity[0] = persistencePackage.getEntity();
        final AdornedTargetList adornedTargetList = (AdornedTargetList) persistencePerspective
                .getPersistencePerspectiveItems().get(PersistencePerspectiveItemType.ADORNEDTARGETLIST);
        if (!adornedTargetList.getMutable()) {
            throw new SecurityServiceException("Field is not mutable");
        }
        try {
            //include deleted items in the case of a promote so that we have a chance to override
            //if we're updating that same item
            sandBoxHelper.optionallyIncludeDeletedItemsInQueriesAndCollections(new Runnable() {
                @Override
                public void run() {
                    try {
                        AdornedTargetRetrieval adornedTargetRetrieval = new AdornedTargetRetrieval(persistencePackage, entity[0],
                                adornedTargetList).invokeForUpdate();
                        List<Serializable> records = adornedTargetRetrieval.getRecords();

                        Assert.isTrue(!CollectionUtils.isEmpty(records), "Entity not found");

                        Map<String, FieldMetadata> mergedProperties = adornedTargetRetrieval.getMergedProperties();

                        Serializable myRecord = records.get(0);

                        String ceilingEntityFullyQualifiedClassname = persistencePackage.getCeilingEntityFullyQualifiedClassname();
                        Class<?>[] entities = getPersistenceManager().getPolymorphicEntities
                                (ceilingEntityFullyQualifiedClassname);
                        Map<String, FieldMetadata> mergedPropertiesTarget = getPersistenceManager().getDynamicEntityDao()
                                .getMergedProperties(
                                        ceilingEntityFullyQualifiedClassname,
                                        entities,
                                        null,
                                        persistencePerspective.getAdditionalNonPersistentProperties(),
                                        persistencePerspective.getAdditionalForeignKeys(),
                                        MergedPropertyType.PRIMARY,
                                        persistencePerspective.getPopulateToOneFields(),
                                        persistencePerspective.getIncludeFields(),
                                        persistencePerspective.getExcludeFields(),
                                        persistencePerspective.getConfigurationKey(),
                                        ""
                                );
                        Object temp = getParentInstance(persistencePackage, adornedTargetList);
                        myRecord = updateSequence(persistencePackage, entity[0], adornedTargetList,
                                            mergedProperties, myRecord, temp);
                        List<Serializable> myList = new ArrayList<Serializable>();
                        myList.add(myRecord);
                        Entity[] payload = getRecords(mergedPropertiesTarget, myList, mergedProperties,
                                adornedTargetList.getTargetObjectPath());
                        entity[0] = payload[0];
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }, sandBoxHelper.isPromote());


            return entity[0];
        } catch (Exception e) {
            throw new ServiceException("Problem updating entity : " + e.getMessage(), e);
        }
    }

    @Override
    public void remove(PersistencePackage persistencePackage) throws ServiceException {
        String[] customCriteria = persistencePackage.getCustomCriteria();
        if (customCriteria != null && customCriteria.length > 0) {
            LOG.warn("custom persistence handlers and custom criteria not supported for remove types other than BASIC");
        }
        PersistencePerspective persistencePerspective = persistencePackage.getPersistencePerspective();
        Entity entity = persistencePackage.getEntity();
        try {
            AdornedTargetList adornedTargetList = (AdornedTargetList) persistencePerspective
                    .getPersistencePerspectiveItems().get(PersistencePerspectiveItemType.ADORNEDTARGETLIST);
            if (!adornedTargetList.getMutable()) {
                throw new SecurityServiceException("Field is not mutable");
            }
            Class<?>[] entities = persistenceManager.getPolymorphicEntities(adornedTargetList
                    .getAdornedTargetEntityClassname());
            Map<String, FieldMetadata> mergedProperties = persistenceManager.getDynamicEntityDao().getMergedProperties(
                    adornedTargetList.getAdornedTargetEntityClassname(),
                    entities,
                    null,
                    new String[]{},
                    new ForeignKey[]{},
                    MergedPropertyType.ADORNEDTARGETLIST,
                    false,
                    new String[]{},
                    new String[]{},
                    null,
                    ""
            );
            CriteriaTransferObject ctoInserted = new CriteriaTransferObject();
            FilterAndSortCriteria filterCriteriaInsertedLinked = ctoInserted.get(adornedTargetList
                    .getCollectionFieldName());
            {
                List<String> filterValues = new ArrayList<String>();
                filterValues.add(entity.findProperty(adornedTargetList.getLinkedObjectPath() +
                        "." + adornedTargetList.getLinkedIdProperty()).getValue());
                if (entity.findProperty("__originalLinkedId") != null) {
                    filterValues.add(entity.findProperty("__originalLinkedId").getValue());
                }
                filterCriteriaInsertedLinked.setFilterValues(filterValues);
            }
            FilterAndSortCriteria filterCriteriaInsertedTarget = ctoInserted.get(adornedTargetList
                    .getCollectionFieldName() + "Target");
            {
                List<String> filterValues = new ArrayList<String>();
                filterValues.add(entity.findProperty(adornedTargetList.getTargetObjectPath() +
                        "." + adornedTargetList.getTargetIdProperty()).getValue());
                if (entity.findProperty("__originalTargetId") != null) {
                    filterValues.add(entity.findProperty("__originalTargetId").getValue());
                }
                filterCriteriaInsertedTarget.setFilterValues(filterValues);
            }
            List<FilterMapping> filterMappings = getAdornedTargetFilterMappings(persistencePerspective,
                    ctoInserted, mergedProperties, adornedTargetList);
            List<Serializable> recordsInserted = getPersistentRecords(adornedTargetList
                    .getAdornedTargetEntityClassname(), filterMappings, ctoInserted.getFirstResult(),
                    ctoInserted.getMaxResults());

            Assert.isTrue(!CollectionUtils.isEmpty(recordsInserted), "Entity not found");

            persistenceManager.getDynamicEntityDao().remove(recordsInserted.get(0));
        } catch (Exception e) {
            throw new ServiceException("Problem removing entity : " + e.getMessage(), e);
        }
    }

    @Override
    public DynamicResultSet fetch(PersistencePackage persistencePackage, CriteriaTransferObject cto) throws ServiceException {
        PersistencePerspective persistencePerspective = persistencePackage.getPersistencePerspective();
        String ceilingEntityFullyQualifiedClassname = persistencePackage.getCeilingEntityFullyQualifiedClassname();
        AdornedTargetList adornedTargetList = (AdornedTargetList) persistencePerspective.getPersistencePerspectiveItems().get(PersistencePerspectiveItemType.ADORNEDTARGETLIST);
        Entity[] payload;
        int totalRecords;
        try {
            Class<?>[] entities = persistenceManager.getPolymorphicEntities(ceilingEntityFullyQualifiedClassname);
            Map<String, FieldMetadata> mergedPropertiesTarget = persistenceManager.getDynamicEntityDao().getMergedProperties(
                    ceilingEntityFullyQualifiedClassname,
                    entities,
                    null,
                    persistencePerspective.getAdditionalNonPersistentProperties(),
                    persistencePerspective.getAdditionalForeignKeys(),
                    MergedPropertyType.PRIMARY,
                    persistencePerspective.getPopulateToOneFields(),
                    persistencePerspective.getIncludeFields(),
                    persistencePerspective.getExcludeFields(),
                    persistencePerspective.getConfigurationKey(),
                    ""
            );
            
            AdornedTargetRetrieval adornedTargetRetrieval = new AdornedTargetRetrieval(persistencePackage, adornedTargetList, cto).invokeForFetch();
            List<Serializable> records = adornedTargetRetrieval.getRecords();
            Map<String, FieldMetadata> mergedProperties = adornedTargetRetrieval.getMergedProperties();
            payload = getRecords(mergedPropertiesTarget, records, mergedProperties, adornedTargetList.getTargetObjectPath());
            totalRecords = getTotalRecords(adornedTargetList.getAdornedTargetEntityClassname(), adornedTargetRetrieval.getFilterMappings());
        } catch (Exception e) {
            throw new ServiceException("Unable to fetch results for " + adornedTargetList.getAdornedTargetEntityClassname(), e);
        }

        DynamicResultSet results = new DynamicResultSet(null, payload, totalRecords);

        return results;
    }

    @Override
    public AdornedTargetRetrieval getAdornedTargetRetrieval(PersistencePackage persistencePackage, Entity entity, AdornedTargetList adornedTargetList) {
        return new AdornedTargetRetrieval(persistencePackage, entity, adornedTargetList);
    }

    @Override
    public AdornedTargetRetrieval getAdornedTargetRetrieval(PersistencePackage persistencePackage, AdornedTargetList adornedTargetList, CriteriaTransferObject cto) {
        return new AdornedTargetRetrieval(persistencePackage, adornedTargetList, cto);
    }

    protected Serializable updateSequence(PersistencePackage persistencePackage, Entity entity,
                                        AdornedTargetList adornedTargetList, Map<String, FieldMetadata> mergedProperties,
                                        Serializable myRecord, Object temp) throws Exception {
        BigDecimal newSequence = null;
        boolean isStart = false;
        boolean isEnd = false;
        boolean ignoreRebalance = false;
        if (adornedTargetList.getSortField() != null &&
                entity.findProperty(adornedTargetList.getSortField()) != null &&
                entity.findProperty(adornedTargetList.getSortField()).getValue() != null) {
            BigDecimal requestedIndex = new BigDecimal(entity.findProperty(adornedTargetList.getSortField())
                    .getValue());
            if (requestedIndex.scale() > 0) {
                newSequence = requestedIndex;
                ignoreRebalance = true;
            } else {
                SequenceValue sequenceValue = getSequenceValue(entity, adornedTargetList, persistencePackage,
                        myRecord);
                newSequence = sequenceValue.getNewSequence();
                isStart = sequenceValue.isStart();
                isEnd = sequenceValue.isEnd();
            }
        }
        getFieldManager().setFieldValue(myRecord, adornedTargetList.getLinkedObjectPath(), temp);
        myRecord = createPopulatedInstance(myRecord, entity, mergedProperties, false);
        if (newSequence != null) {
            getFieldManager().setFieldValue(myRecord, adornedTargetList.getSortField(), newSequence);
        }
        myRecord = getPersistenceManager().getDynamicEntityDao().merge(myRecord);
        if (newSequence != null && !isStart && !isEnd && !ignoreRebalance) {
            getPersistenceManager().getDynamicEntityDao().flush();
            rebalanceAdornedTargets(entity, adornedTargetList, newSequence);
        }

        return myRecord;
    }

    protected Serializable createPopulatedAdornedTargetInstance(AdornedTargetList adornedTargetList, Entity entity) throws InstantiationException, IllegalAccessException, ClassNotFoundException, NumberFormatException, InvocationTargetException, NoSuchMethodException, FieldNotAvailableException {
        Serializable instance = (Serializable) Class.forName(StringUtils.isEmpty(adornedTargetList
                .getAdornedTargetEntityPolymorphicType())? adornedTargetList.getAdornedTargetEntityClassname(): adornedTargetList.getAdornedTargetEntityPolymorphicType()).newInstance();
        String targetPath = adornedTargetList.getTargetObjectPath() + "." + adornedTargetList.getTargetIdProperty();
        String linkedPath = adornedTargetList.getLinkedObjectPath() + "." + adornedTargetList.getLinkedIdProperty();
        getFieldManager().setFieldValue(instance, linkedPath, Long.valueOf(entity.findProperty(linkedPath).getValue()));

        Object test1 = getFieldManager().getFieldValue(instance, adornedTargetList.getLinkedObjectPath());
        Object test1PersistedObject = persistenceManager.getDynamicEntityDao().retrieve(test1.getClass(), Long.valueOf(entity.findProperty(linkedPath).getValue()));
        Assert.isTrue(test1PersistedObject != null, "Entity not found");

        Class<?> type = getFieldManager().getField(instance.getClass(), targetPath).getType();
        if (String.class.isAssignableFrom(type)) {
            getFieldManager().setFieldValue(instance, targetPath, entity.findProperty(targetPath).getValue());
        } else {
            getFieldManager().setFieldValue(instance, targetPath, Long.valueOf(entity.findProperty(targetPath).getValue()));
        }

        Object test2 = getFieldManager().getFieldValue(instance, adornedTargetList.getTargetObjectPath());
        Object test2PersistedObject;
        if (String.class.isAssignableFrom(type)) {
            test2PersistedObject = persistenceManager.getDynamicEntityDao().retrieve(test2.getClass(), entity.findProperty(targetPath).getValue());
        } else {
            test2PersistedObject = persistenceManager.getDynamicEntityDao().retrieve(test2.getClass(), Long.valueOf(entity.findProperty(targetPath).getValue()));
        }
        Assert.isTrue(test2PersistedObject != null, "Entity not found");

        return instance;
    }

    protected Object getParentInstance(PersistencePackage persistencePackage, AdornedTargetList adornedTargetList) {
        Property parentProperty = persistencePackage.getEntity().findProperty(adornedTargetList
                .getLinkedObjectPath() + "." + adornedTargetList.getLinkedIdProperty());
        Long requestedParent = Long.parseLong(parentProperty.getValue());
        Class<?> linkedObjectType = getAdornedTargetParentType(getPersistenceManager(), adornedTargetList);
        return getPersistenceManager().getDynamicEntityDao().getStandardEntityManager().unwrap(Session.class).
                get(linkedObjectType, requestedParent);
    }

    protected Class<?> getAdornedTargetParentType(PersistenceManager persistenceManager, AdornedTargetList
            adornedTargetList) {
        Class<?> joinEntityClass;
        try {
            joinEntityClass = Class.forName(adornedTargetList.getAdornedTargetEntityClassname());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        Class<?> linkedObjectType = persistenceManager.getDynamicEntityDao().getFieldManager().getField(joinEntityClass,
                adornedTargetList.getLinkedObjectPath()).getType();
        Class<?>[] implementations = persistenceManager.getDynamicEntityDao()
                .getAllPolymorphicEntitiesFromCeiling(linkedObjectType);
        return implementations[implementations.length - 1];
    }

    protected void rebalanceAdornedTargets(Entity entity, AdornedTargetList adornedTargetList,
                                           BigDecimal newSequence) throws ClassNotFoundException {
        //rebalance the list
        EntityManager em = getPersistenceManager().getDynamicEntityDao().getStandardEntityManager();
        SessionFactory sessionFactory = em.unwrap(Session.class).getSessionFactory();
        Class<?> adornedTargetClass = Class.forName(adornedTargetList.getAdornedTargetEntityClassname());
        ClassMetadata metadata = sessionFactory.getClassMetadata(adornedTargetClass);
        String tableName = ((AbstractEntityPersister) metadata).getTableName();
        //build the where clause - we only want parent linked items that belong to our sandbox
        String foreignKeyColumnName = ((AbstractEntityPersister) metadata).getPropertyColumnNames(adornedTargetList
                .getLinkedObjectPath())[0];
        String sortColumnName = ((AbstractEntityPersister) metadata).getPropertyColumnNames(adornedTargetList
                .getSortField())[0];
        String foreignKeyColumnValue = entity.findProperty(adornedTargetList.getLinkedObjectPath() + "." +
                adornedTargetList.getLinkedIdProperty()).getValue();
        StringBuilder sb = new StringBuilder();
        sb.append(foreignKeyColumnName);
        sb.append(" IN (");
        sb.append(foreignKeyColumnValue);
        BroadleafRequestContext context = BroadleafRequestContext.getBroadleafRequestContext();
        if (context.getSandBoxId() == null) {
            sb.append(") AND SNDBX_ID IS NULL");
        } else {
            sb.append(") AND SNDBX_ID IN (");
            sb.append(context.getSandBoxId());
            sb.append(")");
        }
        sb.append(" AND SNDBX_DELETED_FLAG IS NULL AND SNDBX_ARCHIVED_FLAG IS NULL");
        String idColumnName = ((AbstractEntityPersister) metadata).getPropertyColumnNames(
                (String) getPersistenceManager().getDynamicEntityDao().getIdMetadata(adornedTargetClass).get("name"))[0];
        BigDecimal floor;
        BigDecimal increment;
        //Boolean unownedAT = (Boolean) context.getAdditionalProperties().get
        // (AbstractSandBoxPersistenceManagerEventHandler.UNOWNED_ADORNED_TARGET_CONTEXT_KEY);
        //if (unownedAT != null && unownedAT) {
        //this adorned target is not wholly owned by the clone. We'll only rebalance inserted items between floor
        //and ceiling of our newSequence value. We make the assumption here that production sort values are groomed
        //appropriately with consecutive integer values.
        floor = newSequence.setScale(0, BigDecimal.ROUND_FLOOR);
        BigDecimal ceiling = newSequence.setScale(0, BigDecimal.ROUND_CEILING);
        sb.append(" AND ");
        sb.append(sortColumnName);
        sb.append(" > ");
        sb.append(floor);
        sb.append(" AND ");
        sb.append(sortColumnName);
        sb.append(" < ");
        sb.append(ceiling);
        increment = new BigDecimal(".00001");
        //} else {
        //this adorned target is wholly owned by the clone, rebalance the whole collection
        //increment = new BigDecimal("1");
        //floor = new BigDecimal("0");
        //}
        //TODO create additional rebalance dialects for db systems other than MySql
        sortRebalance.rebalanceRows(tableName, idColumnName, sortColumnName, sb.toString(), floor, increment);
    }

    protected SequenceValue getSequenceValue(Entity entity, AdornedTargetList adornedTargetList,
                                             PersistencePackage persistencePackage,
                                             Serializable myRecord) throws Exception {
        Integer requestedSequence = new BigDecimal(entity.findProperty(adornedTargetList.getSortField()).getValue())
                .intValueExact();
        AdornedTargetRetrieval positionRetrieval = new AdornedTargetRetrieval(persistencePackage, entity,
                adornedTargetList).invokeForPositionFetch(requestedSequence);
        BigDecimal sequence1;
        BigDecimal sequence2 = null;
        BigDecimal sequence3 = null;
        Serializable record1;
        Serializable record2 = null;
        Serializable record3 = null;
        int totalRecords = getTotalRecords(adornedTargetList.getAdornedTargetEntityClassname(),
                positionRetrieval.getFilterMappings());
        if (positionRetrieval.getRecords().size() > 1) {
            record1 = positionRetrieval.getRecords().get(0);
            record2 = positionRetrieval.getRecords().get(1);
            sequence1 = (BigDecimal) getFieldManager().getFieldValue(record1, adornedTargetList.getSortField());
            sequence2 = (BigDecimal) getFieldManager().getFieldValue(record2, adornedTargetList.getSortField());
            if (positionRetrieval.getRecords().size() > 2) {
                record3 = positionRetrieval.getRecords().get(2);
                sequence3 = (BigDecimal) getFieldManager().getFieldValue(record3,
                        adornedTargetList.getSortField());
            }
        } else {
            record1 = positionRetrieval.getRecords().get(0);
            sequence1 = (BigDecimal) getFieldManager().getFieldValue(record1, adornedTargetList.getSortField());
        }
        BigDecimal mySequence = (BigDecimal) getFieldManager().getFieldValue(myRecord, adornedTargetList.getSortField());
        boolean isStart = requestedSequence - 1 == 0;
        boolean isEnd = requestedSequence == totalRecords;
        BigDecimal currentSequence;
        Serializable currentRecord;
        BigDecimal beforeCurrentSequence = null;
        if (isStart) {
            currentSequence = sequence1;
            currentRecord = record1;
        } else {
            if (sequence2 != null) {
                if ((myRecord.equals(record1) || mySequence.compareTo(sequence2) < 0) && sequence3 != null) {
                    beforeCurrentSequence = sequence2;
                    currentSequence = sequence3;
                } else {
                    beforeCurrentSequence = sequence1;
                    currentSequence = sequence2;
                }
                currentRecord = record2;
            } else {
                currentSequence = sequence1;
                currentRecord = record1;
            }
        }
        BigDecimal newSequence = null;
        if (!myRecord.equals(currentRecord)) {
            if (isStart) {
                newSequence = currentSequence.subtract(new BigDecimal("1"));
            } else if (isEnd) {
                newSequence = currentSequence.add(new BigDecimal("1"));
            } else {
                //Simply get the inserted item in there for now. We'll rebalance a little bit later
                newSequence = beforeCurrentSequence.add(currentSequence.subtract(beforeCurrentSequence).divide(new
                        BigDecimal("2")));
            }
        }
        SequenceValue response = new SequenceValue();
        response.setNewSequence(newSequence);
        response.setEnd(isEnd);
        response.setStart(isStart);

        return response;
    }

    public class SequenceValue {

        protected BigDecimal newSequence;
        protected boolean isStart;
        protected boolean isEnd;

        public boolean isEnd() {
            return isEnd;
        }

        public void setEnd(boolean end) {
            isEnd = end;
        }

        public boolean isStart() {
            return isStart;
        }

        public void setStart(boolean start) {
            isStart = start;
        }

        public BigDecimal getNewSequence() {
            return newSequence;
        }

        public void setNewSequence(BigDecimal newSequence) {
            this.newSequence = newSequence;
        }
    }

    public class AdornedTargetRetrieval {
        private PersistencePackage persistencePackage;
        private PersistencePerspective persistencePerspective;
        private Entity entity;
        private AdornedTargetList adornedTargetList;
        private Map<String, FieldMetadata> mergedProperties;
        private List<Serializable> records;
        private List<FilterMapping> filterMappings;
        private CriteriaTransferObject cto;

        // This constructor is used by the update method
        public AdornedTargetRetrieval(PersistencePackage persistencePackage, Entity entity,
                                      AdornedTargetList adornedTargetList) {
            this(persistencePackage, adornedTargetList, new CriteriaTransferObject());
            this.entity = entity;
        }

        // This constructor is used by the fetch method
        public AdornedTargetRetrieval(PersistencePackage persistencePackage, AdornedTargetList adornedTargetList,
                                      CriteriaTransferObject cto) {
            this.persistencePackage = persistencePackage;
            this.persistencePerspective = persistencePackage.getPersistencePerspective();
            this.adornedTargetList = adornedTargetList;
            this.cto = cto;
        }

        public Map<String, FieldMetadata> getMergedProperties() {
            return mergedProperties;
        }

        public List<Serializable> getRecords() {
            return records;
        }

        public List<FilterMapping> getFilterMappings() {
            return filterMappings;
        }

        public AdornedTargetRetrieval invokeForFetch() throws
                ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException,
                FieldNotAvailableException, NoSuchFieldException {
            invokeInternal();
            return this;
        }

        public AdornedTargetRetrieval invokeForUpdate() throws
                ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException,
                FieldNotAvailableException, NoSuchFieldException {
            FilterAndSortCriteria filterCriteriaInsertedLinked = cto.get(adornedTargetList.getCollectionFieldName());
            List<String> filterValues = new ArrayList<String>();
            filterValues.add(entity.findProperty(adornedTargetList.getLinkedObjectPath() +
                    "." + adornedTargetList.getLinkedIdProperty()).getValue());
            if (entity.findProperty("__originalLinkedId") != null) {
                filterValues.add(entity.findProperty("__originalLinkedId").getValue());
            }
            filterCriteriaInsertedLinked.setFilterValues(filterValues);
            FilterAndSortCriteria filterCriteriaInsertedTarget = cto.get(adornedTargetList.getCollectionFieldName() +
                    "Target");
            filterCriteriaInsertedTarget.setFilterValue(entity.findProperty(adornedTargetList.getTargetObjectPath() +
                    "." + adornedTargetList.getTargetIdProperty()).getValue());

            invokeInternal();

            return this;
        }

        public AdornedTargetRetrieval invokeForPositionFetch(Integer position) throws ClassNotFoundException,
                NoSuchMethodException, IllegalAccessException, InvocationTargetException, FieldNotAvailableException,
                NoSuchFieldException {
            FilterAndSortCriteria filterCriteriaInsertedLinked = cto.get(adornedTargetList.getCollectionFieldName());
            List<String> filterValues = new ArrayList<String>();
            filterValues.add(entity.findProperty(adornedTargetList.getLinkedObjectPath() +
                    "." + adornedTargetList.getLinkedIdProperty()).getValue());
            if (entity.findProperty("__originalLinkedId") != null) {
                filterValues.add(entity.findProperty("__originalLinkedId").getValue());
            }
            filterCriteriaInsertedLinked.setFilterValues(filterValues);
            if (position - 2 >= 0) {
                cto.setFirstResult(position - 2);
            } else {
                cto.setFirstResult(position - 1);
            }
            cto.setMaxResults(3);

            invokeInternal();

            return this;
        }

        private void invokeInternal() throws ClassNotFoundException {
            if (adornedTargetList.getSortField() != null) {
                FilterAndSortCriteria sortCriteria = cto.get(adornedTargetList.getSortField());
                sortCriteria.setSortAscending(adornedTargetList.getSortAscending());
            }

            Class<?>[] entities = getPersistenceManager().getPolymorphicEntities(adornedTargetList
                    .getAdornedTargetEntityClassname());
            mergedProperties = getPersistenceManager().getDynamicEntityDao().getMergedProperties(
                    adornedTargetList.getAdornedTargetEntityClassname(),
                    entities,
                    null,
                    new String[]{},
                    new ForeignKey[]{},
                    MergedPropertyType.ADORNEDTARGETLIST,
                    persistencePerspective.getPopulateToOneFields(),
                    persistencePerspective.getIncludeFields(),
                    persistencePerspective.getExcludeFields(),
                    persistencePerspective.getConfigurationKey(),
                    ""
            );
            filterMappings = getAdornedTargetFilterMappings(persistencePerspective, cto, mergedProperties,
                    adornedTargetList);

            String ceilingEntityFullyQualifiedClassname = persistencePackage.getCeilingEntityFullyQualifiedClassname();
            Class<?>[] entities2 = getPersistenceManager().getPolymorphicEntities
                    (ceilingEntityFullyQualifiedClassname);
            Map<String, FieldMetadata> mergedPropertiesTarget = getPersistenceManager().getDynamicEntityDao()
                    .getMergedProperties(
                            ceilingEntityFullyQualifiedClassname,
                            entities2,
                            null,
                            persistencePerspective.getAdditionalNonPersistentProperties(),
                            persistencePerspective.getAdditionalForeignKeys(),
                            MergedPropertyType.PRIMARY,
                            persistencePerspective.getPopulateToOneFields(),
                            persistencePerspective.getIncludeFields(),
                            persistencePerspective.getExcludeFields(),
                            persistencePerspective.getConfigurationKey(),
                            ""
                    );

            // We need to make sure that the target merged properties have the target object path prefix
            Map<String, FieldMetadata> convertedMergedPropertiesTarget = new HashMap<String, FieldMetadata>();
            String prefix = adornedTargetList.getTargetObjectPath();
            for (Map.Entry<String, FieldMetadata> entry : mergedPropertiesTarget.entrySet()) {
                convertedMergedPropertiesTarget.put(prefix + "." + entry.getKey(), entry.getValue());
            }

            // We also need to make sure that the cto filter and sort criteria have the prefix
            Map<String, FilterAndSortCriteria> convertedCto = new HashMap<String, FilterAndSortCriteria>();
            for (Map.Entry<String, FilterAndSortCriteria> entry : cto.getCriteriaMap().entrySet()) {
                if (adornedTargetList.getSortField() != null && entry.getKey().equals(adornedTargetList.getSortField
                        ())) {
                    convertedCto.put(entry.getKey(), entry.getValue());
                } else {
                    convertedCto.put(prefix + "." + entry.getKey(), entry.getValue());
                }
            }
            cto.setCriteriaMap(convertedCto);
            
            List<FilterMapping> filterMappings2 = getBasicFilterMappings(persistencePerspective, cto, convertedMergedPropertiesTarget, ceilingEntityFullyQualifiedClassname);
            for (FilterMapping fm : filterMappings2) {
                fm.setInheritedFromClass(entities[0]);
            }
            filterMappings.addAll(filterMappings2);

            records = getPersistentRecords(adornedTargetList.getAdornedTargetEntityClassname(),
                    filterMappings, cto.getFirstResult(), cto.getMaxResults());
        }
    }
}
