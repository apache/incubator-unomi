/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.unomi.persistence.spi;

import org.apache.unomi.api.Item;
import org.apache.unomi.api.PartialList;
import org.apache.unomi.api.PropertyType;
import org.apache.unomi.api.conditions.Condition;
import org.apache.unomi.persistence.spi.aggregate.BaseAggregate;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * A service to provide persistence and retrieval of context server entities.
 */
public interface PersistenceService {

    /**
     * Retrieves all known items of the specified class.
     * <em>WARNING</em>: this method can be quite computationally intensive and calling the paged version {@link #getAllItems(Class, int, int, String)} is preferred.
     *
     * @param <T>   the type of the {@link Item}s we want to retrieve
     * @param clazz the {@link Item} subclass of entities we want to retrieve
     * @return a list of all known items with the given type
     */
    <T extends Item> List<T> getAllItems(Class<T> clazz);

    /**
     * Retrieves all known items of the specified class, ordered according to the specified {@code sortBy} String and and paged: only {@code size} of them are retrieved,
     * starting with the {@code offset}-th one.
     *
     * TODO: use a Query object instead of distinct parameters?
     *
     * @param <T>    the type of the {@link Item}s we want to retrieve
     * @param clazz  the {@link Item} subclass of entities we want to retrieve
     * @param offset zero or a positive integer specifying the position of the first item in the total ordered collection of matching items
     * @param size   a positive integer specifying how many matching items should be retrieved or {@code -1} if all of them should be retrieved
     * @param sortBy an optional ({@code null} if no sorting is required) String of comma ({@code ,}) separated property names on which ordering should be performed, ordering
     *               elements according to the property order in the
     *               String, considering each in turn and moving on to the next one in case of equality of all preceding ones. Each property name is optionally followed by
     *               a column ({@code :}) and an order specifier: {@code asc} or {@code desc}.
     * @return a {@link PartialList} of pages items with the given type
     */
    <T extends Item> PartialList<T> getAllItems(Class<T> clazz, int offset, int size, String sortBy);

    /**
     * Retrieves all known items of the specified class, ordered according to the specified {@code sortBy} String and and paged: only {@code size} of them are retrieved,
     * starting with the {@code offset}-th one.
     *
     * TODO: use a Query object instead of distinct parameters?
     *
     * @param <T>    the type of the {@link Item}s we want to retrieve
     * @param clazz  the {@link Item} subclass of entities we want to retrieve
     * @param offset zero or a positive integer specifying the position of the first item in the total ordered collection of matching items
     * @param size   a positive integer specifying how many matching items should be retrieved or {@code -1} if all of them should be retrieved
     * @param sortBy an optional ({@code null} if no sorting is required) String of comma ({@code ,}) separated property names on which ordering should be performed, ordering
     *               elements according to the property order in the
     *               String, considering each in turn and moving on to the next one in case of equality of all preceding ones. Each property name is optionally followed by
     *               a column ({@code :}) and an order specifier: {@code asc} or {@code desc}.
     * @param scrollTimeValidity  the time the scrolling query should stay valid. This must contain a time unit value such as the ones supported by ElasticSearch, such as
     *      *                     the ones declared here : https://www.elastic.co/guide/en/elasticsearch/reference/current/common-options.html#time-units
     * @return a {@link PartialList} of pages items with the given type
     */
    <T extends Item> PartialList<T> getAllItems(final Class<T> clazz, int offset, int size, String sortBy, String scrollTimeValidity);

    /**
     * Set settings of the persistence service
     *
     * @param settings map of setting name and it's value
     * @throws NoSuchFieldException if the field does not exist
     * @throws IllegalAccessException field is not accessible to be changed
     */
    void setSettings(Map<String, Object> settings) throws NoSuchFieldException, IllegalAccessException;

    /**
     * Set settings of the persistence service
     *
     * @param fieldName name of the field to set
     * @param value value of the field to set
     * @throws NoSuchFieldException if the field does not exist
     * @throws IllegalAccessException field is not accessible to be changed
     */
    void setSetting(String fieldName, Object value) throws NoSuchFieldException, IllegalAccessException;

    /**
     * Get settings of the persistence service
     *
     * @param fieldName name of the field to get
     * @return an object corresponding to the field that was accessed
     * @throws NoSuchFieldException if the field does not exist
     * @throws IllegalAccessException field is not accessible to be changed
     */
    Object getSetting(String fieldName) throws NoSuchFieldException, IllegalAccessException;

    /**
     * Return true if the item which is saved in the persistence service is consistent
     *
     * @param item the item to the check if consistent
     * @return {@code true} if the item is consistent, false otherwise
     */
    boolean isConsistent(Item item);

    /**
     * Persists the specified Item in the context server.
     *
     * @param item the item to persist
     * @return {@code true} if the item was properly persisted, {@code false} otherwise
     */
    boolean save(Item item);

    /**
     * Persists the specified Item in the context server.
     *
     * @param item the item to persist
     * @param useBatching whether to use batching or not for saving the item. If activating there may be a delay between
     *                 the call to this method and the actual saving in the persistence backend.
     *
     * @return {@code true} if the item was properly persisted, {@code false} otherwise
     */
    boolean save(Item item, boolean useBatching);

    /**
     * Persists the specified Item in the context server.
     *
     * @param item the item to persist
     * @param useBatching whether to use batching or not for saving the item. If activating there may be a delay between
     *                 the call to this method and the actual saving in the persistence backend
     * @param alwaysOverwrite whether to overwrite a document even if we are holding an old item when saving
     *
     * @return {@code true} if the item was properly persisted, {@code false} otherwise
     */
    boolean save(Item item, Boolean useBatching, Boolean alwaysOverwrite);

    /**
     * Updates the item of the specified class and identified by the specified identifier with new property values provided as name - value pairs in the specified Map.
     *
     * @param item     the item we want to update
     * @param dateHint a Date helping in identifying where the item is located
     * @param clazz    the Item subclass of the item to update
     * @param source   a Map with entries specifying as key the property name to update and as value its new value
     * @return {@code true} if the update was successful, {@code false} otherwise
     */
    boolean update(Item item, Date dateHint, Class<?> clazz, Map<?, ?> source);

    /**
     * Updates the item of the specified class and identified by the specified identifier with a new property value for the specified property name. Same as
     * {@code update(itemId, dateHint, clazz, Collections.singletonMap(propertyName, propertyValue))}
     *
     * @param item          the item we want to update
     * @param dateHint      a Date helping in identifying where the item is located
     * @param clazz         the Item subclass of the item to update
     * @param propertyName  the name of the property to update
     * @param propertyValue the new value of the property
     * @return {@code true} if the update was successful, {@code false} otherwise
     */
    boolean update(Item item, Date dateHint, Class<?> clazz, String propertyName, Object propertyValue);

    /**
     * Updates the item of the specified class and identified by the specified identifier with new property values provided as name - value pairs in the specified Map.
     *
     * @param item     the item we want to update
     * @param dateHint a Date helping in identifying where the item is located
     * @param clazz    the Item subclass of the item to update
     * @param source   a Map with entries specifying as key the property name to update and as value its new value
     * @param alwaysOverwrite whether to overwrite a document even if we are holding an old item when saving
     * @return {@code true} if the update was successful, {@code false} otherwise
     */
    boolean update(Item item, Date dateHint, Class<?> clazz, Map<?, ?> source, final boolean alwaysOverwrite);

    /**
     * Updates Map of items of the specified class and identified by the specified identifier with a new property value for the specified property name. Same as
     * {@code update(itemId, dateHint, clazz, Collections.singletonMap(propertyName, propertyValue))}
     *
     * @param items         A map the consist of item (key) and properties to update (value)
     * @param dateHint      a Date helping in identifying where the item is located
     * @param clazz         the Item subclass of the item to update
     * @return List of failed Items Ids, if all succesful then returns an empty list. if the whole operation failed then will return null
     */
    List<String> update(Map<Item, Map> items, Date dateHint, Class clazz);

    /**
     * Updates the item of the specified class and identified by the specified identifier with a new property value for the specified property name. Same as
     * {@code update(itemId, dateHint, clazz, Collections.singletonMap(propertyName, propertyValue))}
     *
     * @param item          the item we want to update
     * @param dateHint      a Date helping in identifying where the item is located
     * @param clazz         the Item subclass of the item to update
     * @param script        inline script
     * @param scriptParams  script params
     * @return {@code true} if the update was successful, {@code false} otherwise
     */
    boolean updateWithScript(Item item, Date dateHint, Class<?> clazz, String script, Map<String, Object> scriptParams);

    /**
     * Updates the items of the specified class by a query with a new property value for the specified property name
     * based on provided scripts and script parameters
     *
     * @param dateHint      a Date helping in identifying where the item is located
     * @param clazz         the Item subclass of the item to update
     * @param scripts       inline scripts array
     * @param scriptParams  script params array
     * @param conditions    conditions array
     * @return {@code true} if the update was successful, {@code false} otherwise
     */
    boolean updateWithQueryAndScript(Date dateHint, Class<?> clazz, String[] scripts, Map<String, Object>[] scriptParams, Condition[] conditions);

    /**
     * Retrieves the item identified with the specified identifier and with the specified Item subclass if it exists.
     *
     * @param <T>    the type of the Item subclass we want to retrieve
     * @param itemId the identifier of the item we want to retrieve
     * @param clazz  the {@link Item} subclass of the item we want to retrieve
     * @return the item identified with the specified identifier and with the specified Item subclass if it exists, {@code null} otherwise
     */
    <T extends Item> T load(String itemId, Class<T> clazz);

    /**
     * Retrieves the item identified with the specified identifier and with the specified Item subclass if it exists.
     *
     * @param <T>      the type of the Item subclass we want to retrieve
     * @param itemId   the identifier of the item we want to retrieve
     * @param dateHint a Date helping in identifying where the item is located
     * @param clazz    the {@link Item} subclass of the item we want to retrieve
     * @return the item identified with the specified identifier and with the specified Item subclass if it exists, {@code null} otherwise
     */
    <T extends Item> T load(String itemId, Date dateHint, Class<T> clazz);

    /**
     *
     * @param dateHint a Date helping in identifying where the item is located
     * @param clazz    the {@link Item} subclass of the item we want to retrieve
     * @param itemId   the identifier of the item we want to retrieve
     * @param <T>      the type of the Item subclass we want to retrieve
     * @return A list of Items identified with the specified identifiers and with the specified Item subclass if exist, empty {@link List} otherwise
     */
    <T extends Item> List<T> load(Date dateHint, Class<T> clazz, String... itemId);


    /**
     * Deletes the item identified with the specified identifier and with the specified Item subclass if it exists.
     *
     * @param <T>    the type of the Item subclass we want to delete
     * @param itemId the identifier of the item we want to delete
     * @param clazz  the {@link Item} subclass of the item we want to delete
     * @return {@code true} if the deletion was successful, {@code false} otherwise
     */
    <T extends Item> boolean remove(String itemId, Class<T> clazz);

    /**
     * Deletes items with the specified Item subclass matching the specified {@link Condition}.
     *
     * @param <T>   the type of the Item subclass we want to delete
     * @param query a {@link Condition} identifying which elements we want to delete
     * @param clazz the {@link Item} subclass of the items we want to delete
     * @return {@code true} if the deletion was successful, {@code false} otherwise
     */
    <T extends Item> boolean removeByQuery(Condition query, Class<T> clazz);

    /**
     * Persists the specified query under the specified name.
     *
     * @param queryName the name under which the specified query should be recorded
     * @param query     the query to be recorded
     * @return {@code true} if the query was properly saved, {@code false} otherwise
     */
    boolean saveQuery(String queryName, Condition query);

    /**
     * Deletes the query identified by the specified name.
     *
     * @param queryName the name under which the specified query was recorded
     * @return {@code true} if the deletion was successful, {@code false} otherwise
     */
    boolean removeQuery(String queryName);

    /**
     * Retrieve the type mappings for a given itemType. This method queries the persistence service implementation
     * to retrieve any type mappings it may have for the specified itemType.
     *
     * This method may not return any results if the implementation doesn't support property type mappings
     *
     * @param itemType the itemType we want to retrieve the mappings for
     * @return properties mapping
     */
    Map<String, Map<String, Object>> getPropertiesMapping(String itemType);

    /**
     * Retrieve the mapping for one specific property for a given type.
     *
     * @param property the property name (can use nested dot notation)
     * @param itemType the itemType we want to retrieve the mappings for
     * @return property mapping
     */
    Map<String, Object> getPropertyMapping(String property, String itemType);

    /**
     * Create the persistence mapping for specific property for a given type.
     *
     * @param property the PropertyType to create mapping for
     * @param itemType the itemType we want to retrieve the mappings for
     */
    void setPropertyMapping(PropertyType property, String itemType);

    /**
     * Create mapping
     * @param type the type
     * @param source the source
     */
    void createMapping(String type, String source);

    /**
     * Checks whether the specified item satisfies the provided condition.
     *
     * TODO: rename to isMatching?
     *
     * @param query the condition we're testing the specified item against
     * @param item  the item we're checking against the specified condition
     * @return {@code true} if the item satisfies the condition, {@code false} otherwise
     */
    boolean testMatch(Condition query, Item item);

    /**
     * validates if a condition throws exception at query build.
     *
     * @param condition the condition we're testing the specified item against
     * @param item  the item we're checking against the specified condition
     * @return {@code true} if the item satisfies the condition, {@code false} otherwise
     */
    boolean isValidCondition(Condition condition, Item item);
    /**
     * Same as {@code query(fieldName, fieldValue, sortBy, clazz, 0, -1).getList()}
     *
     * @see #query(Condition, String, Class, int, int)
     * @param <T>         the type of the Item subclass we want to retrieve
     * @param fieldName   the name of the field which we want items to have the specified values
     * @param fieldValue  the value the items to retrieve should have for the specified field
     * @param sortBy      an optional ({@code null} if no sorting is required) String of comma ({@code ,}) separated property names on which ordering should be performed, ordering
     *                    elements according to the property order in the
     *                    String, considering each in turn and moving on to the next one in case of equality of all preceding ones. Each property name is optionally followed by
     *                    a column ({@code :}) and an order specifier: {@code asc} or {@code desc}.
     * @param clazz       the {@link Item} subclass of the items we want to retrieve
     * @return a list of items matching the specified criteria
     */
    <T extends Item> List<T> query(String fieldName, String fieldValue, String sortBy, Class<T> clazz);

    /**
     * Retrieves a list of items with the specified field having the specified values.
     *
     * @param <T>         the type of the Item subclass we want to retrieve
     * @param fieldName   the name of the field which we want items to have the specified values
     * @param fieldValues the values the items to retrieve should have for the specified field
     * @param sortBy      an optional ({@code null} if no sorting is required) String of comma ({@code ,}) separated property names on which ordering should be performed, ordering
     *                    elements according to the property order in the
     *                    String, considering each in turn and moving on to the next one in case of equality of all preceding ones. Each property name is optionally followed by
     *                    a column ({@code :}) and an order specifier: {@code asc} or {@code desc}.
     * @param clazz       the {@link Item} subclass of the items we want to retrieve
     * @return a list of items matching the specified criteria
     */
    <T extends Item> List<T> query(String fieldName, String[] fieldValues, String sortBy, Class<T> clazz);

    /**
     * Retrieves a list of items with the specified field having the specified value.
     *
     * @param <T>        the type of the Item subclass we want to retrieve
     * @param fieldName  the name of the field which we want items to have the specified value
     * @param fieldValue the value the items to retrieve should have for the specified field
     * @param sortBy     an optional ({@code null} if no sorting is required) String of comma ({@code ,}) separated property names on which ordering should be performed, ordering
     *                   elements according to the property order in the
     *                   String, considering each in turn and moving on to the next one in case of equality of all preceding ones. Each property name is optionally followed by
     *                   a column ({@code :}) and an order specifier: {@code asc} or {@code desc}.
     * @param clazz      the {@link Item} subclass of the items we want to retrieve
     * @param offset     zero or a positive integer specifying the position of the first item in the total ordered collection of matching items
     * @param size       a positive integer specifying how many matching items should be retrieved or {@code -1} if all of them should be retrieved
     * @return a {@link PartialList} of items matching the specified criteria
     */
    <T extends Item> PartialList<T> query(String fieldName, String fieldValue, String sortBy, Class<T> clazz, int offset, int size);

    /**
     * Retrieves a list of items with the specified field having the specified value and having at least a field with the specified full text value in it, ordered according to the
     * specified {@code sortBy} String and and paged: only {@code size} of them are retrieved, starting with the {@code offset}-th one.
     *
     * @param <T>        the type of the Item subclass we want to retrieve
     * @param fieldName  the name of the field which we want items to have the specified value
     * @param fieldValue the value the items to retrieve should have for the specified field
     * @param fulltext   the text that the item must have in one of its fields to be considered a match
     * @param sortBy     an optional ({@code null} if no sorting is required) String of comma ({@code ,}) separated property names on which ordering should be performed, ordering
     *                   elements according to the property order in the
     *                   String, considering each in turn and moving on to the next one in case of equality of all preceding ones. Each property name is optionally followed by
     *                   a column ({@code :}) and an order specifier: {@code asc} or {@code desc}.
     * @param clazz      the {@link Item} subclass of the items we want to retrieve
     * @param offset     zero or a positive integer specifying the position of the first item in the total ordered collection of matching items
     * @param size       a positive integer specifying how many matching items should be retrieved or {@code -1} if all of them should be retrieved
     * @return a {@link PartialList} of items matching the specified criteria
     */
    <T extends Item> PartialList<T> queryFullText(String fieldName, String fieldValue, String fulltext, String sortBy, Class<T> clazz, int offset, int size);

    /**
     * Retrieves a list of items having at least a field with the specified full text value in it, ordered according to the specified {@code sortBy} String and and paged: only
     * {@code size} of them are retrieved, starting with the {@code offset}-th one.
     *
     * @param <T>      the type of the Item subclass we want to retrieve
     * @param fulltext the text that the item must have in one of its fields to be considered a match
     * @param sortBy   an optional ({@code null} if no sorting is required) String of comma ({@code ,}) separated property names on which ordering should be performed, ordering
     *                 elements according to the property order in the
     *                 String, considering each in turn and moving on to the next one in case of equality of all preceding ones. Each property name is optionally followed by
     *                 a column ({@code :}) and an order specifier: {@code asc} or {@code desc}.
     * @param clazz    the {@link Item} subclass of the items we want to retrieve
     * @param offset   zero or a positive integer specifying the position of the first item in the total ordered collection of matching items
     * @param size     a positive integer specifying how many matching items should be retrieved or {@code -1} if all of them should be retrieved
     * @return a {@link PartialList} of items matching the specified criteria
     */
    <T extends Item> PartialList<T> queryFullText(String fulltext, String sortBy, Class<T> clazz, int offset, int size);

    /**
     * Same as {@code query(query, sortBy, clazz, 0, -1).getList()}
     *
     * @param <T>    the type of the Item subclass we want to retrieve
     * @param query  the {@link Condition} the items must satisfy to be retrieved
     * @param sortBy an optional ({@code null} if no sorting is required) String of comma ({@code ,}) separated property names on which ordering should be performed, ordering
     *               elements according to the property order in the
     *               String, considering each in turn and moving on to the next one in case of equality of all preceding ones. Each property name is optionally followed by
     *               a column ({@code :}) and an order specifier: {@code asc} or {@code desc}.
     * @param clazz  the {@link Item} subclass of the items we want to retrieve
     * @return a {@link PartialList} of items matching the specified criteria
     * @see #query(Condition, String, Class, int, int)
     */
    <T extends Item> List<T> query(Condition query, String sortBy, Class<T> clazz);

    /**
     * Retrieves a list of items satisfying the specified {@link Condition}, ordered according to the specified {@code sortBy} String and and paged: only {@code size} of them
     * are retrieved, starting with the {@code offset}-th one.
     *
     * @param <T>    the type of the Item subclass we want to retrieve
     * @param query  the {@link Condition} the items must satisfy to be retrieved
     * @param sortBy an optional ({@code null} if no sorting is required) String of comma ({@code ,}) separated property names on which ordering should be performed, ordering
     *               elements according to the property order in the
     *               String, considering each in turn and moving on to the next one in case of equality of all preceding ones. Each property name is optionally followed by
     *               a column ({@code :}) and an order specifier: {@code asc} or {@code desc}.
     * @param clazz  the {@link Item} subclass of the items we want to retrieve
     * @param offset zero or a positive integer specifying the position of the first item in the total ordered collection of matching items
     * @param size   a positive integer specifying how many matching items should be retrieved or {@code -1} if all of them should be retrieved
     * @return a {@link PartialList} of items matching the specified criteria
     */
    <T extends Item> PartialList<T> query(Condition query, String sortBy, Class<T> clazz, int offset, int size);

    /**
     * Retrieves a list of items satisfying the specified {@link Condition}, ordered according to the specified {@code sortBy} String and and paged: only {@code size} of them
     * are retrieved, starting with the {@code offset}-th one. If a scroll identifier and time validity are specified, they will be used to perform a scrolling query, meaning
     * that only partial results will be returned, but the scrolling can be continued.
     *
     * @param <T>    the type of the Item subclass we want to retrieve
     * @param query  the {@link Condition} the items must satisfy to be retrieved
     * @param sortBy an optional ({@code null} if no sorting is required) String of comma ({@code ,}) separated property names on which ordering should be performed, ordering
     *               elements according to the property order in the
     *               String, considering each in turn and moving on to the next one in case of equality of all preceding ones. Each property name is optionally followed by
     *               a column ({@code :}) and an order specifier: {@code asc} or {@code desc}.
     * @param clazz  the {@link Item} subclass of the items we want to retrieve
     * @param offset zero or a positive integer specifying the position of the first item in the total ordered collection of matching items
     * @param size   a positive integer specifying how many matching items should be retrieved or {@code -1} if all of them should be retrieved. In the case of a scroll query
     *               this will be used as the scrolling window size.
     * @param scrollTimeValidity the time the scrolling query should stay valid. This must contain a time unit value such as the ones supported by ElasticSearch, such as
     *                           the ones declared here : https://www.elastic.co/guide/en/elasticsearch/reference/current/common-options.html#time-units
     * @return a {@link PartialList} of items matching the specified criteria, with an scroll identifier and the scroll validity used if a scroll query was requested.
     */
    <T extends Item> PartialList<T> query(Condition query, String sortBy, Class<T> clazz, int offset, int size, String scrollTimeValidity);

    /**
     * Continues the execution of a scroll query, to retrieve the next results. If there are no more results the scroll query is also cleared.
     * @param clazz  the {@link Item} subclass of the items we want to retrieve
     * @param scrollIdentifier a scroll identifier obtained by the execution of a first query and returned in the {@link PartialList} object
     * @param scrollTimeValidity a scroll time validity value for the scroll query to stay valid. This must contain a time unit value such as the ones supported by ElasticSearch, such as
     *                           the ones declared here : https://www.elastic.co/guide/en/elasticsearch/reference/current/common-options.html#time-units
     * @param <T>    the type of the Item subclass we want to retrieve
     * @return a {@link PartialList} of items matching the specified criteria, with an scroll identifier and the scroll validity used if a scroll query was requested. Note that if
     * there are no more results the list will be empty but not null.
     */
    <T extends Item> PartialList<T> continueScrollQuery(Class<T> clazz, String scrollIdentifier, String scrollTimeValidity);

    /**
     * Retrieves the same items as {@code query(query, sortBy, clazz, 0, -1)} with the added constraints that the matching elements must also have at least a field matching the
     * specified full text query.
     *
     * @param <T>      the type of the Item subclass we want to retrieve
     * @param fulltext the text that the item must have in one of its fields to be considered a match
     * @param query    the {@link Condition} the items must satisfy to be retrieved
     * @param sortBy   an optional ({@code null} if no sorting is required) String of comma ({@code ,}) separated property names on which ordering should be performed, ordering
     *                 elements according to the property order in the
     *                 String, considering each in turn and moving on to the next one in case of equality of all preceding ones. Each property name is optionally followed by
     *                 a column ({@code :}) and an order specifier: {@code asc} or {@code desc}.
     * @param clazz    the {@link Item} subclass of the items we want to retrieve
     * @param offset   zero or a positive integer specifying the position of the first item in the total ordered collection of matching items
     * @param size     a positive integer specifying how many matching items should be retrieved or {@code -1} if all of them should be retrieved
     * @return a {@link PartialList} of items matching the specified criteria
     */
    <T extends Item> PartialList<T> queryFullText(String fulltext, Condition query, String sortBy, Class<T> clazz, int offset, int size);

    /**
     * Retrieves the number of items of the specified type as defined by the Item subclass public field {@code ITEM_TYPE} and matching the specified {@link Condition}.
     *
     * @param query    the condition the items must satisfy
     * @param itemType the String representation of the item type we want to retrieve the count of, as defined by its class' {@code ITEM_TYPE} field
     * @return the number of items of the specified type
     * @see Item Item for a discussion of {@code ITEM_TYPE}
     */
    long queryCount(Condition query, String itemType);

    /**
     * Retrieves the number of items with the specified type as defined by the Item subclass public field {@code ITEM_TYPE}.
     *
     * @param itemType the String representation of the item type we want to retrieve the count of, as defined by its class' {@code ITEM_TYPE} field
     * @return the number of items of the specified type
     * @see Item Item for a discussion of {@code ITEM_TYPE}
     */
    long getAllItemsCount(String itemType);

    /**
     * Retrieves the number of items with the specified type as defined by the Item subclass public field {@code ITEM_TYPE} matching the optional specified condition and
     * aggregated according to the specified {@link BaseAggregate}.
     * Also return the global count of document matching the {@code ITEM_TYPE}
     *
     * @param filter    the condition the items must match or {@code null} if no filtering is needed
     * @param aggregate an aggregate specifying how matching items must be bundled
     * @param itemType  the String representation of the item type we want to retrieve the count of, as defined by its class' {@code ITEM_TYPE} field
     * @return a Map associating aggregation dimension name as key and cardinality for that dimension as value
     * @deprecated As of 1.3.0-incubating, please use {@link #aggregateWithOptimizedQuery(Condition, BaseAggregate, String)} instead
     */
    @Deprecated
    Map<String, Long> aggregateQuery(Condition filter, BaseAggregate aggregate, String itemType);

    /**
     * Retrieves the number of items with the specified type as defined by the Item subclass public field {@code ITEM_TYPE} matching the optional specified condition and
     * aggregated according to the specified {@link BaseAggregate}.
     * This aggregate won't return the global count and should therefore be much faster than {@link #aggregateQuery(Condition, BaseAggregate, String)}
     *
     * @param filter    the condition the items must match or {@code null} if no filtering is needed
     * @param aggregate an aggregate specifying how matching items must be bundled
     * @param itemType  the String representation of the item type we want to retrieve the count of, as defined by its class' {@code ITEM_TYPE} field
     * @return a Map associating aggregation dimension name as key and cardinality for that dimension as value
     */
    Map<String, Long> aggregateWithOptimizedQuery(Condition filter, BaseAggregate aggregate, String itemType);

    /**
     * Retrieves the number of items with the specified type as defined by the Item subclass public field {@code ITEM_TYPE} matching the optional specified condition and
     * aggregated according to the specified {@link BaseAggregate}.
     *
     * @param filter    the condition the items must match or {@code null} if no filtering is needed
     * @param aggregate an aggregate specifying how matching items must be bundled
     * @param itemType  the String representation of the item type we want to retrieve the count of, as defined by its class' {@code ITEM_TYPE} field
     * @param size      size of returned buckets in the response
     * @return a Map associating aggregation dimension name as key and cardinality for that dimension as value
     */
    Map<String, Long> aggregateWithOptimizedQuery(Condition filter, BaseAggregate aggregate, String itemType, int size);

    /**
     * Updates the persistence's engine indices if needed.
     */
    void refresh();

    /**
     * Updates the persistence's engine specific index.
     *
     * @param clazz will use an index by class type
     * @param dateHint for index with time, can be null
     * @param <T> a class that extends Item
     */
    <T extends Item> void refreshIndex(Class<T> clazz, Date dateHint);

    /**
     * Purges all data in the context server up to the specified date, not included.
     *
     * @param date the date (not included) before which we want to erase all data
     */
    void purge(Date date);

    /**
     * Retrieves all items of the specified Item subclass which specified ranged property is within the specified bounds, ordered according to the specified {@code sortBy} String
     * and and paged: only {@code size} of them are retrieved, starting with the {@code offset}-th one.
     *
     * @param <T>    the type of the Item subclass we want to retrieve
     * @param s      the name of the range property we want items to retrieve to be included between the specified start and end points
     * @param from   the beginning of the range we want to consider
     * @param to     the end of the range we want to consider
     * @param sortBy an optional ({@code null} if no sorting is required) String of comma ({@code ,}) separated property names on which ordering should be performed, ordering
     *               elements according to the property order in the String, considering each in turn and moving on to the next one in case of equality of all preceding ones.
     *               Each property name is optionally followed by a column ({@code :}) and an order specifier: {@code asc} or {@code desc}.
     * @param clazz  the {@link Item} subclass of the items we want to retrieve
     * @param offset zero or a positive integer specifying the position of the first item in the total ordered collection of matching items
     * @param size   a positive integer specifying how many matching items should be retrieved or {@code -1} if all of them should be retrieved
     * @return a {@link PartialList} of items matching the specified criteria
     */
    <T extends Item> PartialList<T> rangeQuery(String s, String from, String to, String sortBy, Class<T> clazz, int offset, int size);

    /**
     * Retrieves the specified metrics for the specified field of items of the specified type as defined by the Item subclass public field {@code ITEM_TYPE} and matching the
     * specified {@link Condition}.
     *
     * @param condition the condition the items must satisfy
     * @param metrics   a String array which metrics should be computed (possible values: {@code sum} for the sum of the values,  {@code avg} for the average of the values, {@code
     *                  min} for the minimum value and {@code max} for the maximum value)
     * @param field     the name of the field for which the metrics should be computed
     * @param type      the String representation of the item type we want to retrieve the count of, as defined by its class' {@code ITEM_TYPE} field
     * @return a Map associating computed metric name as key to its associated value
     */
    Map<String, Double> getSingleValuesMetrics(Condition condition, String[] metrics, String field, String type);

    /**
     * Creates an index with for the specified item type in the persistence engine.
     *
     * TODO: remove from API?
     *
     * @param itemType the item type
     * @return {@code true} if the operation was successful, {@code false} otherwise
     */
    boolean createIndex(final String itemType);

    /**
     * Removes the index for the specified item type.
     *
     * TODO: remove from API?
     *
     * @param itemType the item type
     * @return {@code true} if the operation was successful, {@code false} otherwise
     */
    boolean removeIndex(final String itemType);

    /**
     * Removes all data associated with the provided scope.
     *
     * @param scope the scope for which we want to remove data
     */
    void purge(final String scope);

}
