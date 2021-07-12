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

package org.apache.unomi.services.impl.rules;

import org.apache.unomi.api.Event;
import org.apache.unomi.api.Item;
import org.apache.unomi.api.Metadata;
import org.apache.unomi.api.PartialList;
import org.apache.unomi.api.actions.Action;
import org.apache.unomi.api.conditions.Condition;
import org.apache.unomi.api.query.Query;
import org.apache.unomi.api.rules.Rule;
import org.apache.unomi.api.rules.RuleStatistics;
import org.apache.unomi.api.services.*;
import org.apache.unomi.persistence.spi.CustomObjectMapper;
import org.apache.unomi.persistence.spi.PersistenceService;
import org.apache.unomi.services.actions.ActionExecutorDispatcher;
import org.apache.unomi.services.impl.ParserHelper;
import org.osgi.framework.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

public class RulesServiceImpl implements RulesService, EventListenerService, SynchronousBundleListener {

    public static final String RULE_QUERY_PREFIX = "rule_";
    private static final Logger logger = LoggerFactory.getLogger(RulesServiceImpl.class.getName());

    private BundleContext bundleContext;

    private PersistenceService persistenceService;
    private DefinitionsService definitionsService;
    private EventService eventService;
    private SchedulerService schedulerService;

    private ActionExecutorDispatcher actionExecutorDispatcher;
    private List<Rule> allRules;

    private Map<String,RuleStatistics> allRuleStatistics = new ConcurrentHashMap<>();

    private Integer rulesRefreshInterval = 1000;
    private Integer rulesStatisticsRefreshInterval = 10000;

    private List<RuleListenerService> ruleListeners = new CopyOnWriteArrayList<RuleListenerService>();

    private Map<String,Set<Rule>> rulesByEventType = new HashMap<>();
    private Boolean optimizedRulesActivated = true;

    public void setBundleContext(BundleContext bundleContext) {
        this.bundleContext = bundleContext;
    }

    public void setPersistenceService(PersistenceService persistenceService) {
        this.persistenceService = persistenceService;
    }

    public void setDefinitionsService(DefinitionsService definitionsService) {
        this.definitionsService = definitionsService;
    }

    public void setEventService(EventService eventService) {
        this.eventService = eventService;
    }

    public void setSchedulerService(SchedulerService schedulerService) {
        this.schedulerService = schedulerService;
    }

    public void setActionExecutorDispatcher(ActionExecutorDispatcher actionExecutorDispatcher) {
        this.actionExecutorDispatcher = actionExecutorDispatcher;
    }

    public void setRulesRefreshInterval(Integer rulesRefreshInterval) {
        this.rulesRefreshInterval = rulesRefreshInterval;
    }

    public void setRulesStatisticsRefreshInterval(Integer rulesStatisticsRefreshInterval) {
        this.rulesStatisticsRefreshInterval = rulesStatisticsRefreshInterval;
    }

    public void setOptimizedRulesActivated(Boolean optimizedRulesActivated) {
        this.optimizedRulesActivated = optimizedRulesActivated;
    }

    public void postConstruct() {
        logger.debug("postConstruct {" + bundleContext.getBundle() + "}");

        loadPredefinedRules(bundleContext);
        for (Bundle bundle : bundleContext.getBundles()) {
            if (bundle.getBundleContext() != null && bundle.getBundleId() != bundleContext.getBundle().getBundleId()) {
                loadPredefinedRules(bundle.getBundleContext());
            }
        }

        bundleContext.addBundleListener(this);

        initializeTimers();
        logger.info("Rule service initialized.");
    }

    public void preDestroy() {
        bundleContext.removeBundleListener(this);
        logger.info("Rule service shutdown.");
    }

    private void processBundleStartup(BundleContext bundleContext) {
        if (bundleContext == null) {
            return;
        }
        loadPredefinedRules(bundleContext);
    }

    private void processBundleStop(BundleContext bundleContext) {
        if (bundleContext == null) {
            return;
        }
    }

    private void loadPredefinedRules(BundleContext bundleContext) {
        Enumeration<URL> predefinedRuleEntries = bundleContext.getBundle().findEntries("META-INF/cxs/rules", "*.json", true);
        if (predefinedRuleEntries == null) {
            return;
        }

        while (predefinedRuleEntries.hasMoreElements()) {
            URL predefinedRuleURL = predefinedRuleEntries.nextElement();
            logger.debug("Found predefined rule at " + predefinedRuleURL + ", loading... ");

            try {
                Rule rule = CustomObjectMapper.getObjectMapper().readValue(predefinedRuleURL, Rule.class);
                setRule(rule);
                logger.info("Predefined rule with id {} registered", rule.getMetadata().getId());
            } catch (IOException e) {
                logger.error("Error while loading rule definition " + predefinedRuleURL, e);
            }
        }
    }

    public Set<Rule> getMatchingRules(Event event) {
        Set<Rule> matchedRules = new LinkedHashSet<Rule>();

        Boolean hasEventAlreadyBeenRaised = null;
        Boolean hasEventAlreadyBeenRaisedForSession = null;
        Boolean hasEventAlreadyBeenRaisedForProfile = null;

        Set<Rule> eventTypeRules = new HashSet<>(allRules); // local copy to avoid concurrency issues
        if (optimizedRulesActivated) {
            eventTypeRules = rulesByEventType.get(event.getEventType());
            if (eventTypeRules == null) {
                eventTypeRules = new HashSet<>();
            }
            eventTypeRules = new HashSet<>(eventTypeRules); // local copy to avoid concurrency issues
            Set<Rule> allEventRules = rulesByEventType.get("*");
            if (allEventRules != null && !allEventRules.isEmpty()) {
                eventTypeRules.addAll(allEventRules); // retrieve rules that should always be evaluated.
            }
            if (eventTypeRules.isEmpty()) {
                return matchedRules;
            }
        }

        for (Rule rule : eventTypeRules) {
            if (!rule.getMetadata().isEnabled()) {
                continue;
            }
            RuleStatistics ruleStatistics = getLocalRuleStatistics(rule);
            long ruleConditionStartTime = System.currentTimeMillis();
            String scope = rule.getMetadata().getScope();
            if (scope.equals(Metadata.SYSTEM_SCOPE) || scope.equals(event.getSourceId())) {
                Condition eventCondition = definitionsService.extractConditionBySystemTag(rule.getCondition(), "eventCondition");

                if (eventCondition == null) {
                    updateRuleStatistics(ruleStatistics, ruleConditionStartTime);
                    continue;
                }

                fireEvaluate(rule, event);

                if (!persistenceService.testMatch(eventCondition, event)) {
                    updateRuleStatistics(ruleStatistics, ruleConditionStartTime);
                    continue;
                }

                Condition sourceCondition = definitionsService.extractConditionBySystemTag(rule.getCondition(), "sourceEventCondition");
                if (sourceCondition != null && !persistenceService.testMatch(sourceCondition, event.getSource())) {
                    updateRuleStatistics(ruleStatistics, ruleConditionStartTime);
                    continue;
                }
                if (rule.isRaiseEventOnlyOnce()) {
                    hasEventAlreadyBeenRaised = hasEventAlreadyBeenRaised != null ? hasEventAlreadyBeenRaised : eventService.hasEventAlreadyBeenRaised(event);
                    if (hasEventAlreadyBeenRaised) {
                        updateRuleStatistics(ruleStatistics, ruleConditionStartTime);
                        fireAlreadyRaised(RuleListenerService.AlreadyRaisedFor.EVENT, rule, event);
                        continue;
                    }
                } else if (rule.isRaiseEventOnlyOnceForProfile()) {
                    hasEventAlreadyBeenRaisedForProfile = hasEventAlreadyBeenRaisedForProfile != null ? hasEventAlreadyBeenRaisedForProfile : eventService.hasEventAlreadyBeenRaised(event, false);
                    if (hasEventAlreadyBeenRaisedForProfile) {
                        updateRuleStatistics(ruleStatistics, ruleConditionStartTime);
                        fireAlreadyRaised(RuleListenerService.AlreadyRaisedFor.PROFILE, rule, event);
                        continue;
                    }
                } else if (rule.isRaiseEventOnlyOnceForSession()) {
                    hasEventAlreadyBeenRaisedForSession = hasEventAlreadyBeenRaisedForSession != null ? hasEventAlreadyBeenRaisedForSession : eventService.hasEventAlreadyBeenRaised(event, true);
                    if (hasEventAlreadyBeenRaisedForSession) {
                        updateRuleStatistics(ruleStatistics, ruleConditionStartTime);
                        fireAlreadyRaised(RuleListenerService.AlreadyRaisedFor.SESSION, rule, event);
                        continue;
                    }
                }

                Condition profileCondition = definitionsService.extractConditionBySystemTag(rule.getCondition(), "profileCondition");
                if (profileCondition != null && !persistenceService.testMatch(profileCondition, event.getProfile())) {
                    updateRuleStatistics(ruleStatistics, ruleConditionStartTime);
                    continue;
                }
                Condition sessionCondition = definitionsService.extractConditionBySystemTag(rule.getCondition(), "sessionCondition");
                if (sessionCondition != null && !persistenceService.testMatch(sessionCondition, event.getSession())) {
                    updateRuleStatistics(ruleStatistics, ruleConditionStartTime);
                    continue;
                }
                matchedRules.add(rule);
            }
        }

        return matchedRules;
    }

    private RuleStatistics getLocalRuleStatistics(Rule rule) {
        RuleStatistics ruleStatistics = this.allRuleStatistics.get(rule.getItemId());
        if (ruleStatistics == null) {
            ruleStatistics = new RuleStatistics(rule.getItemId());
        }
        return ruleStatistics;
    }

    private void updateRuleStatistics(RuleStatistics ruleStatistics, long ruleConditionStartTime) {
        long totalRuleConditionTime = System.currentTimeMillis() - ruleConditionStartTime;
        ruleStatistics.setLocalConditionsTime(ruleStatistics.getLocalConditionsTime() + totalRuleConditionTime);
        allRuleStatistics.put(ruleStatistics.getItemId(), ruleStatistics);
    }

    public void refreshRules() {
        try {
            allRules = getAllRules();
        } catch (Throwable t) {
            logger.error("Error loading rules from persistence back-end", t);
        }
    }

    private List<Rule> getAllRules() {
        List<Rule> allItems = persistenceService.getAllItems(Rule.class, 0, -1, "priority").getList();
        Map<String,Set<Rule>> newRulesByEventType = new HashMap<>();
        for (Rule rule : allItems) {
            ParserHelper.resolveConditionType(definitionsService, rule.getCondition(), "rule " + rule.getItemId());
            updateRulesByEventType(newRulesByEventType, rule);
            ParserHelper.resolveActionTypes(definitionsService, rule);
        }
        this.rulesByEventType = newRulesByEventType;
        return allItems;
    }


    public boolean canHandle(Event event) {
        return true;
    }

    public int onEvent(Event event) {
        Set<Rule> rules = getMatchingRules(event);

        int changes = EventService.NO_CHANGE;
        for (Rule rule : rules) {
            logger.debug("Fired rule " + rule.getMetadata().getId() + " for " + event.getEventType() + " - " + event.getItemId());
            fireExecuteActions(rule, event);

            long actionsStartTime = System.currentTimeMillis();
            for (Action action : rule.getActions()) {
                changes |= actionExecutorDispatcher.execute(action, event);
            }
            long totalActionsTime = System.currentTimeMillis() - actionsStartTime;
            Event ruleFired = new Event("ruleFired", event.getSession(), event.getProfile(), event.getSourceId(), event, rule, event.getTimeStamp());
            ruleFired.getAttributes().putAll(event.getAttributes());
            ruleFired.setPersistent(false);
            changes |= eventService.send(ruleFired);

            RuleStatistics ruleStatistics = getLocalRuleStatistics(rule);
            ruleStatistics.setLocalExecutionCount(ruleStatistics.getLocalExecutionCount()+1);
            ruleStatistics.setLocalActionsTime(ruleStatistics.getLocalActionsTime() + totalActionsTime);
            this.allRuleStatistics.put(rule.getItemId(), ruleStatistics);
        }
        return changes;
    }

    @Override
    public RuleStatistics getRuleStatistics(String ruleId) {
        if (allRuleStatistics.containsKey(ruleId)) {
            return allRuleStatistics.get(ruleId);
        }
        return persistenceService.load(ruleId, RuleStatistics.class);
    }

    public Map<String,RuleStatistics> getAllRuleStatistics() {
        return allRuleStatistics;
    }

    @Override
    public void resetAllRuleStatistics() {
        Condition matchAllCondition = new Condition(definitionsService.getConditionType("matchAllCondition"));
        persistenceService.removeByQuery(matchAllCondition,RuleStatistics.class);
        allRuleStatistics.clear();
    }

    public Set<Metadata> getRuleMetadatas() {
        Set<Metadata> metadatas = new HashSet<Metadata>();
        for (Rule rule : persistenceService.getAllItems(Rule.class, 0, 50, null).getList()) {
            metadatas.add(rule.getMetadata());
        }
        return metadatas;
    }

    public PartialList<Metadata> getRuleMetadatas(Query query) {
        if(query.isForceRefresh()){
            persistenceService.refresh();
        }
        definitionsService.resolveConditionType(query.getCondition());
        List<Metadata> descriptions = new LinkedList<>();
        PartialList<Rule> rules = persistenceService.query(query.getCondition(), query.getSortby(), Rule.class, query.getOffset(), query.getLimit());
        for (Rule definition : rules.getList()) {
            descriptions.add(definition.getMetadata());
        }
        return new PartialList<>(descriptions, rules.getOffset(), rules.getPageSize(), rules.getTotalSize(), rules.getTotalSizeRelation());
    }

    public PartialList<Rule> getRuleDetails(Query query) {
        if (query.isForceRefresh()) {
            persistenceService.refresh();
        }
        definitionsService.resolveConditionType(query.getCondition());
        PartialList<Rule> rules = persistenceService.query(query.getCondition(), query.getSortby(), Rule.class, query.getOffset(), query.getLimit());
        List<Rule> details = new LinkedList<>();
        details.addAll(rules.getList());
        return new PartialList<>(details, rules.getOffset(), rules.getPageSize(), rules.getTotalSize(), rules.getTotalSizeRelation());
    }

    public Rule getRule(String ruleId) {
        Rule rule = persistenceService.load(ruleId, Rule.class);
        if (rule != null) {
            ParserHelper.resolveConditionType(definitionsService, rule.getCondition(), "rule " + rule.getItemId());
            ParserHelper.resolveActionTypes(definitionsService, rule);
        }
        return rule;
    }

    public void setRule(Rule rule) {
        if (rule.getMetadata().getScope() == null) {
            rule.getMetadata().setScope("systemscope");
        }
        Condition condition = rule.getCondition();
        if (condition != null) {
            if (rule.getMetadata().isEnabled() && !rule.getMetadata().isMissingPlugins()) {
                ParserHelper.resolveConditionType(definitionsService, condition, "rule " + rule.getItemId());
                definitionsService.extractConditionBySystemTag(condition, "eventCondition");
            }
        }
        persistenceService.save(rule);
    }

    public Set<Condition> getTrackedConditions(Item source){
        Set<Condition> trackedConditions = new HashSet<>();
        for (Rule r : allRules) {
            if (!r.getMetadata().isEnabled()) {
                continue;
            }
            Condition trackedCondition = definitionsService.extractConditionBySystemTag(r.getCondition(), "trackedCondition");
            if(trackedCondition != null){
                Condition sourceEventPropertyCondition = definitionsService.extractConditionBySystemTag(r.getCondition(), "sourceEventCondition");
                if(source != null && sourceEventPropertyCondition != null) {
                    ParserHelper.resolveConditionType(definitionsService, sourceEventPropertyCondition, "rule " + r.getItemId() + " source event condition");
                    if(persistenceService.testMatch(sourceEventPropertyCondition, source)){
                        trackedConditions.add(trackedCondition);
                    }
                } else {
                    trackedConditions.add(trackedCondition);
                }

            }
        }
        return trackedConditions;
    }

    public void removeRule(String ruleId) {
        persistenceService.remove(ruleId, Rule.class);
    }

    private void initializeTimers() {
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                refreshRules();
            }
        };
        schedulerService.getScheduleExecutorService().scheduleWithFixedDelay(task, 0,rulesRefreshInterval, TimeUnit.MILLISECONDS);

        TimerTask statisticsTask = new TimerTask() {
            @Override
            public void run() {
                try {
                    syncRuleStatistics();
                } catch (Throwable t) {
                    logger.error("Error synching rule statistics between memory and persistence back-end", t);
                }
            }
        };
        schedulerService.getScheduleExecutorService().scheduleWithFixedDelay(statisticsTask, 0, rulesStatisticsRefreshInterval, TimeUnit.MILLISECONDS);
    }

    public void bundleChanged(BundleEvent event) {
        switch (event.getType()) {
            case BundleEvent.STARTED:
                processBundleStartup(event.getBundle().getBundleContext());
                break;
            case BundleEvent.STOPPING:
                processBundleStop(event.getBundle().getBundleContext());
                break;
        }
    }

    private void syncRuleStatistics() {
        List<RuleStatistics> allPersistedRuleStatisticsList = persistenceService.getAllItems(RuleStatistics.class);
        Map<String,RuleStatistics> allPersistedRuleStatistics = new HashMap<>();
        for (RuleStatistics ruleStatistics : allPersistedRuleStatisticsList) {
            allPersistedRuleStatistics.put(ruleStatistics.getItemId(), ruleStatistics);
        }
        // first we iterate over the rules we have in memory
        for (RuleStatistics ruleStatistics : allRuleStatistics.values()) {
            boolean mustPersist = false;
            if (allPersistedRuleStatistics.containsKey(ruleStatistics.getItemId())) {
                // we must sync with the data coming from the persistence service.
                RuleStatistics persistedRuleStatistics = allPersistedRuleStatistics.get(ruleStatistics.getItemId());
                ruleStatistics.setExecutionCount(persistedRuleStatistics.getExecutionCount() + ruleStatistics.getLocalExecutionCount());
                if (ruleStatistics.getLocalExecutionCount() > 0) {
                    ruleStatistics.setLocalExecutionCount(0);
                    mustPersist = true;
                }
                ruleStatistics.setConditionsTime(persistedRuleStatistics.getConditionsTime() + ruleStatistics.getLocalConditionsTime());
                if (ruleStatistics.getLocalConditionsTime() > 0) {
                    ruleStatistics.setLocalConditionsTime(0);
                    mustPersist = true;
                }
                ruleStatistics.setActionsTime(persistedRuleStatistics.getActionsTime() + ruleStatistics.getLocalActionsTime());
                if (ruleStatistics.getLocalActionsTime() > 0) {
                    ruleStatistics.setLocalActionsTime(0);
                    mustPersist = true;
                }
                ruleStatistics.setLastSyncDate(new Date());
            } else {
                ruleStatistics.setExecutionCount(ruleStatistics.getExecutionCount() + ruleStatistics.getLocalExecutionCount());
                if (ruleStatistics.getLocalExecutionCount() > 0) {
                    ruleStatistics.setLocalExecutionCount(0);
                    mustPersist = true;
                }
                ruleStatistics.setConditionsTime(ruleStatistics.getConditionsTime() + ruleStatistics.getLocalConditionsTime());
                if (ruleStatistics.getLocalConditionsTime() > 0) {
                    ruleStatistics.setLocalConditionsTime(0);
                    mustPersist = true;
                }
                ruleStatistics.setActionsTime(ruleStatistics.getActionsTime() + ruleStatistics.getLocalActionsTime());
                if (ruleStatistics.getLocalActionsTime() > 0) {
                    ruleStatistics.setLocalActionsTime(0);
                    mustPersist = true;
                }
                ruleStatistics.setLastSyncDate(new Date());
            }
            allRuleStatistics.put(ruleStatistics.getItemId(), ruleStatistics);
            if (mustPersist) {
                persistenceService.save(ruleStatistics, null, true);
            }
        }
        // now let's iterate over the rules coming from the persistence service, as we may have new ones.
        for (RuleStatistics ruleStatistics : allPersistedRuleStatistics.values()) {
            if (!allRuleStatistics.containsKey(ruleStatistics.getItemId())) {
                allRuleStatistics.put(ruleStatistics.getItemId(), ruleStatistics);
            }
        }
    }

    public void bind(ServiceReference<RuleListenerService> serviceReference) {
        RuleListenerService ruleListenerService = bundleContext.getService(serviceReference);
        ruleListeners.add(ruleListenerService);
    }

    public void unbind(ServiceReference<RuleListenerService> serviceReference) {
        if (serviceReference != null) {
            RuleListenerService ruleListenerService = bundleContext.getService(serviceReference);
            ruleListeners.remove(ruleListenerService);
        }
    }

    public void fireEvaluate(Rule rule, Event event) {
        for (RuleListenerService ruleListenerService : ruleListeners) {
            ruleListenerService.onEvaluate(rule, event);
        }
    }

    public void fireAlreadyRaised(RuleListenerService.AlreadyRaisedFor alreadyRaisedFor, Rule rule, Event event) {
        for (RuleListenerService ruleListenerService : ruleListeners) {
            ruleListenerService.onAlreadyRaised(alreadyRaisedFor, rule, event);
        }
    }

    public void fireExecuteActions(Rule rule, Event event) {
        for (RuleListenerService ruleListenerService : ruleListeners) {
            ruleListenerService.onExecuteActions(rule, event);
        }
    }

    private void updateRulesByEventType(Map<String,Set<Rule>> rulesByEventType, Rule rule) {
        Set<String> eventTypeIds = ParserHelper.resolveConditionEventTypes(rule.getCondition());
        for (String eventTypeId : eventTypeIds) {
            Set<Rule> rules = rulesByEventType.get(eventTypeId);
            if (rules == null) {
                rules = new HashSet<>();
            }
            rules.add(rule);
            rulesByEventType.put(eventTypeId, rules);
        }
    }

}
