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

package org.apache.unomi.plugins.baseplugin.conditions;

import ognl.*;
import ognl.enhance.ExpressionAccessor;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.unomi.api.*;
import org.apache.unomi.api.conditions.Condition;
import org.apache.unomi.plugins.baseplugin.conditions.accessors.HardcodedPropertyAccessor;
import org.apache.unomi.scripting.ExpressionFilterFactory;
import org.apache.unomi.scripting.SecureFilteringClassLoader;
import org.apache.unomi.persistence.elasticsearch.conditions.ConditionContextHelper;
import org.apache.unomi.persistence.elasticsearch.conditions.ConditionEvaluator;
import org.apache.unomi.persistence.elasticsearch.conditions.ConditionEvaluatorDispatcher;
import org.apache.unomi.persistence.spi.PropertyHelper;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.joda.JodaDateMathParser;
import org.elasticsearch.common.unit.DistanceUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Member;
import java.lang.reflect.Modifier;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Evaluator for property comparison conditions
 */
public class PropertyConditionEvaluator implements ConditionEvaluator {

    private static final Logger logger = LoggerFactory.getLogger(PropertyConditionEvaluator.class.getName());

    private static final SimpleDateFormat yearMonthDayDateFormat = new SimpleDateFormat("yyyyMMdd");

    private final Map<String, Map<String, ExpressionAccessor>> expressionCache = new HashMap<>(64);
    private boolean usePropertyConditionOptimizations = true;
    private static final ClassLoader secureFilteringClassLoader = new SecureFilteringClassLoader(PropertyConditionEvaluator.class.getClassLoader());
    private static final HardcodedPropertyAccessorRegistry hardcodedPropertyAccessorRegistry = new HardcodedPropertyAccessorRegistry();
    private ExpressionFilterFactory expressionFilterFactory;

    private final boolean useOGNLScripting = Boolean.parseBoolean(System.getProperty("org.apache.unomi.security.properties.useOGNLScripting", "false"));

    public void setUsePropertyConditionOptimizations(boolean usePropertyConditionOptimizations) {
        this.usePropertyConditionOptimizations = usePropertyConditionOptimizations;
    }

    public void setExpressionFilterFactory(ExpressionFilterFactory expressionFilterFactory) {
        this.expressionFilterFactory = expressionFilterFactory;
    }

    private int compare(Object actualValue, String expectedValue, Object expectedValueDate, Object expectedValueInteger, Object expectedValueDateExpr, Object expectedValueDouble) {
        if (expectedValue == null && expectedValueDate == null && expectedValueInteger == null && getDate(expectedValueDateExpr) == null) {
            return actualValue == null ? 0 : 1;
        } else if (actualValue == null) {
            return -1;
        }

        if (expectedValueInteger != null) {
            return PropertyHelper.getInteger(actualValue).compareTo(PropertyHelper.getInteger(expectedValueInteger));
        } else if (expectedValueDouble != null) {
            return PropertyHelper.getDouble(actualValue).compareTo(PropertyHelper.getDouble(expectedValueDouble));
        } else if (expectedValueDate != null) {
            return getDate(actualValue).compareTo(getDate(expectedValueDate));
        } else if (expectedValueDateExpr != null) {
            return getDate(actualValue).compareTo(getDate(expectedValueDateExpr));
        } else {
            return actualValue.toString().compareTo(expectedValue);
        }
    }

    private boolean compareMultivalue(Object actualValue, List<?> expectedValues, List<?> expectedValuesDate, List<?> expectedValuesNumber, List<?> expectedValuesDateExpr, String op) {
        @SuppressWarnings("unchecked")
        List<?> expected = ObjectUtils.firstNonNull(expectedValues, expectedValuesDate, expectedValuesNumber);
        if (actualValue == null) {
            return expected == null;
        } else if (expected == null) {
            return false;
        }

        List<Object> actual = ConditionContextHelper.foldToASCII(getValueSet(actualValue));

        boolean result = true;

        switch (op) {
            case "in":
                result = false;
                for (Object a : actual) {
                    if (expected.contains(a)) {
                        result = true;
                        break;
                    }
                }
                break;
            case "inContains":
                result = false;
                for (Object a : actual) {
                    for (Object b : expected)
                        if (((String) a).contains((String) b)) {
                            result = true;
                            break;
                        }
                }
                break;
            case "notIn":
                for (Object a : actual) {
                    if (expected.contains(a)) {
                        result = false;
                        break;
                    }
                }
                break;
            case "all":
                for (Object e : expected) {
                    if (!actual.contains(e)) {
                        result = false;
                        break;
                    }
                }
                break;
            case "hasNoneOf":
                if (!Collections.disjoint(actual, expected)) {
                    return false;
                }
                break;
            case "hasSomeOf":
                if (Collections.disjoint(actual, expected)) {
                    return false;
                }
                break;

            default:
                throw new IllegalArgumentException("Unknown comparison operator " + op);
        }

        return result;
    }

    @Override
    public boolean eval(Condition condition, Item item, Map<String, Object> context, ConditionEvaluatorDispatcher dispatcher) {
        String op = (String) condition.getParameter("comparisonOperator");
        String name = (String) condition.getParameter("propertyName");

        String expectedValue = ConditionContextHelper.foldToASCII((String) condition.getParameter("propertyValue"));
        Object expectedValueInteger = condition.getParameter("propertyValueInteger");
        Object expectedValueDouble = condition.getParameter("propertyValueDouble");
        Object expectedValueDate = condition.getParameter("propertyValueDate");
        Object expectedValueDateExpr = condition.getParameter("propertyValueDateExpr");

        Object actualValue;
        if (item instanceof Event && "eventType".equals(name)) {
            actualValue = ((Event) item).getEventType();
        } else {
            try {
                long time = System.nanoTime();
                //actualValue = beanUtilsBean.getPropertyUtils().getProperty(item, name);
                actualValue = getPropertyValue(item, name);
                time = System.nanoTime() - time;
                if (time > 5000000L) {
                    logger.info("eval took {} ms for {} {}", time / 1000000L, item.getClass().getName(), name);
                }
            } catch (NullPointerException e) {
                // property not found
                actualValue = null;
            } catch (Exception e) {
                if (!(e instanceof OgnlException)
                        || (!StringUtils.startsWith(e.getMessage(),
                        "source is null for getProperty(null"))) {
                    logger.warn("Error evaluating value for {} {}. See debug level for more information", item.getClass().getName(), name);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Error evaluating value for {} {}", item.getClass().getName(), name, e);
                    }
                }
                actualValue = null;
            }
        }
        if (actualValue instanceof String) {
            actualValue = ConditionContextHelper.foldToASCII((String) actualValue);
        }

        return isMatch(op, actualValue, expectedValue, expectedValueInteger, expectedValueDouble, expectedValueDate,
                expectedValueDateExpr, condition);
    }

    protected boolean isMatch(String op, Object actualValue, String expectedValue, Object expectedValueInteger, Object expectedValueDouble,
                            Object expectedValueDate, Object expectedValueDateExpr, Condition condition) {
        if (op == null) {
            return false;
        } else if (actualValue == null) {
            return op.equals("missing")|| op.equals("notIn") || op.equals("notEquals");
        } else if (op.equals("exists")) {
            if (actualValue instanceof List) {
                return ((List) actualValue).size() > 0;
            }
            return true;
        } else if (op.equals("equals")) {
            if (actualValue instanceof Collection) {
                for (Object o : ((Collection<?>) actualValue)) {
                    if (o instanceof String) {
                        o = ConditionContextHelper.foldToASCII((String) o);
                    }
                    if (compare(o, expectedValue, expectedValueDate, expectedValueInteger, expectedValueDateExpr, expectedValueDouble) == 0) {
                        return true;
                    }
                }
                return false;
            }
            return compare(actualValue, expectedValue, expectedValueDate, expectedValueInteger, expectedValueDateExpr, expectedValueDouble) == 0;
        } else if (op.equals("notEquals")) {
            return compare(actualValue, expectedValue, expectedValueDate, expectedValueInteger, expectedValueDateExpr, expectedValueDouble) != 0;
        } else if (op.equals("greaterThan")) {
            return compare(actualValue, expectedValue, expectedValueDate, expectedValueInteger, expectedValueDateExpr, expectedValueDouble) > 0;
        } else if (op.equals("greaterThanOrEqualTo")) {
            return compare(actualValue, expectedValue, expectedValueDate, expectedValueInteger, expectedValueDateExpr, expectedValueDouble) >= 0;
        } else if (op.equals("lessThan")) {
            return compare(actualValue, expectedValue, expectedValueDate, expectedValueInteger, expectedValueDateExpr, expectedValueDouble) < 0;
        } else if (op.equals("lessThanOrEqualTo")) {
            return compare(actualValue, expectedValue, expectedValueDate, expectedValueInteger, expectedValueDateExpr, expectedValueDouble) <= 0;
        } else if (op.equals("between")) {
            List<?> expectedValuesInteger = (List<?>) condition.getParameter("propertyValuesInteger");
            List<?> expectedValuesDouble = (List<?>) condition.getParameter("propertyValuesDouble");
            List<?> expectedValuesDate = (List<?>) condition.getParameter("propertyValuesDate");
            List<?> expectedValuesDateExpr = (List<?>) condition.getParameter("propertyValuesDateExpr");
            return compare(actualValue, null,
                    (expectedValuesDate != null && expectedValuesDate.size() >= 1) ? getDate(expectedValuesDate.get(0)) : null,
                    (expectedValuesInteger != null && expectedValuesInteger.size() >= 1) ? (Integer) expectedValuesInteger.get(0) : null,
                    (expectedValuesDateExpr != null && expectedValuesDateExpr.size() >= 1) ? (String) expectedValuesDateExpr.get(0) : null,
                    (expectedValuesDouble != null && expectedValuesDouble.size() >= 1) ? (String) expectedValuesDouble.get(0) : null) >= 0
                    &&
                    compare(actualValue, null,
                            (expectedValuesDate != null && expectedValuesDate.size() >= 2) ? getDate(expectedValuesDate.get(1)) : null,
                            (expectedValuesInteger != null && expectedValuesInteger.size() >= 2) ? (Integer) expectedValuesInteger.get(1) : null,
                            (expectedValuesDateExpr != null && expectedValuesDateExpr.size() >= 2) ? (String) expectedValuesDateExpr.get(1) : null,
                            (expectedValuesDouble != null && expectedValuesDouble.size() >= 2) ? (String) expectedValuesDouble.get(1) : null) <= 0;
        } else if (op.equals("contains")) {
            return actualValue.toString().contains(expectedValue);
        } else if (op.equals("notContains")) {
            return !actualValue.toString().contains(expectedValue);
        } else if (op.equals("startsWith")) {
            return actualValue.toString().startsWith(expectedValue);
        } else if (op.equals("endsWith")) {
            return actualValue.toString().endsWith(expectedValue);
        } else if (op.equals("matchesRegex")) {
            return expectedValue != null && Pattern.compile(expectedValue).matcher(actualValue.toString()).matches();
        } else if (op.equals("in") || op.equals("inContains") || op.equals("notIn") || op.equals("hasSomeOf") || op.equals("hasNoneOf") || op.equals("all")) {
            List<?> expectedValues = ConditionContextHelper.foldToASCII((List<?>) condition.getParameter("propertyValues"));
            List<?> expectedValuesInteger = (List<?>) condition.getParameter("propertyValuesInteger");
            List<?> expectedValuesDate = (List<?>) condition.getParameter("propertyValuesDate");
            List<?> expectedValuesDateExpr = (List<?>) condition.getParameter("propertyValuesDateExpr");

            return compareMultivalue(actualValue, expectedValues, expectedValuesDate, expectedValuesInteger, expectedValuesDateExpr, op);
        } else if (op.equals("isDay") && expectedValueDate != null) {
            return yearMonthDayDateFormat.format(getDate(actualValue)).equals(yearMonthDayDateFormat.format(getDate(expectedValueDate)));
        } else if (op.equals("isNotDay") && expectedValueDate != null) {
            return !yearMonthDayDateFormat.format(getDate(actualValue)).equals(yearMonthDayDateFormat.format(getDate(expectedValueDate)));
        } else if (op.equals("distance")) {
            GeoPoint actualCenter = null;
            if (actualValue instanceof GeoPoint) {
                actualCenter = (GeoPoint) actualValue;
            } else if (actualValue instanceof Map) {
                actualCenter = GeoPoint.fromMap((Map<String, Double>) actualValue);
            } else if (actualValue instanceof String) {
                actualCenter = GeoPoint.fromString((String) actualValue);
            }
            if (actualCenter == null) {
                return false;
            }

            final String unitString = (String) condition.getParameter("unit");
            final String centerString = (String) condition.getParameter("center");
            final Double distance = (Double) condition.getParameter("distance");
            if (centerString == null || distance == null) {
                return false;
            }

            final GeoPoint expectedCenter = GeoPoint.fromString(centerString);
            final DistanceUnit expectedUnit = unitString != null ? DistanceUnit.fromString(unitString) : DistanceUnit.DEFAULT;
            final double distanceInMeters = expectedUnit.convert(distance, DistanceUnit.METERS);

            return expectedCenter.distanceTo(actualCenter) <= distanceInMeters;
        }
        return false;
    }

    protected Object getPropertyValue(Item item, String expression) throws Exception {
        if (usePropertyConditionOptimizations) {
            Object result = getHardcodedPropertyValue(item, expression);
            if (!HardcodedPropertyAccessor.PROPERTY_NOT_FOUND_MARKER.equals(result)) {
                return result;
            }
        }
        if (useOGNLScripting) {
            return getOGNLPropertyValue(item, expression);
        } else {
            logger.warn("OGNL Off. Expression not evaluated on item {}. See debug log level for more information", item.getClass().getName());
            if (logger.isDebugEnabled()) {
                logger.debug("OGNL Off. Expression not evaluated on item {}: {}", item.getClass().getName(), expression);
            }
            return null;
        }
    }

    protected Object getHardcodedPropertyValue(Item item, String expression) {
        // the following are optimizations to avoid using the expressions that are slower. The main objective here is
        // to avoid the most used expression that may also trigger calls to the Java Reflection API.
        return hardcodedPropertyAccessorRegistry.getProperty(item, expression);
    }

    protected Object getOGNLPropertyValue(Item item, String expression) throws Exception {
        if (expressionFilterFactory.getExpressionFilter("ognl").filter(expression) == null) {
            logger.warn("OGNL expression filtered because not allowed on item: {}. See debug log level for more information", item.getClass().getName());
            if (logger.isDebugEnabled()) {
                logger.debug("OGNL expression filtered because not allowed: {}", expression);
            }
            return null;
        }
        OgnlContext ognlContext = getOgnlContext(secureFilteringClassLoader);
        ExpressionAccessor accessor = getPropertyAccessor(item, expression, ognlContext, secureFilteringClassLoader);
        if (accessor != null) {
            try {
                return accessor.get(ognlContext, item);
            } catch (Throwable t) {
                logger.error("Error evaluating expression on item {}. See debug level for more information", item.getClass().getName());
                if (logger.isDebugEnabled()) {
                    logger.debug("Error evaluating expression {} on item {}.", expression, item.getClass().getName(), t);
                }

                return null;
            }
        }
        return null;
    }

    private class ClassLoaderClassResolver extends DefaultClassResolver {
        private ClassLoader classLoader;

        public ClassLoaderClassResolver(ClassLoader classLoader) {
            this.classLoader = classLoader;
        }

        @Override
        protected Class toClassForName(String className) throws ClassNotFoundException {
            return Class.forName(className, true, classLoader);
        }
    }

    private OgnlContext getOgnlContext(ClassLoader classLoader) {
        return (OgnlContext) Ognl.createDefaultContext(null, new MemberAccess() {
                    @Override
                    public Object setup(Map context, Object target, Member member, String propertyName) {
                        return null;
                    }

                    @Override
                    public void restore(Map context, Object target, Member member, String propertyName, Object state) {
                    }

                    @Override
                    public boolean isAccessible(Map context, Object target, Member member, String propertyName) {
                        int modifiers = member.getModifiers();
                        boolean accessible = false;
                        if (target instanceof Item && !"getClass".equals(member.getName())) {
                            accessible = Modifier.isPublic(modifiers);
                        }
                        if (!accessible) {
                            logger.warn("OGNL security filtered target, member for property. See debug log level for more information");
                            if (logger.isDebugEnabled()) {
                                logger.debug("OGNL security filtered: Target {} and member {} for property {}. Not allowed", target, member, propertyName);
                            }
                        }
                        return accessible;
                    }
                }, new ClassLoaderClassResolver(classLoader),
                null);
    }

    private ExpressionAccessor getPropertyAccessor(Item item, String expression, OgnlContext ognlContext, ClassLoader classLoader) throws Exception {
        ExpressionAccessor accessor = null;
        String clazz = item.getClass().getName();
        Map<String, ExpressionAccessor> expressions = expressionCache.get(clazz);
        if (expressions == null) {
            expressions = new HashMap<>();
            expressionCache.put(clazz, expressions);
        } else {
            accessor = expressions.get(expression);
        }
        if (accessor == null) {
            long time = System.nanoTime();
            Thread current = Thread.currentThread();
            ClassLoader contextCL = current.getContextClassLoader();
            try {
                current.setContextClassLoader(classLoader);
                Node node = Ognl.compileExpression(ognlContext, item, expression);
                accessor = node.getAccessor();
            } finally {
                current.setContextClassLoader(contextCL);
            }
            if (accessor != null) {
                expressions.put(expression, accessor);
            } else {
                logger.warn("Unable to compile expression for {}. See debug log level for more information", clazz);
                if (logger.isDebugEnabled()) {
                    logger.debug("Unable to compile expression: {} for: {}", expression, clazz);
                }
            }
            time = System.nanoTime() - time;
            if (logger.isDebugEnabled()) {
                logger.debug("Expression compilation for item={} expression={} took {}", item.getClass().getName(), expression, time / 1000000L);
            } else if (logger.isInfoEnabled()) {
                logger.info("Expression compilation for item={} took {}. See debug log level for more information", item.getClass().getName(), time / 1000000L);
            }
        }

        return accessor;
    }

    private Date getDate(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Date) {
            return ((Date) value);
        } else {
            JodaDateMathParser parser = new JodaDateMathParser(Joda.forPattern("strictDateOptionalTime||epoch_millis"));
            try {
                return Date.from(parser.parse(value.toString(), System::currentTimeMillis));
            } catch (ElasticsearchParseException e) {
                logger.warn("unable to parse date. See debug log level for full stacktrace");
                if (logger.isDebugEnabled()) {
                    logger.debug("unable to parse date {}", value.toString(), e);
                }
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private List<Object> getValueSet(Object expectedValue) {
        if (expectedValue instanceof List) {
            return (List<Object>) expectedValue;
        } else if (expectedValue instanceof Collection) {
            return new ArrayList<Object>((Collection<?>) expectedValue);
        } else {
            return Collections.singletonList(expectedValue);
        }
    }

}
