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
 * limitations under the License
 */

package org.apache.unomi.itests;

import org.apache.unomi.api.conditions.Condition;
import org.apache.unomi.api.services.DefinitionsService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

/**
 * Utility class for building conditions
 *
 * @author Sergiy Shyrkov
 */
public class ConditionBuilder {

    private DefinitionsService definitionsService;

    /**
     * Initializes an instance of this class.
     *
     * @param definitionsService an instance of the {@link DefinitionsService}
     */
    public ConditionBuilder(DefinitionsService definitionsService) {
        super();
        this.definitionsService = definitionsService;
    }

    public CompoundCondition and(ConditionItem condition1, ConditionItem condition2) {
        return new CompoundCondition(condition1, condition2, "and");
    }

    public NotCondition not(ConditionItem subCondition) {
        return new NotCondition(subCondition);
    }

    public CompoundCondition or(ConditionItem condition1, ConditionItem condition2) {
        return new CompoundCondition(condition1, condition2, "or");
    }

    public PropertyCondition profileProperty(String propertyName) {
        return new PropertyCondition("profilePropertyCondition", propertyName, definitionsService);
    }

    public PropertyCondition property(String conditionTypeId, String propertyName) {
        return new PropertyCondition(conditionTypeId, propertyName, definitionsService);
    }

    public ConditionItem condition(String conditionTypeId) {
        return new ConditionItem(conditionTypeId, definitionsService);
    }

    public abstract class ComparisonCondition extends ConditionItem {

        ComparisonCondition(String conditionTypeId, DefinitionsService definitionsService) {
            super(conditionTypeId, definitionsService);
        }

        public ComparisonCondition all(String... values) {
            return op("all").stringValues(values);
        }

        public ComparisonCondition all(Date... values) {
            return op("all").dateValues(values);
        }

        public ComparisonCondition all(Integer... values) {
            return op("all").integerValues(values);
        }

        public ComparisonCondition contains(String value) {
            return op("contains").stringValue(value);
        }

        public ComparisonCondition endsWith(String value) {
            return op("endsWith").stringValue(value);
        }

        public ComparisonCondition equalTo(String value) {
            return op("equals").stringValue(value);
        }

        public ComparisonCondition equalTo(Date value) {
            return op("equals").dateValue(value);
        }

        public ComparisonCondition equalTo(Integer value) {
            return op("equals").integerValue(value);
        }

        public ComparisonCondition equalTo(Double value) {
            return op("equals").doubleValue(value);
        }

        public ComparisonCondition exists() {
            return op("exists");
        }

        public ComparisonCondition greaterThan(Date value) {
            return op("greaterThan").dateValue(value);
        }

        public ComparisonCondition greaterThan(Integer value) {
            return op("greaterThan").integerValue(value);
        }

        public ComparisonCondition greaterThan(Double value) {
            return op("greaterThan").doubleValue(value);
        }

        public ComparisonCondition greaterThanOrEqualTo(Date value) {
            return op("greaterThanOrEqualTo").dateValue(value);
        }

        public ComparisonCondition greaterThanOrEqualTo(Integer value) {
            return op("greaterThanOrEqualTo").integerValue(value);
        }

        public ComparisonCondition greaterThanOrEqualTo(Double value) {
            return op("greaterThanOrEqualTo").doubleValue(value);
        }

        public ComparisonCondition in(String... values) {
            return op("in").stringValues(values);
        }

        public ComparisonCondition in(Date... values) {
            return op("in").dateValues(values);
        }

        public ComparisonCondition isDay(Date value) {
            return op("isDay").dateValue(value);
        }

        public ComparisonCondition isNotDay(Date value) {
            return op("isNotDay").dateValue(value);
        }

        public ComparisonCondition in(Integer... values) {
            return op("in").integerValues(values);
        }

        public ComparisonCondition in(Double... values) {
            return op("in").doubleValues(values);
        }

        public ComparisonCondition lessThan(Date value) {
            return op("lessThan").dateValue(value);
        }

        public ComparisonCondition lessThan(Integer value) {
            return op("lessThan").integerValue(value);
        }

        public ComparisonCondition lessThan(Double value) {
            return op("lessThan").doubleValue(value);
        }

        public ComparisonCondition lessThanOrEqualTo(Date value) {
            return op("lessThanOrEqualTo").dateValue(value);
        }

        public ComparisonCondition lessThanOrEqualTo(Integer value) {
            return op("lessThanOrEqualTo").integerValue(value);
        }

        public ComparisonCondition between(Date lowerBound, Date upperBound) {
            return op("between").dateValues(lowerBound, upperBound);
        }

        public ComparisonCondition between(Integer lowerBound, Integer upperBound) {
            return op("between").integerValues(lowerBound, upperBound);
        }

        public ComparisonCondition matchesRegex(String value) {
            return op("matchesRegex").stringValue(value);
        }

        public ComparisonCondition missing() {
            return op("missing");
        }

        public ComparisonCondition notEqualTo(String value) {
            return op("notEquals").stringValue(value);
        }

        public ComparisonCondition notEqualTo(Date value) {
            return op("notEquals").dateValue(value);
        }

        public ComparisonCondition notEqualTo(Integer value) {
            return op("notEquals").integerValue(value);
        }

        public ComparisonCondition notEqualTo(Double value) {
            return op("notEquals").doubleValue(value);
        }

        public ComparisonCondition notIn(String... values) {
            return op("notIn").stringValues(values);
        }

        public ComparisonCondition notIn(Date... values) {
            return op("notIn").dateValues(values);
        }

        public ComparisonCondition notIn(Integer... values) {
            return op("notIn").integerValues(values);
        }

        public ComparisonCondition notIn(Double... values) {
            return op("notIn").doubleValues(values);
        }

        private ComparisonCondition op(String op) {
            return parameter("comparisonOperator", op);
        }

        @Override
        public ComparisonCondition parameter(String name, Object value) {
            return (ComparisonCondition) super.parameter(name, value);
        }

        public ComparisonCondition parameter(String name, Object... values) {
            return (ComparisonCondition) super.parameter(name, values);
        }

        public ComparisonCondition startsWith(String value) {
            return op("startsWith").stringValue(value);
        }

        private ComparisonCondition stringValue(String value) {
            return parameter("propertyValue", value);
        }

        private ComparisonCondition integerValue(Integer value) {
            return parameter("propertyValueInteger", value);
        }

        private ComparisonCondition doubleValue(Double value) {
            return parameter("propertyValueDouble", value);
        }

        private ComparisonCondition dateValue(Date value) {
            return parameter("propertyValueDate", value);
        }

        private ComparisonCondition stringValues(String... values) {
            return parameter("propertyValues", values != null ? Arrays.asList(values) : null);
        }

        private ComparisonCondition integerValues(Integer... values) {
            return parameter("propertyValuesInteger", values != null ? Arrays.asList(values) : null);
        }

        private ComparisonCondition doubleValues(Double... values) {
            return parameter("propertyValuesDouble", values != null ? Arrays.asList(values) : null);
        }

        private ComparisonCondition dateValues(Date... values) {
            return parameter("propertyValuesDate", values != null ? Arrays.asList(values) : null);
        }
    }

    public class CompoundCondition extends ConditionItem {

        CompoundCondition(ConditionItem condition1, ConditionItem condition2, String operator) {
            super("booleanCondition", condition1.definitionsService);
            parameter("operator", operator);
            ArrayList<Condition> subConditions = new ArrayList<Condition>(2);
            subConditions.add(condition1.build());
            subConditions.add(condition2.build());
            parameter("subConditions", subConditions);
        }
    }

    public class ConditionItem {

        protected Condition condition;

        private DefinitionsService definitionsService;

        ConditionItem(String conditionTypeId, DefinitionsService definitionsService) {
            this.definitionsService = definitionsService;
            condition = new Condition(
                    this.definitionsService.getConditionType(conditionTypeId));
        }

        public Condition build() {
            return condition;
        }

        public ConditionItem parameter(String name, Object value) {
            condition.setParameter(name, value);
            return this;
        }

        public ConditionItem parameter(String name, Object... values) {
            condition.setParameter(name, values != null ? Arrays.asList(values) : null);
            return this;
        }

    }

    public class NotCondition extends ConditionItem {

        NotCondition(ConditionItem subCondition) {
            super("notCondition", subCondition.definitionsService);
            parameter("subCondition", subCondition.build());
        }
    }

    public class PropertyCondition extends ComparisonCondition {

        PropertyCondition(String conditionTypeId, String propertyName, DefinitionsService definitionsService) {
            super(conditionTypeId, definitionsService);
            condition.setParameter("propertyName", propertyName);
        }

    }
}
