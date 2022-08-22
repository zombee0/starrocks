// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.operator.scalar;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.base.ColumnRefSet;

import java.util.List;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.operator.OperatorType.COLLECTION_SUBSCRIPT;

public class CollectionSubscriptOperator extends ScalarOperator {
    protected List<ScalarOperator> arguments = Lists.newArrayList();

    public CollectionSubscriptOperator(Type type, ScalarOperator arrayOperator, ScalarOperator subscriptOperator) {
        super(COLLECTION_SUBSCRIPT, type);
        this.arguments.add(arrayOperator);
        this.arguments.add(subscriptOperator);
    }

    @Override
    public boolean isNullable() {
        return true;
    }

    @Override
    public List<ScalarOperator> getChildren() {
        return arguments;
    }

    @Override
    public ScalarOperator getChild(int index) {
        return arguments.get(index);
    }

    @Override
    public void setChild(int index, ScalarOperator child) {
        arguments.set(index, child);
    }

    @Override
    public String toString() {
        return arguments.stream().map(ScalarOperator::toString).collect(Collectors.joining(","));
    }

    @Override
    public ColumnRefSet getUsedColumns() {
        ColumnRefSet used = new ColumnRefSet();
        for (ScalarOperator child : arguments) {
            used.union(child.getUsedColumns());
        }
        return used;
    }

    @Override
    public ScalarOperator clone() {
        CollectionSubscriptOperator operator = (CollectionSubscriptOperator) super.clone();
        // Deep copy here
        List<ScalarOperator> newArguments = Lists.newArrayList();
        this.arguments.forEach(p -> newArguments.add(p.clone()));
        operator.arguments = newArguments;
        return operator;
    }

    @Override
    public <R, C> R accept(ScalarOperatorVisitor<R, C> visitor, C context) {
        return visitor.visitCollectionSub(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CollectionSubscriptOperator that = (CollectionSubscriptOperator) o;
        return Objects.equal(arguments, that.arguments);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(arguments);
    }
}
