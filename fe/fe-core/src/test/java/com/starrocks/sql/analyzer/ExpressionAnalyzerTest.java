// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.CollectionSubscriptExpr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.MapType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.qe.ConnectContext;
import com.starrocks.thrift.TExprNodeType;
import org.junit.Assert;
import org.junit.Test;

public class ExpressionAnalyzerTest {

    @Test
    public void testMapSubscriptAnalyzer() throws Exception {
        ExpressionAnalyzer.Visitor visitor = new ExpressionAnalyzer.Visitor(new AnalyzeState(), new ConnectContext());
        SlotRef slot = new SlotRef(null, "col", "col");
        Type keyType = ScalarType.createType(PrimitiveType.INT);
        Type valueType = ScalarType.createCharType(10);
        Type mapType = new MapType(keyType, valueType);
        slot.setType(mapType);

        IntLiteral sub = new IntLiteral(10);

        CollectionSubscriptExpr collectionSubscriptExpr = new CollectionSubscriptExpr(slot, sub);
        try {
            visitor.visitCollectionSubscriptExpr(collectionSubscriptExpr,
                    new Scope(RelationId.anonymous(), new RelationFields()));
        } catch (Exception e) {
            Assert.assertFalse(true);
        }

        StringLiteral subCast = new StringLiteral("10");
        CollectionSubscriptExpr collectionSubscriptExpr1 = new CollectionSubscriptExpr(slot, subCast);
        try {
            visitor.visitCollectionSubscriptExpr(collectionSubscriptExpr1,
                    new Scope(RelationId.anonymous(), new RelationFields()));
        } catch (Exception e) {
            Assert.assertFalse(true);
        }

        StringLiteral subNoCast = new StringLiteral("aaa");
        CollectionSubscriptExpr collectionSubscriptExpr2 = new CollectionSubscriptExpr(slot, subNoCast);
        Assert.assertThrows(SemanticException.class,
                () -> visitor.visitCollectionSubscriptExpr(collectionSubscriptExpr2,
                        new Scope(RelationId.anonymous(), new RelationFields())));

        Type keyTypeChar = ScalarType.createCharType(10);
        Type valueTypeInt = ScalarType.createType(PrimitiveType.INT);
        mapType = new MapType(keyTypeChar, valueTypeInt);
        slot.setType(mapType);
        StringLiteral subString = new StringLiteral("aaa");
        CollectionSubscriptExpr collectionSubscriptExpr3 = new CollectionSubscriptExpr(slot, subCast);
        try {
            visitor.visitCollectionSubscriptExpr(collectionSubscriptExpr3,
                    new Scope(RelationId.anonymous(), new RelationFields()));
        } catch (Exception e) {
            Assert.assertFalse(true);
        }

        Assert.assertEquals(TExprNodeType.MAP_SUBSCRIPT_EXPR,
                collectionSubscriptExpr3.treeToThrift().getNodes().get(0).getNode_type());
    }

    @Test
    public void testArraySubscriptAnalyzer() throws Exception {
        ExpressionAnalyzer.Visitor visitor = new ExpressionAnalyzer.Visitor(new AnalyzeState(), new ConnectContext());
        SlotRef slot = new SlotRef(null, "col", "col");
        Type elementType = ScalarType.createCharType(10);
        Type arrayType = new ArrayType(elementType);
        slot.setType(arrayType);

        IntLiteral sub = new IntLiteral(10);

        CollectionSubscriptExpr collectionSubscriptExpr = new CollectionSubscriptExpr(slot, sub);
        try {
            visitor.visitCollectionSubscriptExpr(collectionSubscriptExpr,
                    new Scope(RelationId.anonymous(), new RelationFields()));
        } catch (Exception e) {
            Assert.assertFalse(true);
        }

        StringLiteral subCast = new StringLiteral("10");
        CollectionSubscriptExpr collectionSubscriptExpr1 = new CollectionSubscriptExpr(slot, subCast);
        Assert.assertThrows(SemanticException.class,
                () -> visitor.visitCollectionSubscriptExpr(collectionSubscriptExpr1,
                        new Scope(RelationId.anonymous(), new RelationFields())));

        StringLiteral subNoCast = new StringLiteral("aaa");
        CollectionSubscriptExpr collectionSubscriptExpr2 = new CollectionSubscriptExpr(slot, subNoCast);
        Assert.assertThrows(SemanticException.class,
                () -> visitor.visitCollectionSubscriptExpr(collectionSubscriptExpr2,
                        new Scope(RelationId.anonymous(), new RelationFields())));

        Assert.assertEquals(TExprNodeType.ARRAY_ELEMENT_EXPR,
                collectionSubscriptExpr2.treeToThrift().getNodes().get(0).getNode_type());
    }

    @Test
    public void testNoSubscriptAnalyzer() throws Exception {
        ExpressionAnalyzer.Visitor visitor = new ExpressionAnalyzer.Visitor(new AnalyzeState(), new ConnectContext());
        SlotRef slot = new SlotRef(null, "col", "col");
        slot.setType(ScalarType.createType(PrimitiveType.INT));

        IntLiteral sub = new IntLiteral(10);

        CollectionSubscriptExpr collectionSubscriptExpr = new CollectionSubscriptExpr(slot, sub);
        Assert.assertThrows(SemanticException.class,
                () -> visitor.visitCollectionSubscriptExpr(collectionSubscriptExpr,
                        new Scope(RelationId.anonymous(), new RelationFields())));
    }
}
