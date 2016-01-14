package org.apache.drill.exec.planner.logical;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;

import java.util.List;

public class SqlJoinFilterToLuceneQuery extends RexVisitorImpl<Void> {

    private List<String> indexFields;
    private DrillJoinRel joinRel;
    private BooleanQuery compositeQuery;
    private Analyzer analyzer;

    private String currentField;
    private String currentValue;
    private RelDataType currentType;
    private SqlKind currentSqlKind;
    private Boolean isCurrentFieldIndexed;

    public SqlJoinFilterToLuceneQuery(List<String> indexFields, DrillJoinRel joinRel) {
        super(true);
        this.indexFields = indexFields;
        this.joinRel = joinRel;
        this.compositeQuery = new BooleanQuery();
        this.analyzer = new StandardAnalyzer(); //The classic analyser handles escaping in an almost
    }

    public Query getLuceneQuery() {
        return compositeQuery;
    }

    @Override
    public Void visitInputRef(RexInputRef rexInputRef) {
        int index = rexInputRef.getIndex();
        currentField = joinRel.getRowType().getFieldList().get(index).getName();
        currentType = joinRel.getRowType().getFieldList().get(index).getType();
        if (indexFields.contains(currentField)) {
            if (currentSqlKind == null) {
                throw new RuntimeException(new Exception("SqlKind not set"));
            }
            if (currentSqlKind != SqlKind.EQUALS) {
                throw new RuntimeException(new Exception("For indexed fields only equality conditions are valid. You have used " + currentSqlKind.toString()));
            }
            isCurrentFieldIndexed = true;
        } else {
            isCurrentFieldIndexed = false;
        }
        return super.visitInputRef(rexInputRef);
    }

    @Override
    public Void visitLocalRef(RexLocalRef localRef) {
        return super.visitLocalRef(localRef);
    }

    @Override
    public Void visitLiteral(RexLiteral literal) {
        return super.visitLiteral(literal);
    }

    @Override
    public Void visitOver(RexOver over) {
        return super.visitOver(over);
    }

    @Override
    public Void visitCorrelVariable(RexCorrelVariable correlVariable) {
        return super.visitCorrelVariable(correlVariable);
    }

    @Override
    public Void visitCall(RexCall rexCall) {
        //Support nested conditions
        currentSqlKind = rexCall.getKind();
        rexCall.getOperands().get(0).accept(this);
        rexCall.getOperands().get(1).accept(this);

        //return super.visitCall(rexCall);
        return null;
    }

    @Override
    public Void visitDynamicParam(RexDynamicParam dynamicParam) {
        return super.visitDynamicParam(dynamicParam);
    }

    @Override
    public Void visitRangeRef(RexRangeRef rangeRef) {
        return super.visitRangeRef(rangeRef);
    }

    @Override
    public Void visitFieldAccess(RexFieldAccess fieldAccess) {
        return super.visitFieldAccess(fieldAccess);
    }
}
