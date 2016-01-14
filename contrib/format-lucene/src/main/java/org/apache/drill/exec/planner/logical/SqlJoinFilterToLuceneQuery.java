package org.apache.drill.exec.planner.logical;

import org.apache.calcite.rex.*;
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
    public Void visitInputRef(RexInputRef inputRef) {
        return super.visitInputRef(inputRef);
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
    public Void visitCall(RexCall call) {
        return super.visitCall(call);
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
