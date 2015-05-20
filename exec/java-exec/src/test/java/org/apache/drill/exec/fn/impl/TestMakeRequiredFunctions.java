package org.apache.drill.exec.fn.impl;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.util.TestTools;
import org.junit.Test;

/**
 * Created by rchallapalli on 5/20/15.
 */
public class TestMakeRequiredFunctions extends BaseTestQuery {
    static final String WORKING_PATH = TestTools.getWorkingPath();
    static final String TEST_RES_PATH = WORKING_PATH + "/src/test/resources";

    @Test
    public void testMakeRequiredInt() throws Exception {
        String query1 = String.format("select r.columns[0] as v, r.columns[1] as w, r.columns[2] as x, u.columns[0] as y, t.columns[0] as z from dfs_test.`%s/uservisits/rankings.tbl` r, "
                + " dfs_test.`%s/uservisits/uservisits.tbl` u, dfs_test.`%s/uservisits/temp1.tbl` t "
                + " where r.columns[1]=u.columns[1] and r.columns[1] = t.columns[1]", TEST_RES_PATH, TEST_RES_PATH, TEST_RES_PATH);
        test(query1);
    }

    public void testMakeRequiredInt1() throws Exception {
        String query1 = String.format("select d.id from dfs_test.`%s/jsoninput/fewtypes_null.json`", TEST_RES_PATH);
        test(query1);

    }


}
