package com.contiamo;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcRules;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.avatica.org.apache.http.annotation.Immutable;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.prepare.PlannerImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.*;
import org.apache.calcite.util.Util;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.rules.FilterSetOpTransposeRule;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rex.RexNode;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Properties;
import java.sql.*;

public class SampleApp {

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        // creates a calcite driver so queries go through it
        Class.forName("org.apache.calcite.jdbc.Driver");
        Properties properties = new Properties();
        properties.setProperty("schemaType", "JDBC");
        properties.setProperty("schema", "source1");
        properties.setProperty("schema.jdbcUrl", "jdbc:postgresql://localhost/foodmart");
        properties.setProperty("schema.jdbcUser", "foodmart");

        Connection connection =
                DriverManager.getConnection("jdbc:calcite:", properties);
        CalciteConnection calciteConnection =
                connection.unwrap(CalciteConnection.class);

        // creating mysql connection
        Class.forName("org.postgresql.Driver");

        List<RunQuery> queries = new ArrayList<RunQuery>();
        for (int i=0; i < 50; i++) {
            RunQuery rq = new RunQuery(calciteConnection.getRootSchema(), i);
            queries.add(rq);
        }

        for (RunQuery rq: queries) {
            rq.start();
        }

        for (RunQuery rq: queries) {
            try {
                rq.stop();
            } catch (InterruptedException e) {
                System.out.println(e.toString());
            }
        }
    }
}

public class RunQuery implements Runnable {
    private Thread t;
    private Integer number;
    private SchemaPlus rootSchema;

    RunQuery(SchemaPlus s, Integer n) {
        number = n;
        rootSchema = s;
    }

    @Override
    public void run() {
        JdbcConvention out = JdbcConvention.of(null, null, "source1");
        List<RelOptRule> rules = JdbcRules.rules(out);
        List<RelOptRule> newRules = rules;

        Program program = Programs.ofRules(newRules);

        System.out.println(newRules.toString());
        FrameworkConfig config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema.getSubSchema("source1"))
                .parserConfig(SqlParser.Config.DEFAULT)
                .programs(program)
                .build();

        RelBuilder builder = RelBuilder.create(config);

        RelNode root = builder
                .scan("agg_lc_06_sales_fact_1997")
                .filter(builder.call(SqlStdOperatorTable.EQUALS, builder.field(1, 0, "time_id"), builder.literal(400 + number)))
                .project(builder.field(1, 0, "time_id"))
                .build();

        PreparedStatement stmt = RelRunners.run(root);
        ResultSet resultSet = null;
        try {
            resultSet = stmt.executeQuery();
        } catch (SQLException e) {
            System.out.println(e.toString());
        }

        System.out.println(RelOptUtil.toString(root));
        try {
            printres(resultSet);
        } catch (SQLException e) {
            System.out.println(e.toString());
        }
    }

    private void printres(ResultSet resultSet) throws SQLException {
        final StringBuilder buf = new StringBuilder();
        while (resultSet.next()) {
            int n = resultSet.getMetaData().getColumnCount();
            for (int i = 1; i <= n; i++) {
                buf.append(i > 1 ? "; " : "")
                        .append(resultSet.getMetaData().getColumnLabel(i))
                        .append("=")
                        .append(resultSet.getObject(i));
            }
            System.out.println(buf.toString());
            buf.setLength(0);
        }
        resultSet.close();
    }

    public void start() {
        String name = Integer.toString(number);
        System.out.println("Starting " + name);
        if (t == null) {
            t = new Thread(this, name);
            t.start();
        }
    }

    public void stop() throws InterruptedException {
        t.join();
    }
}
