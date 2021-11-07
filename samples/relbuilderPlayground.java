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

public class Playground {

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
//        System.out.println(org.apache.calcite.plan.RelOptPlanner.LOGGER.isTraceEnabled());

        // creates a calcite driver so queries go through it
        Class.forName("org.apache.calcite.jdbc.Driver");
        Properties properties = new Properties();
//        properties.setProperty("schemaType", "JDBC");
//        properties.setProperty("schema", "source1");
//        properties.setProperty("schema.jdbcUrl", "jdbc:postgresql://localhost/foodmart");
//        properties.setProperty("schema.jdbcUser", "foodmart");

        Connection connection =
                DriverManager.getConnection("jdbc:calcite:", properties);
        CalciteConnection calciteConnection =
                connection.unwrap(CalciteConnection.class);

        // creating mysql connection
        Class.forName("org.postgresql.Driver");

        // Data source 1

        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDefaultCatalog("public");
        dataSource.setUrl("jdbc:postgresql://localhost/foodmart");
        dataSource.setUsername("foodmart");
        SchemaPlus rootSchema = calciteConnection.getRootSchema();

        JdbcSchema jdbcSchema = JdbcSchema.create(rootSchema, "source1", dataSource, null, null);

        // adding schema to connection
        rootSchema.add("source1", jdbcSchema);

        CalciteConnection calciteConnection2 =
                connection.unwrap(CalciteConnection.class);
        calciteConnection.close();

        try {
            Thread.sleep(5000);
        } catch(InterruptedException ex) {
            Thread.currentThread().interrupt();
        }

        try {
            SampleApp.testcase(calciteConnection2.getRootSchema());
        } catch (SqlParseException e) {
            System.out.println(e);
        } catch (ValidationException e) {
            System.out.println(e);
        } catch (RelConversionException e) {
            System.out.println(e);
        } catch (Throwable e){
            System.out.println(e);
        }
    }

    private static void testsql(Connection connection) throws ClassNotFoundException, SQLException {
        // creating statement and executing
        Statement statement = connection.createStatement();
        ResultSet resultSet =
                statement.executeQuery("select \"source1\".\"agg_lc_06_sales_fact_1997\".\"time_id\", \"source1\".\"agg_lc_06_sales_fact_1997\".\"t_arr\"[1] from \"source1\".\"agg_lc_06_sales_fact_1997\" where \"source1\".\"agg_lc_06_sales_fact_1997\".\"time_id\" = 400 AND 400 = any(\"source1\".\"agg_lc_06_sales_fact_1997\".\"t_arr\")");
        printres(resultSet);
        statement.close();
        connection.close();
    }

    private static void testcase(SchemaPlus rootSchema) throws ClassNotFoundException, SQLException, SqlParseException, ValidationException, RelConversionException {
        JdbcConvention out = JdbcConvention.of(null, null, "source1");
        List<RelOptRule> rules = JdbcRules.rules(out);
        RelOptRule rule1 = FilterSetOpTransposeRule.INSTANCE;
        RelOptRule rule2 = ProjectRemoveRule.INSTANCE;

//        List<RelOptRule> newRules = new ImmutableList.Builder<RelOptRule>().addAll(rules).add(rule1).add(rule2).build();
        List<RelOptRule> newRules = rules;

        Program program = Programs.ofRules(newRules);

        System.out.println(newRules.toString());
        FrameworkConfig config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema.getSubSchema("source1"))
                .parserConfig(SqlParser.Config.DEFAULT)
//                .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2))
                .programs(program)
                .build();
//        Planner planner = Frameworks.getPlanner(config);
//        convention.register(planner);

        RelBuilder builder = RelBuilder.create(config);

        String snippet = "store_sales / store_cost";
        SqlNode sn = SqlParser.create(snippet).parseExpression();


        /*
        RexBuilder rexBuilder = createRexBuilder();
        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
        RelOptPlanner planner = cluster.getPlanner();
        planner.setExecutor(config.getExecutor());
        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);

        RelDataTypeFieldImpl

        SqlImplementor
        relNode.getCluster()
        final SqlConformance conformance = conformance();
        CalciteSqlValidator validator = new CalciteSqlValidator(operatorTable, catalogReader, typeFactory,
                conformance);

        SqlToRelConverter conv = new SqlToRelConverter(new ViewExpanderImpl(), validator, createCatalogReader(rootSchema.getSubSchema("source1")), cluster, convertletTable);
        RexNode rn = conv.convertExpression(sn);

*/

        // plannerImpl can't handle the snippet because it expects a full query expression; not a snippet
//        PlannerImpl planner = new PlannerImpl(config);
//        SqlNode snParsed = planner.parse(snippet);
//        SqlNode snValid = planner.validate(snParsed);
//        System.out.println("after validate");
//        RelRoot snFinal = planner.rel(snValid);
        builder.scan("agg_lc_06_sales_fact_1997");
        RelNode left = builder
                .filter(builder.call(SqlStdOperatorTable.EQUALS, builder.field(1, 0, "city"), builder.literal("Coronado")))
                .scan("time_by_day2")
                .filter(builder.call(SqlStdOperatorTable.EQUALS, builder.field(1, 0, "the_year"), builder.literal(1997)))
                .filter(builder.call(SqlStdOperatorTable.EQUALS, builder.field(1, 0, "month_of_year"), builder.literal(2)))
                .filter(builder.call(SqlStdOperatorTable.EQUALS, builder.field(1, 0, "day_of_month"), builder.literal(3)))
                .join(JoinRelType.INNER, "time_id")
                .aggregate(builder.groupKey(builder.field(1, 0, "the_year"), builder.field(1, 0, "month_of_year")), builder.count(false, "cnt"), builder.sum(false, "sm", builder.field("unit_sales")))
                .build();

        RelBuilder lhs = builder.push(left);

        RelNode right = builder.scan("agg_c_special_sales_fact_1997")
                .aggregate(builder.groupKey(builder.field(1, 0, "time_year"), builder.field(1, 0, "time_month")), builder.sum(false, "m2", builder.field("unit_sales_sum")))
                .build();

//        RelNode root = builder
//                .push(left)
        RelNode root = lhs
                .push(right)
                .join(JoinRelType.INNER, builder.call(SqlStdOperatorTable.EQUALS,
                        builder.field(2, 0, "the_year"),
                        builder.field(2, 1, "time_year")
                        ), builder.call(SqlStdOperatorTable.EQUALS,
                        builder.field(2, 0, "month_of_year"),
                        builder.field(2, 1, "time_month"))
                )
                .build();

//        RelNode root = builder
//                .scan("agg_lc_06_sales_fact_1997")
//                .aggregate(builder.groupKey("time_id", "city"), builder.count(false, "cnt"), builder.sum(false, "sm", builder.field("unit_sales")))
//                .filter(builder.call(SqlStdOperatorTable.EQUALS, builder.field(1, 0, "time_id"), builder.literal(400)))
//                //.project(builder.field(1, 0, "time_id"))
//                .build();

        System.out.println(RelOptUtil.toString(root));
        PreparedStatement stmt = RelRunners.run(root);
        ResultSet resultSet = stmt.executeQuery();
        printres(resultSet);
    }

    // helper methods from PlannerImpl.java
    /*
    private static CalciteCatalogReader createCatalogReader(SchemaPlus defaultSchema) {
        SchemaPlus rootSchema = rootSchema(defaultSchema);
        return new CalciteCatalogReader(
                CalciteSchema.from(rootSchema),
                parserConfig.caseSensitive(),
                CalciteSchema.from(defaultSchema).path(null),
                typeFactory);
    }
    private static SchemaPlus rootSchema(SchemaPlus schema) {
        for (;;) {
            if (schema.getParentSchema() == null) {
                return schema;
            }
            schema = schema.getParentSchema();
        }
    }
    */


    private static void strbuild(Connection connection) throws ClassNotFoundException, SQLException {
        // creating statement and executing
        Statement statement = connection.createStatement();
        ResultSet resultSet =
                statement.executeQuery("select *\n"
                        + "from \"source1\".\"articles\" "
                        + "join \"source2\".\"article_facts\" on  \"source2\".\"article_facts\".\"article_id\" = \"source1\".\"articles\".\"id\" WHERE \"source1\".\"articles\".\"id\" = 0"
                );
        statement.close();
        connection.close();
    }

    private static void relsimple(SchemaPlus rootSchema) throws ClassNotFoundException, SQLException {
        FrameworkConfig config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema)
                .parserConfig(SqlParser.Config.DEFAULT)
                .build();
        RelBuilder builder = RelBuilder.create(config);
        //RelRunner runner = connection.unwrap(RelRunner.class);
//        builder.scan("article_facts").build();


        builder.scan("source1", "article_facts")
                .filter(builder.call(SqlStdOperatorTable.EQUALS, builder.field(1, 0, "property_id"), builder.literal(5)))
                .project(builder.field(1, 0, "property_id"), builder.field(1, 0, "article_id"))
                .scan("source1", "articles")
                .project(builder.field(1, 0, "version"), builder.field(1, 0, "id"), builder.field(1, 0, "title"))
                .join(JoinRelType.INNER, builder.call(SqlStdOperatorTable.EQUALS,
                        builder.field(2, 0, "article_id"),
                        builder.field(2, 1, "id")
                ))
//            .project(builder.field(1, 0, "article_id"))
//            .distinct()
                .limit(0, 5)
        ;

        RelNode node = builder.build();

        System.out.println(RelOptUtil.toString(node));

        PreparedStatement stmt = RelRunners.run(node);
        ResultSet resultSet = stmt.executeQuery();

        printres(resultSet);
    }

    private static void relbuild(SchemaPlus rootSchema) throws ClassNotFoundException, SQLException {
        FrameworkConfig config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema.getSubSchema("source1"))
                .parserConfig(SqlParser.Config.DEFAULT)
                .build();
        System.out.println(rootSchema.getSubSchema("source1").getTableNames());

        RelBuilder builder = RelBuilder.create(config);

        RelNode root = builder
//            .scan("article_facts")
//            .filter(builder.call(SqlStdOperatorTable.EQUALS, builder.field(1, 0, "property_id"), builder.literal(5)))
                .scan("agg_lc_06_sales_fact_1997")
                .filter(builder.call(SqlStdOperatorTable.EQUALS, builder.field(1, 0, "time_id"), builder.literal(400)))
                .project(builder.field(1, 0, "time_id"))
//            .project(builder.field(1, 0, "property_id"), builder.field(1, 0, "article_id"))
//            .project(builder.field(1, 0, "article_id"))
//            .scan("source2", "articles")
//            .project(builder.field(1, 0, "id"))
//            .join(JoinRelType.INNER,
//                builder.call(SqlStdOperatorTable.EQUALS,
//                    builder.field(2, 0, "article_id"),
//                    builder.field(2, 1, "id"))
//            )
//            .aggregate(builder.groupKey(builder.field("article_id")), builder.sum(false, "m1", builder.field(1, 0, "property_id")))
//            .distinct()
//            .limit(0, 5)
                .build()
                ;

//        System.out.println(RelOptUtil.toString(root));
//        System.out.println("some text");

        PreparedStatement stmt = RelRunners.run(root);
        ResultSet resultSet = stmt.executeQuery();

        printres(resultSet);
    }

    private static void printres(ResultSet resultSet) throws SQLException {
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
}
