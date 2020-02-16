package com.nkomiya.examples.beam.io;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import java.sql.ResultSetMetaData;
import java.sql.Types;

public class JDBCConnector {
  public interface MysqlOptions extends PipelineOptions {
    public String getUser();

    void setUser(String s);

    public String getPassword();

    void setPassword(String s);

    public String getDatabase();

    void setDatabase(String s);

    public String getTable();

    void setTable(String s);
  }

  public static void main(String[] args) {
    MysqlOptions opt = PipelineOptionsFactory.fromArgs(args).withValidation().as(MysqlOptions.class);
    Pipeline pipeline = Pipeline.create(opt);

    // MySQL connection info.
    String url = String.format("jdbc:mysql://localhost/%s?user=%s", opt.getDatabase(), opt.getUser());
    String query = String.format("SELECT * FROM %s;", opt.getTable());

    // Reader
    JdbcIO.Read<String> reader = JdbcIO.<String>read()
        .withDataSourceConfiguration(
            JdbcIO.DataSourceConfiguration.create(
                "com.mysql.cj.jdbc.Driver", url)
                .withUsername(opt.getUser())
                .withPassword(opt.getPassword()))
        .withQuery(query)
        .withRowMapper(
            (JdbcIO.RowMapper<String>) resultSet ->
            {
              // Map a SQL record to Jackson object mapper
              ObjectMapper mapper = new ObjectMapper();
              ObjectNode root = mapper.createObjectNode();

              // convert
              ResultSetMetaData meta = resultSet.getMetaData();
              for (int i = 1; i <= meta.getColumnCount(); i++) {
                String name = meta.getColumnName(i);
                switch (meta.getColumnType(i)) {
                  case Types.INTEGER:
                    root.put(name, resultSet.getInt(i));
                    break;
                  case Types.VARCHAR:
                    root.put(name, resultSet.getString(i));
                    break;
                  default:
                    throw new RuntimeException("Unsupported SQL date type");
                }
              }

              // Return JSON string
              return mapper.writeValueAsString(root);
            }).withCoder(StringUtf8Coder.of());


    // Construct pipeline Graph & execute
    pipeline
        .apply("Read from localhost", reader)
        .apply("print",
            ParDo.of(
                new DoFn<String, Void>() {
                  @ProcessElement
                  public void m(@Element String input) {
                    System.out.println(input);
                  }
                }
            ));
    pipeline.run();
  }
}
