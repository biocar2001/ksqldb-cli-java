package com.carlos.example.ksqlcli;

import com.carlos.example.ksqlcli.utils.UtilsKsqlCli;
import io.confluent.ksql.api.client.*;
import org.apache.commons.io.FileUtils;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class App {
    //properties
    public static String KSQLDB_SERVER_HOST = "127.0.0.1";
    public static int KSQLDB_SERVER_HOST_PORT = 8088;

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException, URISyntaxException {
        ClientOptions options = ClientOptions.create()
                .setHost(KSQLDB_SERVER_HOST)
                .setPort(KSQLDB_SERVER_HOST_PORT);
        Client client = Client.create(options);

        //Create Stream of Data from file
        String sql = null;
        ExecuteStatementResult result_create_stream = null;
        Map<String, Object> propertiesCreateStream = Collections.singletonMap("auto.offset.reset", "earliest");
        try {
            sql = FileUtils.readFileToString(new UtilsKsqlCli().getFileFromResource("create_streams.ksql"), "UTF-8");
            result_create_stream = client.executeStatement(sql,propertiesCreateStream).get();
            System.out.println("Query ID Create Stream: " + result_create_stream.queryId().orElse("<null>"));

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

        // Create tbale from stream reading from file
        Thread.sleep(100);
        String sql2 = FileUtils.readFileToString(new UtilsKsqlCli().getFileFromResource("create_tables_out.ksql"), "UTF-8");
        Map<String, Object> properties = Collections.singletonMap("auto.offset.reset", "earliest");
        ExecuteStatementResult result = null;
        try {
            result = client.executeStatement(sql2, properties).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        System.out.println("Query ID create Table: " + result.queryId().orElse("<null>"));
        Thread.sleep(100);
        // Show all streams created
        List<StreamInfo> streams = client.listStreams().get();
        for (StreamInfo stream : streams) {
            System.out.println(
                    stream.getName()
                            + " " + stream.getTopic()
                            + " " + stream.getKeyFormat()
                            + " " + stream.getValueFormat()
                            + " " + stream.isWindowed()
            );
        }
        // Show all tables created
        List<TableInfo> tables = client.listTables().get();
        for (TableInfo table : tables) {
            System.out.println(
                    table.getName()
                            + " " + table.getTopic()
                            + " " + table.getKeyFormat()
                            + " " + table.getValueFormat()
                            + " " + table.isWindowed()
            );
        }
        client.close();
    }

}
