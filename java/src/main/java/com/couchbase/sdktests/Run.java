package com.couchbase.sdktests;

import com.couchbase.client.core.message.search.UpsertSearchIndexRequest;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.analytics.AnalyticsQuery;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.view.DefaultView;
import com.couchbase.client.java.view.DesignDocument;
import com.couchbase.client.java.view.ViewQuery;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.util.Arrays;
import java.util.UUID;

public class Run {

    public static void main(String... args) throws Exception {

        Options options = new Options();
        options.addOption(Option.builder().longOpt("connection").hasArg().required().desc("Connection string for Couchbase cluster").build());
        options.addOption(Option.builder().longOpt("username").hasArg().required().desc("Username for Couchbase cluster").build());
        options.addOption(Option.builder().longOpt("password").hasArg().required().desc("Password for Couchbase cluster").build());
        options.addOption(Option.builder().longOpt("bucket").hasArg().required().desc("Bucket on which to run").build());
        options.addOption(Option.builder().longOpt("cafile").hasArg().desc("CA File Path").build());

        CommandLine cmd = new DefaultParser().parse(options, args);

        // options:
        // connection string
        String connection = cmd.getOptionValue("connection");
        // username
        String username = cmd.getOptionValue("username");
        // password
        String password = cmd.getOptionValue("password");
        // bucket name
        String bucketName = cmd.getOptionValue("bucket");

        // set up cluster login with username/password
        CouchbaseEnvironment env;
        // check if CA file is provided
        if (cmd.hasOption("cafile")) {
            String caFile = cmd.getOptionValue("cafile");
            env = DefaultCouchbaseEnvironment.builder().sslEnabled(true).sslKeystoreFile(caFile).build();
        } else {
            env = DefaultCouchbaseEnvironment.builder().build();
        }

        // connect to cluster
        Cluster cluster = CouchbaseCluster.create(connection);
        cluster.authenticate(username, password);

        // connect to bucket & default collection
        Bucket bucket = cluster.openBucket(bucketName);

        // upsert a doc
        JsonObject content = JsonObject.create().put("author", "mike").put("title", "My Blog Post 1");
        bucket.upsert(JsonDocument.create("test-key", content));
        System.out.println("upsert done");

        // subdoc mutate
        bucket.mutateIn("test-key").upsert("author", "steve").execute();
        System.out.println("subdoc mutate done");

        // run a n1ql query
        bucket.query(N1qlQuery.simple("SELECT *"));
        System.out.println("n1ql query done");

        // run an analytics query
        AnalyticsQuery.simple("select \"hello\" as greeting");
        System.out.println("analytics query done");

        // create fts index

        String indexName = "idx-" + UUID.randomUUID().toString().substring(0, 8);
        String indexDefinition = "{\"name\":\"index-name\",\"type\":\"" + indexName + "\",\"params\":{\"mapping\":{\"default_mapping\":{\"enabled\":true," +
                "\"dynamic\":true},\"default_type\":\"_default\",\"default_analyzer\":\"standard\",\"default_datetime_parser\":\"dateTimeOptional\"," +
                "\"default_field\":\"_all\",\"store_dynamic\":false,\"index_dynamic\":true},\"store\":{\"indexType\":\"scorch\",\"kvStoreName\":\"\"}," +
                "\"doc_config\":{\"mode\":\"type_field\",\"type_field\":\"type\",\"docid_prefix_delim\":\"\",\"docid_regexp\":\"\"}},\"sourceType\":" +
                "\"couchbase\",\"sourceName\":\"" + bucketName + "\",\"sourceUUID\":\"\",\"sourceParams\":{},\"planParams\":{\"maxPartitionsPerPIndex" +
                "\":171,\"numReplicas\":0,\"indexPartitions\":6},\"uuid\":\"\"}";
        cluster.core().send(new UpsertSearchIndexRequest(indexName, indexDefinition, username, password)).toBlocking().single();

        bucket.query(new SearchQuery(indexName, SearchQuery.match("test")));
        System.out.println("fts done");

        // create view design doc
        String ddName = "dd-" + UUID.randomUUID().toString().substring(0, 8);
        String viewName = "view-" + UUID.randomUUID().toString().substring(0, 8);
        DesignDocument dd = DesignDocument.create(ddName, Arrays.asList(DefaultView.create(viewName, "function(doc,meta) { emit(meta.id, doc) }")));
        bucket.bucketManager().insertDesignDocument(dd);

        bucket.query(ViewQuery.from(ddName, viewName));
        System.out.println("views done");

        cluster.disconnect();
        env.shutdown();
    }
}
