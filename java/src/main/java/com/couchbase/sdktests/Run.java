package com.couchbase.sdktests;

import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.manager.search.SearchIndex;
import com.couchbase.client.java.manager.view.DesignDocument;
import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.view.DesignDocumentNamespace;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.FileInputStream;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static com.couchbase.client.java.kv.MutateInSpec.upsert;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

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
        ClusterEnvironment env;
        // check if CA file is provided
        if (cmd.hasOption("cafile")) {
            // add cert to cluster options
            String caFile = cmd.getOptionValue("cafile");
            List<X509Certificate> certs = new ArrayList<>();
            FileInputStream fis = new FileInputStream(caFile);
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            while (fis.available() > 0) {
                X509Certificate cert = (X509Certificate) cf.generateCertificate(fis);
                System.out.println(cert.toString());
                certs.add(cert);
            }
            fis.close();
            env = ClusterEnvironment.builder().securityConfig(SecurityConfig.enableTls(true).trustCertificates(certs)).build();
        } else {
            env = ClusterEnvironment.builder().build();
        }

        // connect to cluster
        Cluster cluster = Cluster.connect(connection, ClusterOptions.clusterOptions(username, password).environment(env));
        cluster.waitUntilReady(Duration.ofSeconds(5));

        // connect to bucket & default collection
        Bucket bucket = cluster.bucket(bucketName);
        bucket.waitUntilReady(Duration.ofSeconds(5));
        Collection collection = bucket.defaultCollection();

        // upsert a doc
        JsonObject content = JsonObject.create().put("author", "mike").put("title", "My Blog Post 1");
        collection.upsert("test-key", content);
        System.out.println("upsert done");

        // subdoc mutate
        collection.mutateIn("test-key", Arrays.asList(upsert("author", "steve")));
        System.out.println("subdoc mutate done");

        // run a n1ql query
        cluster.query("SELECT *");
        System.out.println("n1ql query done");

        // run an analytics query
        cluster.analyticsQuery("select \"hello\" as greeting");
        System.out.println("analytics query done");

        // create fts index
        String indexName = "idx-" + UUID.randomUUID().toString().substring(0, 8);
        cluster.searchIndexes().upsertIndex(new SearchIndex(indexName, bucketName));

        // try to run an fts search, waiting for index to be created
        try {
            runWithRetry(Duration.ofSeconds(5), () -> {
                cluster.searchQuery(indexName, SearchQuery.queryString("test"));
            });
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.out.println("fts done");

        // create view design doc
        String ddName = "dd-" + UUID.randomUUID().toString().substring(0, 8);
        String viewName = "view-" + UUID.randomUUID().toString().substring(0, 8);
        DesignDocument dd = new DesignDocument(ddName).putView(viewName, "function(doc,meta) { emit(meta.id, doc) }");
        bucket.viewIndexes().upsertDesignDocument(dd, DesignDocumentNamespace.PRODUCTION);

        bucket.viewQuery(ddName, viewName);
        System.out.println("views done");

        cluster.disconnect();
        env.shutdown();
    }

    private static void runWithRetry(Duration timeout, Runnable task) throws Throwable {
        long startNanos = System.nanoTime();
        Throwable deferred = null;
        do {
            if (deferred != null) {
                MILLISECONDS.sleep(250);
            }
            try {
                task.run();
                return;
            } catch (Throwable t) {
                System.out.println("Retrying FTS (waiting for index)");
                deferred = t;
            }
        } while (System.nanoTime() - startNanos < timeout.toNanos());
        throw deferred;
    }
}
