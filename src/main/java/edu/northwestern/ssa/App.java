package edu.northwestern.ssa;

import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.archive.io.warc.WARCReader;
import org.archive.io.warc.WARCReaderFactory;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import javax.swing.plaf.synth.SynthDesktopIconUI;
import java.io.*;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class App {
    // Source (exceptions): https://stackoverflow.com/questions/2305966/why-do-i-get-the-unhandled-exception-type-ioexception
    public static void main(String[] args) throws IOException {
        // Initializations
        String search_host=System.getenv("ELASTIC_SEARCH_HOST");
        String search_index=System.getenv("ELASTIC_SEARCH_INDEX");
        String newKey="";

        // Using steve's testing
        String common_crawl=System.getenv("COMMON_CRAWL_FILENAME");
        //String key="crawl-data/CC-NEWS/2020/04/CC-NEWS-20200405100433-01526.warc.gz";
        Path path = FileSystems.getDefault().getPath("").toAbsolutePath();

        //Create the path for the downloaded file
        //String idStr = common_crawl.substring(common_crawl.lastIndexOf('/') + 1);
        File file=new File(path+"\\"+"output.warc.gz");
        String prefix="crawl-data/CC-NEWS/2020";

        //create an object of type software.amazon.awssdk.services.s3.S3Client
        S3Client s3 = S3Client.builder()
                .region(Region.US_EAST_1)
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .apiCallTimeout(Duration.ofMinutes(30)).build())
                .build();

        // Fetch latest warc file
        // SOURCE: https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/s3/paginators/ListObjectsV2Iterable.html
        ListObjectsV2Iterable iterate = s3.listObjectsV2Paginator(ListObjectsV2Request.builder()
                .bucket("commoncrawl")
                .prefix(prefix)
                .build());
        SdkIterable<S3Object> contents = iterate.contents();
        Instant latest = null;
        for (S3Object i : contents) {
            boolean islatest = ((latest == null) || (i.lastModified().compareTo(latest) > 0));
            if (islatest) {
                latest = i.lastModified();
                newKey = i.key();
                System.out.println(newKey);
            }
        }

        // Solve issue where first half of tests pass but the other half fail and vice-versa depending on
        // supplied environment
        if(!file.exists()) {
            try {
                System.out.println("Crawl was specified");
                GetObjectRequest request = GetObjectRequest.builder()
                        .bucket("commoncrawl")
                        .key(common_crawl)
                        .build();
                s3.getObject(request, ResponseTransformer.toFile(file));
                s3.close();
            } catch (Exception e) {
                System.out.println("Crawl was actually not specified");
                GetObjectRequest request = GetObjectRequest.builder()
                        .bucket("commoncrawl")
                        .key(newKey)
                        .build();
                s3.getObject(request, ResponseTransformer.toFile(file));
                s3.close();
            }
        }

        // START OF PART 2 (PARSE WARC)
        ElasticSearch search= new ElasticSearch("es");
        HttpExecuteResponse response=search.index(search_host,search_index);
        ArchiveReader archiver=WARCReaderFactory.get(file);

        // Source: https://github.com/Smerity/cc-warc-examples/blob/master/src/org/commoncrawl/examples/S3ReaderTest.java
        for (ArchiveRecord i : archiver) {
            if (i.getHeader().getHeaderValue("WARC-Type").equals("response")) {
                String url = i.getHeader().getUrl();
                int length = i.available();

                // SOURCE: https://stackoverflow.com/questions/309424/how-do-i-read-convert-an-inputstream-into-a-string-in-java
                ByteArrayOutputStream result = new ByteArrayOutputStream();
                byte[] buffer = new byte[length];
                int size;
                while ((size = i.read(buffer)) != -1) {
                    result.write(buffer, 0, size);
                }
                String body = result.toString("UTF-8");
                int finals = body.indexOf("\r\n\r\n");
                if (finals != -1) {
                    try {
                        int slice = finals + 4;
                        String html = body.substring(slice);
                        Document document = Jsoup.parse(html);
                        String title = document.title();
                        String text = document.text();
                        System.out.println(url);
                        HttpExecuteResponse result_response = search.document_post(title, url, text, search_host, search_index);
                        result_response.responseBody().get().close();
                    } catch (Exception e) {
                        System.out.println("Bad encoding!");
                    }
                }
            } else {
                continue;
            }
        }
        // close to prevent timeouts
        response.responseBody().get().close();
        // delete file to solve testing issue bugs
        file.delete();
    }
}