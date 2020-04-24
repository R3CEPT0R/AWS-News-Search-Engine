package edu.northwestern.ssa;

import jdk.nashorn.internal.parser.JSONParser;
import org.json.JSONObject;
import org.json.JSONString;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpMethod;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

public class ElasticSearch extends AwsSignedRestRequest {
    public ElasticSearch(String service)
    {
        super(service);
    }

    // Sources (exceptions): https://stackoverflow.com/questions/2305966/why-do-i-get-the-unhandled-exception-type-ioexception
    // https://docs.oracle.com/javase/8/docs/api/java/util/Optional.html

    public HttpExecuteResponse index(String host,String path) throws IOException {
        Optional<Map<String, String>> arg = Optional.empty();
        Optional<JSONObject> json = Optional.empty();
        return super.restRequest(SdkHttpMethod.PUT,host,path, arg,json);
    }

    // Following method from AwsSignedRestRequest.java
    public HttpExecuteResponse document_post(String title, String URL, String txt, String host, String path) throws IOException {
        path+="/_doc";
        JSONObject json=new JSONObject();
        Optional<Map<String, String>> x = Optional.empty();
        json.put("title",title);
        json.put("url",URL);
        json.put("txt",txt);
        Optional<JSONObject> y = Optional.of(json);
        return super.restRequest(SdkHttpMethod.POST,host,path,x,y);
    }
}
