package dev.otavioximarelli;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class Main implements RequestHandler<Map<String, Object>, Map<String, Object>> {

    private final S3Client s3Client = S3Client.builder().build();
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Map<String, Object> handleRequest(Map<String, Object> input, Context context) {
        //GET THE URL SHORT CODE FROM THE JSON
        System.out.println("Evento recebido pelo Lam   bda: " + input);

        // Obter os parâmetros do caminho
        Map<String, String> pathParameters = (Map<String, String>) input.get("pathParameters");
        if (pathParameters == null || !pathParameters.containsKey("urlCode")) {
            throw new IllegalArgumentException("urlCode is missing in pathParameters");
        }

        String shortUrlCode = pathParameters.get("urlCode");
        if (shortUrlCode.isEmpty()) {
            throw new IllegalArgumentException("Invalid input: 'urlCode' is required");
        }

        System.out.println("Código encurtado recebido: " + shortUrlCode);

        //SETUP S3 CONNECTION

        System.out.println("Attempting to fetch key: " + shortUrlCode + ".json");
        System.out.println("Bucket: my-bucket-1-zika");


        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket("my-bucket-1-zika")
                .key(shortUrlCode + ".json")
                .build();


        //GET THE S3 OBJECT AND DESERIALIZE
        OriginalUrlData originalUrl;
        try (InputStream s3ObjectStream = s3Client.getObject(getObjectRequest)) {
            originalUrl = objectMapper.readValue(s3ObjectStream, OriginalUrlData.class);
        } catch (NoSuchKeyException e) {
            System.err.println("Error: Key not found - " + getObjectRequest.key());
            return buildResponse(404, "The specified URL code does not exist.");
        } catch (S3Exception e) {
            System.err.println("S3 Error: " + e.awsErrorDetails().errorMessage());
            throw new RuntimeException("S3 Error: " + e.awsErrorDetails().errorMessage(), e);
        } catch (Exception e) {
            throw new RuntimeException("Error reading or deserializing S3 object", e);
        }


        //VERIFY THE EXPIRATION TIME
        long currentTimeSeconds = System.currentTimeMillis() / 1000;
        if (originalUrl.getExpirationTime() < currentTimeSeconds) {
            return buildResponse(410, "This URL has expired.");
        }

        // REDIRECT TO THE ORIGINAL URL
        Map<String, Object> headers = new HashMap<>();
        headers.put("Location", originalUrl.getOriginalUrl());

        return buildResponse(301, headers);

    }

    //RESPONSE METHOD
    private Map<String, Object> buildResponse(int statusCode, Object body) {
        Map<String, Object> response = new HashMap<>();
        response.put("statusCode", statusCode);
        if (body instanceof String) {
            response.put("body", body);
        } else if (body instanceof Map) {
            response.put("headers", body);
        }
        return response;
    }

}