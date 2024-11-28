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

// Classe principal que implementa a interface RequestHandler para processar requisições Lambda
public class Main implements RequestHandler<Map<String, Object>, Map<String, Object>> {

    // Cliente S3 para interagir com o serviço AWS S3
    private final S3Client s3Client = S3Client.builder().build();
    // Objeto para conversão de JSON para objetos Java
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Map<String, Object> handleRequest(Map<String, Object> input, Context context) {
        // Log do evento recebido pela função Lambda
        System.out.println("Evento recebido pelo Lambda: " + input);

        // Obter os parâmetros do caminho da URL
        Map<String, String> pathParameters = (Map<String, String>) input.get("pathParameters");
        if (pathParameters == null || !pathParameters.containsKey("urlCode")) {
            throw new IllegalArgumentException("urlCode is missing in pathParameters");
        }

        // Recuperar o código curto da URL a partir dos parâmetros
        String shortUrlCode = pathParameters.get("urlCode");
        if (shortUrlCode.isEmpty()) {
            throw new IllegalArgumentException("Invalid input: 'urlCode' is required");
        }

        System.out.println("Código encurtado recebido: " + shortUrlCode);

        // Configurar a requisição para buscar o objeto no S3
        System.out.println("Attempting to fetch key: " + shortUrlCode + ".json");
        System.out.println("Bucket: YOUR-S3-BUCKET-NAME");

        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket("YOUR-S3-BUCKET-NAME") // Substitua pelo nome real do seu bucket
                .key(shortUrlCode + ".json")  // O nome do arquivo JSON no S3
                .build();

        // Recuperar o objeto do S3 e desserializá-lo em um objeto Java
        OriginalUrlData originalUrl;
        try (InputStream s3ObjectStream = s3Client.getObject(getObjectRequest)) {
            originalUrl = objectMapper.readValue(s3ObjectStream, OriginalUrlData.class);
        } catch (NoSuchKeyException e) {
            // Se o arquivo não for encontrado no S3, retornar um erro 404
            System.err.println("Error: Key not found - " + getObjectRequest.key());
            return buildResponse(404, "The specified URL code does not exist.");
        } catch (S3Exception e) {
            // Log de erro relacionado ao S3
            System.err.println("S3 Error: " + e.awsErrorDetails().errorMessage());
            throw new RuntimeException("S3 Error: " + e.awsErrorDetails().errorMessage(), e);
        } catch (Exception e) {
            // Tratamento genérico para erros de leitura ou desserialização
            throw new RuntimeException("Error reading or deserializing S3 object", e);
        }

        // Verificar se a URL encurtada expirou
        long currentTimeSeconds = System.currentTimeMillis() / 1000;
        if (originalUrl.getExpirationTime() < currentTimeSeconds) {
            return buildResponse(410, "This URL has expired.");
        }

        // Retornar um redirecionamento para a URL original
        Map<String, Object> headers = new HashMap<>();
        headers.put("Location", originalUrl.getOriginalUrl()); // Adiciona a URL original ao cabeçalho de redirecionamento

        return buildResponse(301, headers); // Retorna o status HTTP 301 (Redirecionamento permanente)
    }

    // Método auxiliar para construir uma resposta HTTP com status e corpo
    private Map<String, Object> buildResponse(int statusCode, Object body) {
        Map<String, Object> response = new HashMap<>();
        response.put("statusCode", statusCode);
        if (body instanceof String) {
            response.put("body", body); // Adiciona o corpo da resposta se for uma string
        } else if (body instanceof Map) {
            response.put("headers", body); // Adiciona os cabeçalhos da resposta se for um mapa
        }
        return response;
    }
}
