package co.elastic.clients.elasticsearch_java;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;

import java.io.*;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

class EmployeeDataLoader {

    public static List<Map<String, Object>> loadEmployeeDataFromZip(String zipFilePath) throws IOException {
        List<Map<String, Object>> employeeData = new ArrayList<>();

        try (ZipInputStream zipInputStream = new ZipInputStream(new FileInputStream(zipFilePath))) {
            ZipEntry entry;

            // Iterate through the entries in the ZIP file
            while ((entry = zipInputStream.getNextEntry()) != null) {
                if (entry.getName().endsWith(".csv")) {
                    // Read CSV file
                    try (BufferedReader reader = new BufferedReader(new InputStreamReader(zipInputStream))) {
                        String line;
                        String[] headers = reader.readLine().split(",");  // Read the headers

                        // Read each line and parse the data
                        while ((line = reader.readLine()) != null) {
                            String[] fields = line.split(",");
                            Map<String, Object> employee = new HashMap<>();
                            for (int i = 0; i < headers.length; i++) {
                                employee.put(headers[i], fields[i]);
                            }
                            employeeData.add(employee);
                        }
                    }
                }
            }
        }

        return employeeData;
    }
}

public class ElasticsearchService {

    private ElasticsearchClient client;

    public ElasticsearchService() {
        RestClient restClient = RestClient.builder(new HttpHost("localhost", 9200,"http")).build();
        RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        this.client = new ElasticsearchClient(transport);
    }

    // Create collection (Elasticsearch index)
    public void createCollection(String collectionName) throws IOException {
        CreateIndexRequest request = new CreateIndexRequest.Builder()
            .index(collectionName)
            .build();
        client.indices().create(request);
    }

    // Index employee data into the collection (Elasticsearch index)
    public void indexData(String collectionName, List<Map<String, Object>> employeeData, String excludeColumn) throws IOException {
        for (Map<String, Object> employee : employeeData) {
            // Remove the excluded column
            employee.remove(excludeColumn);

            // Index each employee document
            IndexRequest<Map<String, Object>> request = new IndexRequest.Builder<Map<String, Object>>()
                .index(collectionName)
                .document(employee)
                .build();
            client.index(request);
        }
    }

    // Search employee by column value
    public List<Map<String, Object>> searchByColumn(String collectionName, String columnName, String columnValue) throws IOException {
        SearchRequest request = new SearchRequest.Builder()
            .index(collectionName)
            .query(q -> q
                .match(m -> m
                    .field(columnName)
                    .query(columnValue)
                )
            ).build();
        SearchResponse<Map> response = client.search(request, Map.class);

        List<Map<String, Object>> results = new ArrayList<>();
        for (Hit<Map> hit : response.hits().hits()) {
            results.add(hit.source());
        }
        return results;
    }

    // Get employee count from the collection
    public long getEmpCount(String collectionName) throws IOException {
        CountRequest countRequest = new CountRequest.Builder().index(collectionName).build();
        CountResponse countResponse = client.count(countRequest);
        return countResponse.count();
    }

    // Delete employee by ID
    public void delEmpById(String collectionName, String employeeId) throws IOException {
        DeleteRequest request = new DeleteRequest.Builder()
            .index(collectionName)
            .id(employeeId)
            .build();
        client.delete(request);
    }

    // Get department facet (count of employees grouped by department)
    public Map<String, Long> getDepFacet(String collectionName) throws IOException {
        SearchRequest request = new SearchRequest.Builder()
            .index(collectionName)
            .aggregations("departments", a -> a
                .terms(t -> t.field("Department.keyword"))
            ).build();

        SearchResponse<Map> response = client.search(request, Map.class);
        
        Map<String, Long> departmentCounts = new HashMap<>();
        response.aggregations().get("departments").sterms().buckets().array().forEach(bucket -> {
            departmentCounts.put(bucket.key(), bucket.docCount());
        });
        
        return departmentCounts;
    }

    // Load employee data from the zip file and index it
    public void loadAndIndexDataFromZip(String zipFilePath, String collectionName, String excludeColumn) throws IOException {
        List<Map<String, Object>> employeeData = EmployeeDataLoader.loadEmployeeDataFromZip(zipFilePath);
        indexData(collectionName, employeeData, excludeColumn);
    }

    public static void main(String[] args) throws IOException {
        ElasticsearchService esService = new ElasticsearchService();

        // Step 1: Create collections
        esService.createCollection("Hash_YourName");
        esService.createCollection("Hash_YourPhoneLastFourDigits");

        // Step 2: Load and index data from a zip file
        String zipFilePath = "C:/Users/Dell/Downloads/archive.zip"; // Change this to your actual zip file path
        esService.loadAndIndexDataFromZip(zipFilePath, "Hash_YourName", "Department");
        esService.loadAndIndexDataFromZip(zipFilePath, "Hash_YourPhoneLastFourDigits", "Gender");

        // Step 3: Get employee count
        System.out.println("Employee Count: " + esService.getEmpCount("Hash_YourName"));

        // Step 4: Delete an employee by ID
        esService.delEmpById("Hash_YourName", "E02003");

        // Step 5: Search employees by column
        List<Map<String, Object>> itEmployees = esService.searchByColumn("Hash_YourName", "Department", "IT");
        System.out.println("IT Employees: " + itEmployees);

        // Step 6: Get department facet
        Map<String, Long> depFacet = esService.getDepFacet("Hash_YourName");
        System.out.println("Department Facet: " + depFacet);
    }
}
