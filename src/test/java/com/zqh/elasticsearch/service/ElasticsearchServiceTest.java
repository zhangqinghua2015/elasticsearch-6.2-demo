package com.zqh.elasticsearch.service;

import org.junit.Test;

import java.io.IOException;

public class ElasticsearchServiceTest {

    ElasticsearchService elasticsearchService = new ElasticsearchService();

    @org.junit.Test
    public void createIndex() throws IOException {
        elasticsearchService.createIndex("rest_index_create", "v1", "rest_type");
    }

    @Test
    public void openIndex() throws IOException {
        elasticsearchService.openIndex("rest_index_create");
    }

    @Test
    public void closeIndex() throws IOException {
        elasticsearchService.closeIndex("rest_index_create");
    }

    @Test
    public void deleteIndex() throws IOException {
        elasticsearchService.deleteIndex("rest_index_create_v1");
    }

    @Test
    public void getDoc() throws IOException {
        elasticsearchService.getDoc("rest_index_create", "rest_type", "12");
    }

    @Test
    public void addDoc() throws IOException {
        elasticsearchService.addDoc("rest_index_create", "rest_type", "12");
    }

    @Test
    public void updDoc() throws IOException {
        elasticsearchService.updDoc("rest_index_create", "rest_type", "12");
    }

    @Test
    public void delDoc() throws IOException {
        elasticsearchService.delDoc("rest_index_create", "rest_type", "12");
    }

    @Test
    public void bulkAddDoc() throws IOException, InterruptedException {
        elasticsearchService.bulkAddDoc("rest_index_create", "rest_type", "12");
    }

    @Test
    public void searchDocs() throws IOException {
        elasticsearchService.searchDocs("rest_index_create", "title", "title upd");
    }
}
