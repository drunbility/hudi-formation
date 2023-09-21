package org.example;

import cn.hutool.http.HttpUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class AppTest extends TestCase {

    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(AppTest.class);
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp() throws JsonProcessingException {


        Query qr= new Query("app","success");

        ObjectMapper mapper = new ObjectMapper();

        String s = mapper.writeValueAsString(qr);

        Query query = mapper.readValue(s, Query.class);


        System.out.println(s);

        System.out.println(query.getId());



    }

    public void testQuery() throws JsonProcessingException {

        Query query = new Query();
        query.setId("appId");
        query.setStatus("success");

        ObjectMapper mapper = new ObjectMapper();

        String str = mapper.writeValueAsString(query);

        HttpUtil.post("http://localhost:7070/api/v1/querys", str);

    }




}
