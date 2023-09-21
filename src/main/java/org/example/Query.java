package org.example;




public class Query {

    private String id;

    private String status;


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Query() {
    }

    public Query(String id, String status) {
        this.id = id;
        this.status = status;
    }

    @Override
    public String toString() {
        return "Query{" +
            "name='" + id + '\'' +
            ", status='" + status + '\'' +
            '}';
    }




}
