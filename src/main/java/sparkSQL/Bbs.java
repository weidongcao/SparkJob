package main.java.sparkSQL;

import java.io.Serializable;

/**
 * Created by jxl on 2016/11/22.
 */
public class Bbs implements Serializable {


    private String id;
    private String sessionid;
    private String url;
    private String name;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getSessionid() {
        return sessionid;
    }

    public void setSessionid(String sessionid) {
        this.sessionid = sessionid;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}



