package cn.tim.flink.transformation;

public class AccessLog {
    private Long time;

    private String domain;

    private Double traffic;

    public AccessLog(){

    }

    public AccessLog(Long time, String domain, Double traffic) {
        this.time = time;
        this.domain = domain;
        this.traffic = traffic;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public Double getTraffic() {
        return traffic;
    }

    public void setTraffic(Double traffic) {
        this.traffic = traffic;
    }

    @Override
    public String toString() {
        return "AccessLog{" +
                "time=" + time +
                ", domain='" + domain + '\'' +
                ", traffic=" + traffic +
                '}';
    }
}
