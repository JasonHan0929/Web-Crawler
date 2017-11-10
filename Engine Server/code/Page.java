import java.util.Map;

class Page {
    final String pageURL;
    final int pageSize;
    final int wetNum;
    final long startBytes;

    Page(String pageURL, int pageSize, int wetNum, long startBytes) {
        this.pageSize = pageSize;
        this.pageURL = pageURL;
        this.wetNum = wetNum;
        this.startBytes = startBytes;
    }

    @Override
    public String toString() {
        return String.format("%s %d %d %d", pageURL, pageSize, wetNum, startBytes);
    }
}