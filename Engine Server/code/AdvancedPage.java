import java.util.HashMap;
import java.util.Map;

class AdvancedPage extends Page {
    double bm25;
    Map<String, Integer> freqMap = new HashMap<>();
    String snippet;

    AdvancedPage(String pageURL, int pageSize, int wetNum, long startBytes) {
        super(pageURL, pageSize, wetNum, startBytes);
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        result.append("URL: ").append(pageURL).append("\t");
        result.append("Size: ").append(pageSize).append("\t");
        result.append("BM25: ").append(String.format("%.2f", bm25)).append("\n");
        for (Map.Entry<String,Integer> entry : freqMap.entrySet()) {
            result.append("term: ").append(entry.getKey()).append(" ");
            result.append("frequency: ").append(entry.getValue()).append("\t");
        }
        if (freqMap.size() > 0) {
            result.deleteCharAt(result.length() - 1);
        }
        return result.toString();
    }
}