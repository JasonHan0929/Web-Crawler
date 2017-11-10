import java.io.*;
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Array;
import java.util.*;

public class Query {

    private final String lexiconName;
    private final String urlTableName;
    private final String chunkTableName;
    private final String invertedListName;
    private final String wetName;
    private int bufferSize = 10 * 1024 * 1024;
    private int resultSize = 10;
    private final double k1 = 1.2;
    private final double b = 0.75;
    private long totalLength = 0;
    private int totalFiles = 0;
    private final List<Chunk> chunkList = new ArrayList<>();
    private final List<AdvancedPage> urlTable = new ArrayList<>();
    private final Map<String, InvertedIndex> lexicon = new HashMap<>();
    private final PriorityQueue<AdvancedPage> topK = new PriorityQueue<>(resultSize, (x, y) -> Double.compare(x.bm25, y.bm25));
    private final Map<Integer, Queue<int[]>> uncompressed = new HashMap<>();
    private final VariableByteCode vByte = new VariableByteCode();

    public Query(String lexiconName, String urlTableName, String chunkTableName, String invertedListName, String wetName) {
        this.lexiconName = lexiconName;
        this.urlTableName = urlTableName;
        this.chunkTableName = chunkTableName;
        this.invertedListName = invertedListName;
        this.wetName = wetName;
    }

    void readChunkList(String fileName) throws IOException {
        try(FileReader fw = new FileReader(fileName)) {
            BufferedReader chunkFile = new BufferedReader(fw, bufferSize);
            while (chunkFile.ready()) {
                String[] words = chunkFile.readLine().split(" ");
                chunkList.add(new Chunk(Long.valueOf(words[2]), Long.valueOf(words[3]), Integer.valueOf(words[1])));
            }
        }
    }

    void readLexicon(String fileName) throws IOException {
        try(FileReader fw = new FileReader(fileName)) {
            BufferedReader lexiconFile = new BufferedReader(fw, bufferSize);
            while (lexiconFile.ready()) {
                String[] words = lexiconFile.readLine().split(" ");
                int countFiles = Integer.valueOf(words[1]);
                totalFiles += countFiles;
                lexicon.put(words[0], new InvertedIndex(countFiles, Integer.valueOf(words[2]), Integer.valueOf(words[3])));
            }
        }
    }

    void readURLTable(String fileName) throws IOException {
        urlTable.add(new AdvancedPage("", -1, -1, -1));
        try(FileReader fw = new FileReader(fileName)) {
            BufferedReader urlFile = new BufferedReader(fw, bufferSize);
            while (urlFile.ready()) {
                String[] words = urlFile.readLine().split(" ");
                int fileLength = Integer.valueOf(words[2]);
                totalLength += fileLength;
                urlTable.add(new AdvancedPage(words[1], fileLength, Integer.valueOf(words[3]), Long.valueOf(words[4])));
            }
        }
        totalFiles = urlTable.size();
    }

    double calculateK(int pageLength) {
        return k1 * ((1 - b) + b * pageLength / totalLength * totalFiles);
    }

    double calculateBM25(int containsTerm, int freq, int pageLength) {
        double K = calculateK(pageLength);
        double part1 = (totalFiles - containsTerm + 0.5) / (containsTerm + 0.5);
        double part2 = (k1 + 1) * freq / (K + freq);
        return Math.log(part1 * part2);
    }

    int[] nextGEQ(int chunkId, int k, String term, RandomAccessFile raf) throws IOException {
        Queue<int[]> uncompressedInvert = null;
        if (uncompressed.containsKey(chunkId)) {
            uncompressedInvert = uncompressed.get(chunkId);
        } else {
            long start = chunkList.get(chunkId).startByte;
            long end = chunkList.get(chunkId).endByte;
            byte[] chunkByte = new byte[(int)(end - start + 1)];
            raf.seek(start);
            raf.read(chunkByte);
            uncompressedInvert = vByte.decodeChunk(chunkByte);
            uncompressed.put(chunkId, uncompressedInvert);
            if (uncompressed.containsKey(chunkId - 1) && lexicon.get(term).startChunk >= chunkId - 1) {
                uncompressed.remove(chunkId - 1);
            }
        }
        while (!uncompressedInvert.isEmpty()) {
            int currId = uncompressedInvert.peek()[0];
            if (currId >= k) {
                return uncompressedInvert.peek();
            } else { // should keep in queue when >=
                uncompressedInvert.poll();
            }
        }
        uncompressed.remove(chunkId); // remove uncompressed list when used up
        return null;
    }

    void combinationFreqMap(Map<String, Integer> page, Map<String, Integer> temp) {
        for (Map.Entry<String, Integer> entry : temp.entrySet()) {
            if (page.containsKey(entry.getKey())) {
                page.put(entry.getKey(), entry.getValue() + page.get((entry.getKey())));
            } else {
                page.put(entry.getKey(), entry.getValue());
            }
        }
    }

    Set<AdvancedPage> addQuery(String[] terms) throws IOException {
        Set<AdvancedPage> result = new HashSet<>();
        try (RandomAccessFile raf = new RandomAccessFile(invertedListName, "r")) {
            for (String term : terms) {
                if (!lexicon.containsKey((term))) {
                    return result;
                }
            }
            int length = terms.length;
            if (length == 0) {
                return result;
            }
            int[] currChunk = new int[length];
            Arrays.sort(terms, (x, y) -> Integer.compare(lexicon.get(x).countFiles, lexicon.get(y).countFiles));
            for (int i = 0; i < length; i++) {
                currChunk[i] = lexicon.get(terms[i]).startChunk;
            }
            int did = 0;
            double bm25 = 0;
            Map<String, Integer> freqMap = new HashMap<>();
            while (did <= totalFiles) {
                for (int i = 0; i < length; i++) {
                    while (currChunk[i] <= lexicon.get(terms[i]).endChunk &&
                            did > chunkList.get(currChunk[i]).lastId) {
                        currChunk[i] += 1;
                    }
                    if (currChunk[i] > lexicon.get(terms[i]).endChunk) {
                        return result;
                    }
                    int[] docInfo = nextGEQ(currChunk[i], did, terms[i], raf);
                    if (docInfo == null) {
                        return result;
                    } else if (docInfo[0] != did) {
                        did = docInfo[0];
                        bm25 = 0;
                        freqMap.clear();
                        break;
                    } else {
                        freqMap.put(terms[i], docInfo[1]);
                        bm25 += calculateBM25(lexicon.get(terms[i]).countFiles, docInfo[1], urlTable.get(i).pageSize);
                    }
                }
                if (bm25 != 0) {
                    AdvancedPage currPage = urlTable.get(did);
                    currPage.bm25 += bm25;
                    combinationFreqMap(currPage.freqMap, freqMap);
                    result.add(currPage);
                    did++;
                }
            }
        }
        return result;
    }

    void orQuery(Set<AdvancedPage> candidates) throws IOException {
        int length = candidates.size();
        if (length <= 0) {
            return;
        }
        for (AdvancedPage curr : candidates) {
            if (topK.size() < resultSize) {
                topK.offer(curr);
            } else if (curr.bm25 > topK.peek().bm25) {
                AdvancedPage last = topK.poll();
                last.bm25 = 0;
                last.freqMap.clear();
            } else {
                curr.bm25 = 0;
                curr.freqMap.clear();
            }
        }
    }

    void combinationQuery(List<String[]> query) throws IOException { // input is [[addQuery1],[addQuery2]....]
        if (query.size() <= 0) {
            return;
        }
        Set<AdvancedPage> candidates = new HashSet<>();
        for (String[] terms : query) {
            candidates.addAll(addQuery(terms));
        }
        orQuery(candidates);
        for (AdvancedPage page : topK) {
            createSnippet(page, query);
        }
    }

    void afterQuery() {
        for (AdvancedPage page : topK) {
            page.bm25 = 0;
            page.freqMap.clear();
            page.snippet = null;
        }
        uncompressed.clear();
        topK.clear();
    }


    List<String[]> parseQuery(String input) {
        String[] segments = input.trim().split("\\|");
        List<String[]> result = new ArrayList<>(segments.length);
        for (String segment : segments) {
            segment = segment.trim();
            if (segment.length() > 0) {
                result.add(segment.split("[&\\s]"));
            }
        }
        return result;
    }

    void createSnippet(AdvancedPage page, List<String[]> terms) throws IOException {
        StringBuilder content = new StringBuilder();
        String path = String.format(wetName, (page.wetNum > 9 ? "" : "0") + page.wetNum);
        try (RandomAccessFile raf = new RandomAccessFile(path, "r")) {
            FileReader fr = new FileReader(raf.getFD());
            raf.seek(page.startBytes);
            BufferedReader wet = new BufferedReader(fr);
            while (wet.ready()) {
                String currLine = wet.readLine();
                if (currLine.startsWith("WARC")) {
                    break;
                } else if (currLine.length() > 0) {
                    content.append(currLine);
                }
            }
        }
        String contents = " " + content.toString().replaceAll("[-|:/,._]", " ") + " ";
        StringBuilder result = new StringBuilder();
        Set<String> termSet = new HashSet<>();
        for (String[] array : terms) {
            for (String term : array) {
                if (termSet.contains(term)) {
                    continue;
                } else {
                    termSet.add(term);
                }
                int index = contents.indexOf(" " + term + " ");
                if (index == -1) {
                    continue;
                }
                String snippet = content.substring(Math.max(0, index - 64), Math.min(content.length(), index + 64));
                result.append("\t" + term + ": ").append(snippet).append("\n");
            }
        }
        if (result.length() > 0) {
            result.deleteCharAt(result.length() - 1);
        }
        page.snippet = result.toString();
    }

    String output(long startTime, String input) {
        StringBuilder result = new StringBuilder();
        List<AdvancedPage> sorted = new ArrayList<>(topK);
        sorted.sort((x, y) -> Double.compare(y.bm25, x.bm25));
        for (AdvancedPage page : sorted) {
            result.append(page.toString()).append("\n");
            result.append("Snippet:\n").append(page.snippet).append("\n\n");
        }
        if (result.length() > 0) {
            result.deleteCharAt(result.length() - 1);
        }
        long endTime = new Date().getTime();
        result.append("\nQuery Input: ").append(input).append("    ");
        result.append("Number of Results: ").append(topK.size()).append("    ");
        result.append("Time Consumed: ").append(endTime - startTime).append("ms");
        result.append("\n===========================================================================\n\n");
        afterQuery();
        return result.toString();
    }

    public static void main(String[] args) throws IOException {
        String path = "../index/";
        String wetName = "../wet/CC-MAIN-20170919112242-20170919132242-000%s.warc.wet";
        if (args.length > 1) {
            path = args[0];
            wetName = args[1] += "CC-MAIN-20170919112242-20170919132242-000%s.warc.wet";
        }
        Query test = new Query(path + "Lexicon", path + "URLTable", path + "ChunkTable", path + "InvertedList", wetName);
        System.out.println("Initializing ChunkTable");
        test.readChunkList(test.chunkTableName);
        System.out.println("Initializing Lexicon");
        test.readLexicon(test.lexiconName);
        System.out.println("Initializing URLTable");
        test.readURLTable(test.urlTableName);
        System.out.println("Finished Initialization, please input any query:");
        Scanner scanner = new Scanner(System.in);
        while (true) {
            String input = scanner.nextLine();
            if (input.equals("q!")) {
                break;
            }
            long start = new Date().getTime();
            test.combinationQuery(test.parseQuery(input));
            System.out.print(test.output(start, input));
        }
    }
}
