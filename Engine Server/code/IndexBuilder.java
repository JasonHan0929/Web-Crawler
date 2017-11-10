import java.util.*;
import java.io.*;
import java.util.regex.*;

public abstract class IndexBuilder {
    private int tempFileCount = 0;
    protected final int  bufferSize = 10 * 1024 * 1024;
    private int docID = 1; // 0 will cause problem for uncompress
    private long startByteURL = 0;
    private final HashMap<String, Integer> map = new HashMap<>();

    abstract void makeLexicon(String fileName) throws IOException;

    Page parseHead(BufferedReader fileIn, int wetNum) throws IOException{
        int pageSize = 0;
        String pageURL = null;
        while (fileIn.ready()) {
            String currLine = fileIn.readLine();
            startByteURL += currLine.getBytes().length + 1; // +1 for "\n"
            if (currLine.startsWith("WARC-Target-URI"))
                pageURL = currLine.split(" ")[1];
            else if (!currLine.startsWith("Content-Type: text/plain"))
                continue;
            while (fileIn.ready()) {
                currLine = fileIn.readLine();
                startByteURL += currLine.getBytes().length + 1;
                if (currLine.startsWith("Content-Length"))
                    pageSize = Integer.valueOf(currLine.split(" ")[1]);
                else if (currLine.equals("")) {
                    return new Page(pageURL, pageSize, wetNum, startByteURL);
                }
            }
        }
        return null;
    }

    void parseLine(HashMap<String, Integer> map, String currLine) {
        String[] segments = currLine.split("[\\s-|:/,._]");
        String pattern = "[\\w]{1,}";
        int wordLength = 128;
        for (String word : segments) {
            word = word.trim();
            if (Pattern.matches(pattern, word) && word.length() <= wordLength) {
                if (map.containsKey(word)) {
                    map.put(word, map.get(word) + 1);
                } else {
                    map.put(word, 1);
                }
            }
        }
    }

    boolean makePosts(BufferedReader fileIn, BufferedWriter fileOut) throws IOException {
        map.clear();
        while (fileIn.ready()) {
            String currLine = fileIn.readLine();
            startByteURL += currLine.getBytes().length + 1;
            if (currLine.startsWith("WARC")) {
                break;
            } else if (currLine.length() > 0){
                parseLine(map, currLine);
            }
        }
        if (map.size() == 0)
            return false;
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            fileOut.write(String.format("%s:%d:%d", entry.getKey(), docID, entry.getValue()));
            fileOut.newLine();
        }
        return true;
    }

    void makeUrlTable(BufferedWriter fo, Page currPage) throws IOException {
        fo.write(String.format("%d %s %d %d %d\n", docID, currPage.pageURL, currPage.pageSize, currPage.wetNum, currPage.startBytes));
    }

    void buildIndex(String fileName, int wetNum) throws Exception {
        try (FileInputStream fi = new FileInputStream(fileName); FileWriter fo = new FileWriter("temp" + tempFileCount, true);
             FileWriter url = new FileWriter("URLTable", true)) {
            InputStreamReader isr = new InputStreamReader(fi);
            BufferedReader fileIn = new BufferedReader(isr, bufferSize);
            BufferedWriter fileOut = new BufferedWriter(fo, bufferSize);
            BufferedWriter urlData = new BufferedWriter(url, bufferSize);
            while (fileIn.ready()) {
                Page currPage = parseHead(fileIn, wetNum);
                if (makePosts(fileIn, fileOut) && currPage != null) {
                    System.out.println(docID + " " + currPage);
                    makeUrlTable(urlData, currPage);
                    docID++;
                }
            }
            fileOut.flush();
            urlData.flush();
        }
        String path = System.getProperty("user.dir");
        System.out.println("Sorting current file's posts");
        Process process = new ProcessBuilder("/bin/bash", "-c", String.format("sort -k 1d,1 -k 2n,2 -t : %s/temp%d -o %s/temp%d", path, tempFileCount, path, tempFileCount)).start();
        process.waitFor();
        tempFileCount++;
    }

    void mergeFile() throws Exception {
        String path = System.getProperty("user.dir");
        String cmd = "sort -k 1d,1 -k 2n,2 -t : -m";
        int count = tempFileCount - 1;
        while (count >= 0) {
            cmd += String.format(" %s/temp%d", path, count);
            count--;
        }
        cmd += " >> merged";
        Process process = new ProcessBuilder("/bin/bash", "-c", cmd).start();
        process.waitFor();
        count = tempFileCount - 1;
        while (count >= 0) {
            (new File(path + "/temp" + count)).delete();
            count--;
        }
    }

    String timeCalculator (long difference) {
        int hour = (int)(difference / (1000 * 3600));
        difference -= hour * 1000 * 3600;
        int minute = (int)(difference / (1000 * 60));
        difference -= minute * 1000 * 60;
        int second = (int)(difference / 1000);
        return String.format("Time Used: %dh %dm %ds", hour, minute, second);
    }

    String sizeCalculator (String... files) {
        long size = 0;
        for (String file : files) {
            size += new File("./" + file).length();
        }
        return String.format("Total Size: %.2fMB", size / (double)(1024 * 1024));
    }

    public static void main(String[] args) throws Exception {
        IndexBuilder builder = null;
        if (args[0].equals("ascii")) {
            builder = new InvertedIndexBuilder();
        } else if (args[0].equals("binary")) {
            builder = new InvertedIndexBuilderBinary();
        } else {
            System.out.println("Wrong Parameters!");
            return;
        }
        String path = args[1];
        File file = new File(path);
        long startTime = new Date().getTime();
        int wetNum = 0;
        String[] files = file.list();
        Arrays.sort(files);
        for (String fileName : files) {
            builder.startByteURL = 0;
            if (fileName.startsWith("CC-MAIN-201709")) {
                System.out.println("Pasring File: " + fileName);
                builder.buildIndex(path + "/" + fileName, wetNum);
                wetNum++;
            }
        }
        System.out.println("Merging temp files");
        builder.mergeFile();
        System.out.println("Building final lexicon");
        builder.makeLexicon("merged");
        System.out.println("\nAll done\n");
        long endTime = new Date().getTime();
        System.out.println(builder.timeCalculator(endTime - startTime));
        if (args[0].equals("ascii")) {
            System.out.println(builder.sizeCalculator("InvertedList", "Lexicon", "URLTable"));
        } else if (args[0].equals("binary")) {
            System.out.println(builder.sizeCalculator("InvertedList", "Lexicon", "URLTable", "ChunkTable"));
        }
    }

}