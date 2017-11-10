import java.util.*;
import java.io.*;
import java.util.regex.Pattern;

public class InvertedIndexBuilderBinary extends IndexBuilder{

    private final VariableByteCode vByte = new VariableByteCode();

    public InvertedIndexBuilderBinary() {}

    void makeLexicon(String fileName) throws IOException {
        int chunkSize = 256;// how many posts for a chunk
        try (FileInputStream fi = new FileInputStream(fileName); FileWriter fol = new FileWriter("Lexicon", true);
             FileOutputStream foi = new FileOutputStream("InvertedList", true);
             FileWriter cfoi = new FileWriter("ChunkTable", true)) {
            InputStreamReader isr = new InputStreamReader(fi);
            BufferedReader fileIn = new BufferedReader(isr, bufferSize);
            BufferedWriter lexicon = new BufferedWriter(fol, bufferSize);
            BufferedOutputStream invert = new BufferedOutputStream(foi, bufferSize);
            BufferedWriter chunk = new BufferedWriter(cfoi, bufferSize);
            String term = null;
            long startBytesInvert = 1;
            long endBytesInvert = 0;
            int countFile = 0;
            int lastId = 0;
            int currId = 0;
            int countChunk = 0;
            int chunkFiles = 0;
            int startChunk = 0;
            while (fileIn.ready()) {
                String currLine = fileIn.readLine();
                String[] post = currLine.split(":");
                currId = Integer.valueOf(post[1]);
                if (!post[0].equals(term)) {
                    if (term != null) {
                        lexicon.write(String.format("%s %d %d %d\n", term, countFile, startChunk, countChunk));
                        chunk.write(String.format("%d %d %d %d\n", countChunk, lastId , startBytesInvert - 1, endBytesInvert - 1));
                        countChunk++;
                        startBytesInvert = endBytesInvert + 1;
                        chunkFiles = 0;
                    }
                    startChunk = countChunk;
                    countFile = 0;
                    lastId = 0;
                    term = post[0];
                }
                byte[] docIDByte = vByte.encodeNumber(currId - lastId);
                lastId = currId;
                byte[] freqByte = vByte.encodeNumber(Integer.valueOf(post[2]));
                invert.write(docIDByte);
                invert.write(freqByte);
                endBytesInvert += docIDByte.length + freqByte.length;
                countFile++;
                chunkFiles++;
                if (chunkFiles == chunkSize) {
                    chunk.write(String.format("%d %d %d %d\n", countChunk, currId , startBytesInvert - 1, endBytesInvert - 1));
                    countChunk++;
                    startBytesInvert = endBytesInvert + 1;
                    chunkFiles = 0;
                }
            }
            lexicon.write(String.format("%s %d %d %d\n", term, countFile, startChunk, countChunk));
            chunk.write(String.format("%d %d %d %d\n", countChunk, currId , startBytesInvert - 1, endBytesInvert - 1));
            lexicon.flush();
            invert.flush();
            chunk.flush();
        }
        String path = System.getProperty("user.dir");
        (new File(path + "/" + fileName)).delete();
    }

}