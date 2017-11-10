import java.util.*;
import java.io.*;
import java.util.regex.Pattern;

public class InvertedIndexBuilder extends IndexBuilder {

    public InvertedIndexBuilder() {}

    void makeLexicon(String fileName) throws IOException {
        try (FileInputStream fi = new FileInputStream(fileName); FileWriter fol = new FileWriter("Lexicon", true); FileWriter foi = new FileWriter("InvertedList", true)) {
            InputStreamReader isr = new InputStreamReader(fi);
            BufferedReader fileIn = new BufferedReader(isr, bufferSize);
            BufferedWriter fileOut = new BufferedWriter(fol, bufferSize);
            BufferedWriter result = new BufferedWriter(foi, bufferSize);
            String term = null;
            long startBytes = 0;
            long endBytes = 0;
            long countFile = 0;
            while (fileIn.ready()) {
                String currLine = fileIn.readLine();
                String[] post = currLine.split(":");
                if (!post[0].equals(term)) {
                    if (term != null) {
                        fileOut.write(String.format("%s %d %d %d\n", term, countFile, startBytes - 1, endBytes - 1));
                    }
                    startBytes = endBytes + 1;
                    countFile = 0;
                    term = post[0];
                }
                String newLine = String.format("%s %s\n", post[1], post[2]);
                result.write(newLine);
                endBytes += newLine.getBytes().length;
                countFile++;
            }
            fileOut.write(String.format("%s %d %d %d\n", term, countFile, startBytes - 1, endBytes - 1));
            fileOut.flush();
            result.flush();
        }
        String path = System.getProperty("user.dir");
        (new File(path + "/output/" + fileName)).delete();
    }

}