import java.nio.Buffer;
import java.util.*;
import java.io.*;

public class Test {
    public static void main(String[] args) throws Exception {
        String file = "/home/jason/Documents/Course/Web Search Engine/Assignment 2/real/CC-MAIN-20170919112242-20170919132242-00000.warc.wet";
        System.out.println(createSnippet(164219084, file));
    }

    static void read (long num, String file) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(file, "r");
        FileReader fr = new FileReader(raf.getFD());
        raf.seek(num);
        BufferedReader bf = new BufferedReader(fr);
        String line = bf.readLine();
        System.out.println(line);
    }

    static void read2(long num, String file) throws IOException {
        FileReader fr = new FileReader(file);
        BufferedReader bf = new BufferedReader(fr);
        bf.skip(num);
        System.out.println(bf.readLine());
    }

    static void test(String file) throws IOException{
        FileReader fr = new FileReader(file);
        BufferedReader buffer = new BufferedReader(fr);
        Scanner scanner = new Scanner(System.in);
        long bytes = 0;
        while(scanner.nextInt() == 1) {
            String line = buffer.readLine();
            int temp =line.getBytes().length;
            bytes += temp + 2;
            System.out.println(line + " " + temp + " " + bytes);
        }
    }

    static void test2(String file) throws IOException{
        FileReader fr = new FileReader(file);
        BufferedReader buffer = new BufferedReader(fr);
        Scanner scanner = new Scanner(System.in);
        long chrs = 0;
        while(scanner.nextInt() == 1) {
            String line = buffer.readLine();
            int temp =line.length();
            chrs += temp + 2;
            System.out.println(line + " " + temp + " " + chrs);
        }
    }

    static String createSnippet(long num, String file) throws IOException {
        StringBuilder content = new StringBuilder();
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            FileReader fr = new FileReader(raf.getFD());
            raf.seek(num);
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
        return content.toString();
    }
}
