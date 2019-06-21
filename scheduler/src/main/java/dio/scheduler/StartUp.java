package dio.scheduler;

import dio.datamodel.PairCouple;

import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by IgorPc on 6/4/2019.
 */
public class StartUp {
    public static long time;
    public static void main(String[] args) throws InterruptedException,
            IOException, SQLException {
        time=System.currentTimeMillis();
        int quantityOfWorkers = Integer.valueOf(args[0]);
        String pathToDb = args[1];
        String pathToJar = args[2];
        String pathToResult = args[3];
        double resultPercent = Double.valueOf(args[4]);
        long quantityOfRows = getQuantity(pathToDb);

        double endPoint = round(getFirstPoint(quantityOfWorkers)/100,3);
        double startPoint = 0;

        BlockingQueue<List<PairCouple>> pairCouples = new ArrayBlockingQueue<>(quantityOfWorkers+1);

        ExecutorService taskExecutor = Executors.newFixedThreadPool(quantityOfWorkers);

        int q=1;

        int id=0;
        while (endPoint<=1) {
            /*String fPath="script"+id+".sh";
            PrintWriter writer = new PrintWriter(fPath, "UTF-8");
            writer.println("#!/bin/bash");
            String command = String.format("java -jar %s %s %s %s %s", pathToJar, String.valueOf(endPoint), String.valueOf(startPoint),
                    String.valueOf(quantityOfRows), pathToDb);
            writer.println(command);
            writer.close();

            File f=new File(fPath);
            f.setReadable(true, true);
            f.setWritable(true, true);
            f.setExecutable(true);

            ProcessBuilder pb = new ProcessBuilder( "qsub","-I","-x","./"+fPath);*/
            ProcessBuilder pb=new ProcessBuilder("java","-jar",
                    pathToJar, String.valueOf(endPoint), String.valueOf(startPoint),
                    String.valueOf(quantityOfRows), pathToDb);
            Process process = pb.start();
            taskExecutor.execute(new WorkerHandler(process, pairCouples,String.valueOf(id)));
            startPoint+=endPoint;
            endPoint*=2;
            id++;
        }
        Thread.sleep(100);
        taskExecutor.shutdown();

        Thread thread = new Thread(new AggregationDbHandler(resultPercent, pairCouples,pathToResult));
        thread.start();

        taskExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

        List<PairCouple> POISON_PILL=new ArrayList<>();
        PairCouple poison = new PairCouple();
        poison.setCount(Integer.MIN_VALUE);
        POISON_PILL.add(poison);

        pairCouples.add(POISON_PILL);

    }
    public static long getQuantity(String pathToDb) throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:sqlite:"+pathToDb);
        ResultSet resultSet = connection.createStatement().executeQuery("select count(*) from reference");

        resultSet.next();
       return resultSet.getLong(1);
    }
    public static double getFirstPoint(int quantityOfWorkers) {
       return 100/((1-Math.pow(2,quantityOfWorkers))*-1);
    }
    public static double round(double value, int places) {
        if (places < 0) throw new IllegalArgumentException();

        BigDecimal bd = new BigDecimal(value);
        bd = bd.setScale(places, RoundingMode.HALF_UP);
        return bd.doubleValue();
    }


}
