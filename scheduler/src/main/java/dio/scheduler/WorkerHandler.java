package dio.scheduler;

/**
 * Created by IgorPc on 6/7/2019.
 */

import dio.datamodel.PairCouple;
import org.nustaq.serialization.FSTConfiguration;
import org.nustaq.serialization.FSTObjectInput;

import java.io.*;

import java.util.List;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;

public class WorkerHandler implements Runnable {
    private InputStream inputStream;
    private Process process;
    private BlockingQueue<List<PairCouple>> buffer;
    private InputStream errorStream;
    private String id;

    private class ErrorStreamListener implements Runnable {
        private InputStream err;
        private Process process;

        public ErrorStreamListener(InputStream err, Process process) {
            this.err = err;
            this.process = process;
        }

        @Override
        public void run() {
            try {
                while (true) {

                    if (err.available() == 0) {
                        if (!process.isAlive()) {
                            break;
                        }

                    }
                    if(err.available()>0){
                        readFromInfoData();
                    }
                    Thread.sleep(1000);
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
                private void readFromInfoData () throws IOException {
                    BufferedReader br = new BufferedReader(new InputStreamReader(errorStream));
                    StringBuilder sb = new StringBuilder();
                    sb.append("Worker-").append(id).append(": ");
                    sb.append(br.readLine());
                    System.out.println(sb.toString());
                }

    }


    public WorkerHandler(Process process, BlockingQueue<List<PairCouple>> pairCouples,String id) {
        this.errorStream=process.getErrorStream();
        this.inputStream = process.getInputStream();
        this.process = process;
        this.buffer = pairCouples;
        this.id=id;
    }

    public void run() {
        new Thread(new WorkerHandler.ErrorStreamListener(errorStream,process)).start();
        try {
            Scanner sc=new Scanner(inputStream);
            sc.nextLine();
            sc.nextLine();
            sc.nextLine();

            FSTConfiguration conf = FSTConfiguration.createDefaultConfiguration();
            FSTObjectInput in = new FSTObjectInput(inputStream);
            while (true) {
                if (in.available() == 0) {
                    if (!process.isAlive()) {
                        break;
                    }
                    Thread.sleep(100);
                    continue;
                }
                int size = in.readInt();
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                byte[] arr = new byte[2048];

                while (size > arr.length) {
                    int read = in.read(arr);
                    outputStream.write(arr, 0, read);
                    size -= read;
                }
                if (size != 0) {
                    in.read(arr, 0, size);
                    outputStream.write(arr, 0, size);
                }
                if (outputStream.size() > 0) {
                    List<PairCouple> list = (List<PairCouple>) conf.asObject(outputStream.toByteArray());
                    System.out.println(list.size());
                    buffer.put(list);
                    sc.nextLine();
                }
            }
            in.close(); // required !
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /*private void readFromInfoData() throws IOException {
        BufferedReader br=new BufferedReader(new InputStreamReader(errorStream));
        StringBuilder sb=new StringBuilder();
        sb.append("Worker-").append(id).append(": ");
            sb.append(br.readLine());
        *//*
        byte[] arr = new byte[2048];
        ByteArrayOutputStream baos=new ByteArrayOutputStream();
        int tmp=0;
        while ((tmp=errorStream.read(arr))>0) {
            baos.write(arr,0,tmp);
        }
        StringBuilder sb=new StringBuilder();
        sb.append("Worker-").append(id).append(": ").append(new String(baos.toByteArray(),"UTF-8"));*//*
        System.out.println(sb.toString());
    }*/

}
