package dio.scheduler;

        import dio.datamodel.PairCouple;

        import java.sql.*;
        import java.util.*;
        import java.util.concurrent.BlockingQueue;
        import java.util.concurrent.TimeUnit;

/**
 * Created by IgorPc on 6/7/2019.
 */
public class AggregationDbHandler implements Runnable {
    private double percent;
    private BlockingQueue<List<PairCouple>> buffer;
    private String pathToResult;

    public AggregationDbHandler(double percent, BlockingQueue<List<PairCouple>> buffer, String pathToResult) {
        this.percent=percent;
        this.buffer = buffer;
        this.pathToResult = pathToResult;
    }

    @Override
    public void run() {
        Map<PairCouple,Integer> pairs=new HashMap<>();
        try {
            Connection connection = DriverManager.getConnection("jdbc:sqlite:" + pathToResult);

            boolean check = checkTableExistance(connection);

            if(check) {
                Statement create = connection.createStatement();
                create.execute("CREATE TABLE doublePair(\n" +
                        "   FIRST           TEXT    NOT NULL,\n" +
                        "   SECOND          TEXT    NOT NULL,\n" +
                        "   QUANTITY        INT     NOT NULL\n" +
                        ");");
                create.execute("CREATE TABLE tripplePair(\n" +
                        "   FIRST           TEXT    NOT NULL,\n" +
                        "   SECOND          TEXT    NOT NULL,\n" +
                        "   THIRD          TEXT    NOT NULL,\n" +
                        "   QUANTITY        INT     NOT NULL\n" +
                        ");");
            }
            Statement statement = connection.createStatement();

            while (true) {
                    List<PairCouple> poll = buffer.poll(1000, TimeUnit.MILLISECONDS);
                    if (poll==null || poll.isEmpty()) continue;
                    if(poll.get(0).getCount()==Integer.MIN_VALUE){
                        break;
                    }
                System.out.println("Received");
                for (PairCouple pairCouple : poll) {
                        pairs.merge(pairCouple,pairCouple.getCount(),(t1, t2) -> pairCouple.getCount() + t2);
                }

            }

            List<PairCouple> result = new ArrayList<>(pairs.keySet());

            result.sort(new PairCouple.CountComparator());
            connection.setAutoCommit(false);
            int quantity=(int)(result.size()*percent);
            for(int i=0;i<quantity;i++){
                PairCouple poll=result.get(i);
                if (poll.getWords().length == 2) {
                    String sql = String.format("INSERT INTO doublePair(FIRST,SECOND,QUANTITY) VALUES('%s','%s',%d);", poll.getWords()[0], poll.getWords()[1], poll.getCount());
                    statement.execute(sql);
                }
                if (poll.getWords().length == 3) {
                    String sql = String.format("INSERT INTO tripplePair(FIRST,SECOND,THIRD,QUANTITY) VALUES('%s','%s','%s',%d);", poll.getWords()[0], poll.getWords()[1],poll.getWords()[2], poll.getCount());
                    statement.execute(sql);
                }
            }


            connection.commit();
            connection.close();
        }catch (Exception ex){
            ex.printStackTrace();
        }
        System.out.println(System.currentTimeMillis()-StartUp.time);
    }

    private boolean checkTableExistance(Connection connection) throws SQLException {
        DatabaseMetaData dbm= connection.getMetaData();
        ResultSet tables = dbm.getTables(null, null, "%Pair", null);


        if(tables.next()){
            Statement clean= connection.createStatement();
            clean.execute("delete from doublePair");
            clean.execute("delete from tripplePair");
            return false;
        }
        return true;
    }
}
