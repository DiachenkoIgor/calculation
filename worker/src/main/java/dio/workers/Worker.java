package dio.workers;

import dio.datamodel.CoupleInfo;
import dio.datamodel.PairCouple;
import dio.datamodel.Reference;
import org.nustaq.serialization.FSTConfiguration;
import org.nustaq.serialization.FSTObjectOutput;
import org.sqlite.SQLiteConfig;


import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;


/**
 * Виконує роль робітника, який знаходить пари і трійки слів, які повторюються в одному контексті.
 */
public class Worker {
    private static String SELECT_QUERY;

    public static void main(String[] args) throws IOException, SQLException {

        double percent=Double.valueOf(args[0]);
        double offset=Double.valueOf(args[1]);

        String all=args[2];
        String pathToDb = args[3];

        System.out.println("TestData");
        System.out.println("TestData");
        System.out.println("TestData");

        System.err.println("Received data: percent -  " + percent + "; offset - " + offset);
        System.err.flush();

        SELECT_QUERY = String.format("select source,context,context_type,target,constructor from reference LIMIT %s OFFSET %s", all, 0);

        List<Reference> references = null;

        try {
            references = fillList(pathToDb);
        } catch (SQLException sq) {
            sq.printStackTrace();
            return;
        }

      System.err.println("Start searching");
        System.err.flush();
        List<List<String>> lists = distinctResultsFiltering(references);


        List<PairCouple> fullResult = pairsSearching(lists,percent,offset);

        System.err.println("Finished for pairs searching");
        System.err.flush();
        FSTConfiguration conf = FSTConfiguration.createDefaultConfiguration();

        byte[] bytesFirst = conf.asByteArray(fullResult);



        FSTObjectOutput fstObjectOutput = new FSTObjectOutput(System.out);
        fstObjectOutput.writeInt(bytesFirst.length);
        fstObjectOutput.write(bytesFirst);
        fstObjectOutput.flush();

        System.out.println("Finished");
        System.out.flush();

    }

    //Створює об'єкти на основі БД
    public static List<Reference> fillList(String pathToDb) throws SQLException {
        SQLiteConfig config = new SQLiteConfig();

        config.setReadOnly(true);
        Connection connection = DriverManager.getConnection("jdbc:sqlite:" + pathToDb,config.toProperties());
        ResultSet resultSet = connection.createStatement().executeQuery(SELECT_QUERY);

        LinkedList<Reference> references = new LinkedList<>();
        while (resultSet.next()) {
            Reference reference = new Reference();
            reference.setSource(resultSet.getString("source"));
            reference.setContext(resultSet.getString("context"));
            reference.setContextType(resultSet.getString("context_type"));
            reference.setTarget(resultSet.getString("target"));
            reference.setConstructor(resultSet.getString("constructor"));
            references.add(reference);
        }
        connection.close();
        return references;
    }
    // Групує контексти і повертає списки унікальних слів в кожному контексті
    public static List<List<String>> distinctResultsFiltering(List<Reference> references) {

        Map<Reference.TopicWords, List<Reference>> groupResult = references.stream()
                .collect(Collectors.groupingBy(Reference::constructTopicWords, Collectors.toList()));

        Map<Reference.TopicWords, List<String>> distinctResult = clearGroupByResult(groupResult);

        return new ArrayList<>(distinctResult.values());
    }

    // Видаляються слова, які повторюються
    public static Map<Reference.TopicWords, List<String>> clearGroupByResult(Map<Reference.TopicWords, List<Reference>> groupResult) {
        Map<Reference.TopicWords, List<String>> distinctResult = new HashMap<>();

        for (Map.Entry<Reference.TopicWords, List<Reference>> topicWordsListEntry : groupResult.entrySet()) {
            Reference.TopicWords key = topicWordsListEntry.getKey();
            Set<String> collect = topicWordsListEntry.getValue().stream().map(Reference::getConstructor).collect(Collectors.toSet());
            distinctResult.put(key, new ArrayList<>(collect));
            if (collect.size() < 2) {
                distinctResult.remove(key);
            }
        }
        return distinctResult;
    }

    //Формує пари на основі перетину множин і з результату формує всіх можливі пари
    public static List<PairCouple> pairsSearching(List<List<String>> lists, double percent,double offset) {
        int start=(int)(lists.size()*offset);
        int end=(int)(lists.size()*percent)+start;
        int mid=end-start/2;
        Map<PairCouple, CoupleInfo> pairs = new HashMap<>();
        for (int i =start; i < end && i<lists.size(); i++) {
            if(i==mid){
                System.err.println("Done for 50%");
            }
            for (int j = i + 1; j < lists.size(); j++) {
                List<String> result = lists.get(i).stream()
                        .filter(lists.get(j)::contains)
                        .sorted()
                        .collect(Collectors.toList());


                if (result.size() > 1) {

                    for (int k = 0; k < result.size(); k++) {
                        for (int l = k + 1; l < result.size(); l++) {

                            PairCouple couple = new PairCouple(result.get(k), result.get(l));
                            CoupleInfo coupleInfo = pairs.get(couple);
                            if (coupleInfo == null) {
                                CoupleInfo value = new CoupleInfo(1, j);
                                pairs.put(couple, value);
                            } else {
                                if (j > coupleInfo.getLevel()) {
                                    coupleInfo.incrementCount();
                                    coupleInfo.setLevel(j);
                                }
                            }

                            for (int g = l + 1; g < result.size(); g++) {
                                PairCouple tripleCouple = new PairCouple(result.get(k), result.get(l), result.get(g));
                                CoupleInfo trippleInfo = pairs.get(tripleCouple);
                                if (trippleInfo == null) {
                                    CoupleInfo trippleValue = new CoupleInfo(1, j);
                                    pairs.put(tripleCouple, trippleValue);
                                } else {
                                    if (j > trippleInfo.getLevel()) {
                                        trippleInfo.incrementCount();
                                        trippleInfo.setLevel(j);
                                    }
                                }
                            }
                        }
                    }

                }

            }


        }

        List<PairCouple> couples = new LinkedList<>();
        for (Map.Entry<PairCouple, CoupleInfo> pairCoupleIntegerEntry : pairs.entrySet()) {
            PairCouple key = pairCoupleIntegerEntry.getKey();
            CoupleInfo value = pairCoupleIntegerEntry.getValue();
            key.setCount(value.getCount());
            couples.add(key);
        }

        return couples;
    }
}
