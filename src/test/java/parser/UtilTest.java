package parser;

import me.rk5.parser.dto.Transaction;
import me.rk5.parser.util.Util;
import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Before;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class UtilTest {

    SparkSession spark = SparkSession.builder().appName("junit").master("local[1]")
            .config("spark.sql.warehouse.dir","file:///s3:/logs/")
            .getOrCreate();

    List<Row> inMemory = new ArrayList<>();
    Dataset<Row> dataset;
    private StructType structType;
    private Util util = new Util();

    @Before
    public void beforeTest() {
        inMemory.add(RowFactory.create("TX10001", "ACC334455", "ACC778899", "20/10/2018 12:47:55", "25.00", "PAYMENT",""));
        inMemory.add(RowFactory.create("TX10002", "ACC334455", "ACC998877", "20/10/2018 17:33:43", "10.50", "PAYMENT",""));
        inMemory.add(RowFactory.create("TX10003", "ACC998877", "ACC778899", "20/10/2018 18:00:00", "5.00", "PAYMENT","" ));
        inMemory.add(RowFactory.create("TX10004", "ACC334455", "ACC998877", "20/10/2018 19:45:00", "10.50", "REVERSAL", "TX10002"));
        inMemory.add(RowFactory.create("TX10005", "ACC334455", "ACC778899", "21/10/2018 09:30:00", "7.25", "PAYMENT",""));


        StructField[] structFields = new StructField[] {
                new StructField("transactionId",DataTypes.StringType,false,Metadata.empty()),
                new StructField("fromAccountId",DataTypes.StringType,false,Metadata.empty()),
                new StructField("toAccountId",DataTypes.StringType, false, Metadata.empty()),
                new StructField("createdAt",DataTypes.StringType,false,Metadata.empty()),
                new StructField("amount",DataTypes.StringType, false, Metadata.empty()),
                new StructField("transactionType",DataTypes.StringType,false,Metadata.empty()),
                new StructField("relatedTransaction",DataTypes.StringType, false, Metadata.empty()),

        };

        structType = new StructType(structFields);
        dataset = spark.createDataFrame(inMemory, structType);

    }



    @Test
    public void testgetTransactionDatasetShouldReturnCorrectTransactionsSet() {

        Dataset<Transaction> results = util.getTransactionDataset(dataset, "ACC334455");
        JavaRDD<Transaction> rdd = results.toJavaRDD();

        List<String> list =  Arrays.asList("TX10001", "TX10002", "TX10004", "TX10005");
        List<String> resultsRdd =  Arrays.asList(rdd.collect().get(0).getTransactionId(),
                rdd.collect().get(1).getTransactionId(),rdd.collect().get(2).getTransactionId(),
                rdd.collect().get(3).getTransactionId());

        assertTrue(rdd.count() == 4);
        assertTrue(CollectionUtils.isEqualCollection(resultsRdd, list));



    }



    @Test
    public void testgetPaymentDatasetShouldReturnCorrectTransactionsSet() throws ParseException {

        Dataset<Transaction> transactionDataset = util.getTransactionDataset(dataset, "ACC334455");
        SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");

        String fromDate = new java.sql.Timestamp(dateFormat.parse("20/10/2018 12:00:00").getTime()).toString();
        String toDate = new java.sql.Timestamp(dateFormat.parse("20/10/2018 19:00:00").getTime()).toString();

        Dataset<Transaction> results = util.getPaymentDataset(transactionDataset, fromDate, toDate);
        JavaRDD<Transaction> rdd = results.toJavaRDD();

        List<String> list =  Arrays.asList("TX10001", "TX10002");
        List<String> resultsRdd =  Arrays.asList(rdd.collect().get(0).getTransactionId(),
                rdd.collect().get(1).getTransactionId());

        assertTrue(rdd.count() == 2);
        assertTrue(CollectionUtils.isEqualCollection(resultsRdd, list));


    }

    @Test
    public void testgetReversalDatasetShouldReturnCorrectTransactionsSet()  {

        Dataset<Transaction> transactionDataset = util.getTransactionDataset(dataset, "ACC334455");

        Dataset<Transaction> results = util.getReversalDataset(transactionDataset);
        JavaRDD<Transaction> rdd = results.toJavaRDD();

        List<String> list =  Arrays.asList("TX10004");
        List<String> resultsRdd =  Arrays.asList(rdd.collect().get(0).getTransactionId());

        assertTrue(rdd.count() == 1);
        assertTrue(CollectionUtils.isEqualCollection(resultsRdd, list));


    }


    @Test
    public void testgetTotalPaymentsShouldReturnCorrectTotal() throws ParseException {

        dataset = spark.createDataFrame(inMemory, structType);
        Dataset<Transaction> results = util.getTransactionDataset(dataset, "ACC334455");

        SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        String fromDate = new java.sql.Timestamp(dateFormat.parse("20/10/2018 12:00:00").getTime()).toString();
        String toDate = new java.sql.Timestamp(dateFormat.parse("20/10/2018 19:00:00").getTime()).toString();

        Dataset<Transaction> paymentDataset = util.getPaymentDataset(results, fromDate, toDate);
        double totalPayments = util.getTotalPayments(paymentDataset);

        assertEquals(totalPayments, -35.5, 0.00000001);

    }

    @Test
    public void testgetTotalReversalsShouldReturnCorrectTotal() throws ParseException {

        Dataset<Transaction> results = util.getTransactionDataset(dataset, "ACC334455");

        SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");

        String fromDate = new java.sql.Timestamp(dateFormat.parse("20/10/2018 12:00:00").getTime()).toString();
        String toDate = new java.sql.Timestamp(dateFormat.parse("20/10/2018 19:00:00").getTime()).toString();
        Dataset<Transaction> paymentDataset = util.getPaymentDataset(results, fromDate, toDate);
        List<Transaction> reversals = util.findReversals(util.getReversalDataset(results), paymentDataset);
        double totalValueOFreversals = reversals.stream().mapToDouble(i -> i.getAmount()).sum();;

        assertTrue(reversals.size() == 1);
        assertEquals(totalValueOFreversals ,10.5, 0.00000001);


    }


}

