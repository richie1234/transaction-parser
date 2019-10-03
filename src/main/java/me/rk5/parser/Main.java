package me.rk5.parser;

import me.rk5.parser.dto.Transaction;
import me.rk5.parser.util.Util;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;


public class Main {


    @SuppressWarnings("resource")
    public static void main(String[] args) throws ParseException {

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder().appName("transactions-parser").master("local[*]")
                .config("spark.sql.warehouse.dir","file:///s3:/logs/")
                .getOrCreate();
        Dataset<Row> dataset;

        if (args.length == 1 ) {
            dataset = spark.read()
                    .option("delimiter", ",")
                    .option("header", "true")
                    .option("ignoreLeadingWhiteSpace","true")
                    .option("ignoreTrailingWhiteSpace","true").csv(args[0]);
        } else {
            dataset = spark.read()
                    .option("delimiter", ",")
                    .option("header", "true")
                    .option("ignoreLeadingWhiteSpace","true")
                    .option("ignoreTrailingWhiteSpace","true")
                    .csv("src/main/resources/transactions.csv");
        }


        SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        String fromDate = new java.sql.Timestamp(dateFormat.parse(args[2]).getTime()).toString();
        String toDate = new java.sql.Timestamp(dateFormat.parse(args[3]).getTime()).toString();


        Dataset<Transaction> transactionDataset = Util.getTransactionDataset(dataset, args[1]);
        Dataset<Transaction> paymentDataset = Util.getPaymentDataset(transactionDataset, fromDate, toDate);
        Dataset<Transaction> reversalDataset = Util.getReversalDataset(transactionDataset);

        List<Transaction> transactionReversals = Util.findReversals(reversalDataset, paymentDataset);

        Print(transactionReversals, paymentDataset);
        spark.close();
    }

    private static void Print(List<Transaction> transactionReversals, Dataset<Transaction> paymentDataset) {
        double totalPayments = Util.getTotalPayments(paymentDataset);
        double totalTransactionReversals = transactionReversals.stream().mapToDouble(i -> i.getAmount()).sum();
        System.out.println("Relative balance for the period is: " + (totalPayments + totalTransactionReversals));
        System.out.println("Number of transactions included is: " + (paymentDataset.collectAsList().size() - transactionReversals.size()));

    }


}
