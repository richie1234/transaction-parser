package me.rk5.parser.util;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import me.rk5.parser.dto.Transaction;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.to_timestamp;

public class Util {

    public static final String TO_ACCOUNT_ID = "toAccountId";
    public static final String CREATED_AT = "createdAt";
    public static final String AMOUNT = "amount";
    public static final String TRANSACTION_TYPE = "transactionType";
    public static final String RELATED_TRANSACTION = "relatedTransaction";
    public static final String DD_MM_YYYY_HH_MM_SS = "dd/MM/yyyy HH:mm:ss";
    public static final String TIMESTAMP = "timestamp";
    public static final String FROM_ACCOUNT_ID = "fromAccountId";
    public static final String TRANSACTION_ID = "transactionId";
    public static final String REVERSAL = "REVERSAL";
    public static final String PAYMENT = "PAYMENT";


    public static Dataset<Transaction> getTransactionDataset(Dataset<Row> dataset, String accountId) {
        Dataset<Transaction> transactionDataset = dataset.select(col(TRANSACTION_ID), col(FROM_ACCOUNT_ID),
                col(TO_ACCOUNT_ID),col(CREATED_AT),
                col(AMOUNT).cast(DataTypes.DoubleType),col(TRANSACTION_TYPE),col(RELATED_TRANSACTION),
                to_timestamp(col(CREATED_AT), DD_MM_YYYY_HH_MM_SS).as(TIMESTAMP)).
                filter(dataset.col(FROM_ACCOUNT_ID).contains(accountId)).as(Encoders.bean(Transaction.class));
        return transactionDataset;
    }

    public static Dataset<Transaction> getPaymentDataset(Dataset<Transaction> transactionDataset, String fromDate, String toDate) {
        Dataset<Transaction> intersectDataset = transactionDataset.select(col(TRANSACTION_ID),col(FROM_ACCOUNT_ID),
                col(TO_ACCOUNT_ID),col(CREATED_AT),col(AMOUNT).cast(DataTypes.DoubleType),
                col(TRANSACTION_TYPE), col(RELATED_TRANSACTION),
                to_timestamp(col(CREATED_AT), DD_MM_YYYY_HH_MM_SS).as(TIMESTAMP)).
                filter(col(TRANSACTION_TYPE).contains(PAYMENT)).
                filter(col(TIMESTAMP).$less$eq(toDate)).as(Encoders.bean(Transaction.class)).
                intersect(transactionDataset.select(col(TRANSACTION_ID),col(FROM_ACCOUNT_ID),
                        col(TO_ACCOUNT_ID),col(CREATED_AT),col(AMOUNT).cast(DataTypes.DoubleType),
                        col(TRANSACTION_TYPE), col(RELATED_TRANSACTION),
                        to_timestamp(col(CREATED_AT), DD_MM_YYYY_HH_MM_SS).as(TIMESTAMP)).
                        filter(col(TRANSACTION_TYPE).contains(PAYMENT)).
                        filter(col(TIMESTAMP).$greater$eq(fromDate)).as(Encoders.bean(Transaction.class)));

        return intersectDataset;
    }

    public static Dataset<Transaction> getReversalDataset(Dataset<Transaction> transactionDataset) {
        Dataset<Transaction> reversalDataset = transactionDataset.select(col(TRANSACTION_ID),col(FROM_ACCOUNT_ID),
                col(TO_ACCOUNT_ID),col(CREATED_AT),col(AMOUNT).cast(DataTypes.DoubleType),
                col(TRANSACTION_TYPE), col(RELATED_TRANSACTION),
                to_timestamp(col(CREATED_AT), DD_MM_YYYY_HH_MM_SS).as(TIMESTAMP)).
                filter(col(TRANSACTION_TYPE).contains(REVERSAL)).as(Encoders.bean(Transaction.class));
        return reversalDataset;
    }

    public static double getTotalPayments(Dataset<Transaction> paymentDataset) {

        List<Transaction> paymentDatasetList = paymentDataset.collectAsList();
        double totalPayments = 0;

        for (int i = 0; i < paymentDatasetList.size(); i++) {
            totalPayments = totalPayments - paymentDatasetList.get(i).getAmount();

        }
        return totalPayments;
    }

    public static List<Transaction> findReversals(Dataset<Transaction> reversalDataset, Dataset<Transaction> paymentDataset) {

        List<Transaction> allReversals = reversalDataset.collectAsList();
        List<Transaction> paymentDatasetList = paymentDataset.collectAsList();
        List<Transaction> reversals = new ArrayList<>();

        for (int i = 0; i < allReversals.size(); i++) {

            for (int j = 0; j < paymentDatasetList.size(); j++) {

                if(allReversals.get(i).getRelatedTransaction().equals(paymentDatasetList.get(j).getTransactionId())) {
                    reversals.add(allReversals.get(i));
                }
            }

        }
        return reversals;

    }
}
