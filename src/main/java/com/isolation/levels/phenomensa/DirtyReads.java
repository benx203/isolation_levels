package com.isolation.levels.phenomensa;

import com.isolation.levels.ConnectionsProvider;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import static com.isolation.levels.Utils.printResultSet;
import static java.sql.Connection.TRANSACTION_READ_UNCOMMITTED;

/**
 * Created by dreambig on 11.03.17.
 */
public class DirtyReads {

    public static void main(String[] args) {
        CyclicBarrier barrier1 = new CyclicBarrier(2);
        CyclicBarrier barrier2 = new CyclicBarrier(2);

        setUp(ConnectionsProvider.getConnection());

        Connection connection1 = ConnectionsProvider.getConnection();
        Connection connection2 = ConnectionsProvider.getConnection();

        assert connection1 != connection2;

        Transaction1 transaction1 = new Transaction1(barrier1, barrier2, connection1);
        Transaction2 transaction2 = new Transaction2(barrier1, barrier2, connection2);

        Thread thread1 = new Thread(transaction1);
        thread1.start();

        Thread thread2 = new Thread(transaction2);
        thread2.start();
    }

    private static void setUp(Connection connection) {
        try {
            connection.prepareStatement("delete from actor where first_name=\"Ivan\"").execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static class Transaction1 implements Runnable {
        private CyclicBarrier barrier1;
        private CyclicBarrier barrier2;
        private Connection connection;

        public Transaction1(CyclicBarrier barrier1, CyclicBarrier barrier2, Connection connection) {
            this.barrier1 = barrier1;
            this.barrier2 = barrier2;
            this.connection = connection;
        }

        public void run() {
            try {
                // wait here until thread 2 start a transaction and insert some rows.
                barrier2.await();

                // start a transaction by setting auto commit to false
                connection.setAutoCommit(false);
                // Set the isolation level to of the transaction to TRANSACTION_READ_UNCOMMITTED
                connection.setTransactionIsolation(TRANSACTION_READ_UNCOMMITTED);

                // reading the first result
                ResultSet resultSet = connection.prepareStatement("select * from actor where first_name=\"Ivan\"").executeQuery();

                printResultSet(resultSet);
                connection.commit();

                // thread 1 read already
                barrier1.await();

                // wait thread 2 rollback
                barrier2.await();

                // now execute the same query
                ResultSet resultSet2 = connection.prepareStatement("select * from actor where first_name=\"Ivan\"").executeQuery();

                // now we have no results, because thread 2 rollback!
                printResultSet(resultSet2);
                connection.commit();
            } catch (SQLException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (BrokenBarrierException e) {
                e.printStackTrace();
            }
        }
    }

    public static class Transaction2 implements Runnable {
        private CyclicBarrier barrier1;
        private CyclicBarrier barrier2;
        private Connection connection;

        public Transaction2(CyclicBarrier barrier1, CyclicBarrier barrier2, Connection connection) {
            this.barrier1 = barrier1;
            this.barrier2 = barrier2;
            this.connection = connection;
        }

        public void run() {
            try {
                connection.setAutoCommit(false);
                connection.prepareStatement("insert into actor (first_name,last_name,last_update) VALUE (\"Ivan\",\"Ivanov\",\"2019-02-01\")").execute();

                // let transaction 1 continue and isuue a select statement,
                // even this current transaction is not committed, transaction 1 will be able to see the inserted value but the statement above !
                barrier2.await();

                // wait thread 1 read
                barrier1.await();

                connection.rollback();

                // thread 2 rollback already
                barrier2.await();
            } catch (SQLException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (BrokenBarrierException e) {
                e.printStackTrace();
            }
        }
    }
}
