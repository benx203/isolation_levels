package com.isolation.levels.phenomensa;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import static com.isolation.levels.ConnectionsProvider.getConnection;
import static com.isolation.levels.Utils.printResultSet;

/**
 * Created by dreambig on 12.03.17.
 */
public class NonRepeatableReads {

    public static void main(String[] args) {
        setUp(getConnection());
        CyclicBarrier barrier1 = new CyclicBarrier(2);
        CyclicBarrier barrier2 = new CyclicBarrier(2);
        Thread t1 = new Thread(new Transaction1(barrier1,barrier2, getConnection()));
        Thread t2 = new Thread(new Transaction2(barrier1,barrier2, getConnection()));

        t1.start();
        t2.start();
    }

    private static void setUp(Connection connection) {
        try {
            connection.prepareStatement("delete from actor where first_name=\"Ivan\"").execute();
            connection.prepareStatement("insert into actor (first_name,last_name,last_update) VALUE (\"Ivan\",\"Ivanov\",\"2019-02-01\")").execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    static class Transaction1 implements Runnable {
        private CyclicBarrier barrier1;
        private CyclicBarrier barrier2;
        private Connection connection;

        public Transaction1(CyclicBarrier barrier1, CyclicBarrier barrier2, Connection connection) {
            this.barrier1 = barrier1;
            this.barrier2 = barrier2;
            this.connection = connection;
        }

        @Override
        public void run() {
            try {
                connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                connection.setAutoCommit(false);

                ResultSet firstResult = connection.prepareStatement("select * from actor where first_name=\"Ivan\"").executeQuery();
                printResultSet(firstResult);

                // thread 1 first query complete
                barrier1.await();

                // wait thread 2 update
                barrier2.await();

                ResultSet secondResult = connection.prepareStatement("select * from actor where first_name=\"Ivan\"").executeQuery();
                printResultSet(secondResult);

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

    static class Transaction2 implements Runnable {
        private CyclicBarrier barrier1;
        private CyclicBarrier barrier2;
        private Connection connection;

        public Transaction2(CyclicBarrier barrier1, CyclicBarrier barrier2, Connection connection) {
            this.barrier1 = barrier1;
            this.barrier2 = barrier2;
            this.connection = connection;
        }

        @Override
        public void run() {
            try {
                // wait thread 1 first query
                barrier1.await();

                connection.prepareStatement("UPDATE actor SET last_name=\"ALEC\" WHERE first_name=\"Ivan\"").execute();

                // thread 2 update complete
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
