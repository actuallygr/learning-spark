package com.actuallygr.spark;


import java.util.Scanner;

class Runner extends Thread {
    public void run() {

        for (int i = 0; i < 10; i++) {
            System.out.println("Hello" + i);
        }

        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}


class RunnerableUser implements Runnable {
    public void run() {
        for (int i = 0; i < 10; i++) {
            System.out.println("Hello" + i);
        }

        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}


class Processor extends Thread {

    private volatile boolean running = true;
    public void run() {
        while (running) {
            System.out.println("Hello");

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void shutdown() {
        running = false;
    }
}

public class Multithreading {

    public void start() {
//        Runner runner1 = new Runner();
//        runner1.start();
//
//        Runner runner2 = new Runner();
//        runner2.start();


//        Thread t1 = new Thread(new RunnerableUser());
//        Thread t2 = new Thread(new RunnerableUser());
//
//        t1.start();
//        t2.start();

//        Thread t1 = new Thread(new Runnable() {
//            @Override
//            public void run() {
//                for (int i =0; i<10; i++) {
//                    System.out.println("Hello" +i);
//                }

//
//                try {
//                    Thread.sleep(200);
//                } catch(InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
//        });
//
//        t1.start();

        Processor proc1 = new Processor();

        proc1.start();

        System.out.println("Press return to Stop");
        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();

        proc1.shutdown();


    }
}
