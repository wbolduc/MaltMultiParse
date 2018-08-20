package maltparsetest;


import java.util.concurrent.atomic.AtomicInteger;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author wbolduc
 */
public class ConcurrentCounter implements Runnable {
    private String updateMessege;
    private AtomicInteger value;
    private int sleepTime;

    public ConcurrentCounter(String updateMessege, AtomicInteger value, int sleepTime) {
        this.updateMessege = updateMessege;
        this.value = value;
        this.sleepTime = sleepTime;
    }
    
    
    
    public void run()
    {
        while(!Thread.currentThread().isInterrupted())
        {
            System.out.println(updateMessege + value.toString());
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException ignore) {
                return;
            }
        }
    }
    
}
