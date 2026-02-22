import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.StampedLock;

public class Locking {

    public static class Inventory {
        private int stock;
        private StampedLock lock = new StampedLock();
        public Inventory(int stock) {
            this.stock = stock;
        }
        public Inventory() {
            this.stock = 100;
        }

        public void customerPurchase(int quantity) {
            System.out.println("current thread: " + Thread.currentThread().getName() + " acquiring write lock for purchase" + "at " + System.currentTimeMillis());
            long stamp = lock.writeLock();
            try {
                System.out.println("Customer purchasing " + quantity + " items. Current stock: " + stock + " at " + System.currentTimeMillis());
                sleep(50);
                stock -= quantity;
                System.out.println("Customer purchased " + quantity + " items. Remaining stock: " + stock + " at " + System.currentTimeMillis());
            } finally {
                lock.unlockWrite(stamp);
            }
        }

        public void opportuniticRestock(int quantity) {
            long stamp = lock.tryWriteLock();
            if (stamp == 0L) {
                System.out.println("Could not acquire lock for restocking. Retrying...");
                return; // don't block, just return
            }
            try {
                stock += quantity; // simulate restocking
                System.out.println("Restocked " + quantity + " items. New stock: " + stock);
            } finally {
                lock.unlockWrite(stamp);
            }
        }

        public void checkStockWithReadLock() {
            System.out.println("current thread: " + Thread.currentThread().getName() + " acquiring read lock for checking stock");
            long stamp = lock.readLock();
            try {
                System.out.println("Checking stock. Current stock: " + stock + " at " + System.currentTimeMillis());
                sleep(1000);
            } finally {
                lock.unlockRead(stamp);
            }
        }

        // tryReadLock non blocking read lock, tries to acquire read lock immediately
        public void checkStock() {
            long stamp = lock.tryReadLock();
            if(stamp == 0L) {
                System.out.println("Could not acquire lock for checking stock. Retrying...");
                return;
            }
            try {
                System.out.println("Checking stock. Current stock: " + stock);
            } finally {
                lock.unlockRead(stamp);
            }
        }

        public void displayScreenUpdate() {
            long stamp = lock.tryOptimisticRead();
            int currentStock = stock; // read the stock without locking
            sleep(10); // simulate time taken to update screen
            if (!lock.validate(stamp)) { // check if a write occurred during the optimistic read
                System.out.println("Optimistic read failed. Acquiring read lock...");
                stamp = lock.readLock(); // acquire a proper read lock
                try {
                    System.out.println("Re-checking stock under read lock. Current stock: " + stock);
                    currentStock = stock; // re-read the stock under lock
                } finally {
                    lock.unlockRead(stamp);
                }
            } else {
                System.out.println("Optimistic read successful. Current stock: " + currentStock);
            }
            System.out.println("Screen updated with current stock: " + currentStock);
        }

        public void sleep(int millis) {
            try {
                Thread.sleep(millis);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }
    public static void main(String[] args) {
        Inventory inventory = new Inventory(100);
        ExecutorService executor = java.util.concurrent.Executors.newFixedThreadPool(4);
        executor.submit(() -> inventory.customerPurchase(10));
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {}

        executor.submit(() -> inventory.opportuniticRestock(20));

        executor.submit(() -> inventory.checkStock());

        executor.submit(() -> inventory.displayScreenUpdate());

        // showcasing multiple readers acquiring read lock concurrently
        executor.submit(() -> inventory.checkStockWithReadLock());
        executor.submit(() -> inventory.checkStockWithReadLock());
        executor.submit(() -> inventory.checkStockWithReadLock());

        // showcasing writers acquiring write lock needs to wait for readers to release read lock
        executor.submit(() -> inventory.customerPurchase(15));
        executor.shutdown();
        try {
            executor.awaitTermination(2, java.util.concurrent.TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }    
}
