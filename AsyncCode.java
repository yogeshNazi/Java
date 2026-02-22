import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AsyncCode {
    private static final ExecutorService executor = Executors.newFixedThreadPool(5);    
    public static class User {
        String name;
        Integer age;
        public User(String name, Integer age) {
            this.name = name;
            this.age = age;
        }
    }
    public interface DatabaseCallback {
        void onSuccess(String result);
        void onFailure(Exception e);
    }
    public static void sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static CompletableFuture<User> fetchUserDetailsFromDatabase(String userId) {
        CompletableFuture<User> future = CompletableFuture.supplyAsync(() -> {
            sleep(500);
            try {
                System.out.print("current thread: " + Thread.currentThread().getName() + " fetching user details for: " + userId);
                if (userId.equals("yogesh")) {
                    return new User("Yogesh", 25);
                } else if (userId.equals("swastik")){
                    return new User("Swastik", 25);
                } 
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
            return null;
        }, executor);
        return future;
    }

    public static void fetchDataFromDatabase(String userId, DatabaseCallback callback) {
        new Thread(() -> {
             sleep(500);
            if (userId.equals("yogesh")) {
                callback.onSuccess("momos");
            } else if(userId.equals("swastik")) {
                callback.onSuccess("chowmein");
            } else {
                callback.onFailure(new Exception("User not found"));
            }
        }).start();
    }
    public static CompletableFuture<String> fetchDishes(String userId) {
        CompletableFuture<String> future = new CompletableFuture<>();
        fetchDataFromDatabase(userId, new DatabaseCallback() {
            @Override
            public void onSuccess(String result) {
                future.complete(result);
            }

            @Override
            public void onFailure(Exception e) {
                future.completeExceptionally(e);
            }
        });
        return future;
    }
    public static void main(String[] args) {
        System.out.println("Fetching dishes for user: swastik");
        fetchDishes("swastik").thenApply((dish) -> {
            return dish.toUpperCase();
        }).thenAccept((dish) -> {
            System.out.println("Dish: " + dish);
        }).exceptionally((e) -> {
            System.out.println("Error: " + e.getMessage());
            return null;
        });
        System.out.println("while the code is fetching dishes");
        for (int i = 0; i < 5; i++) {
            System.out.println("Doing other work: " + i);
            sleep(200);
        }

        CompletableFuture<User> userFuture = fetchUserDetailsFromDatabase("yogesh");
        User u = null;
        System.out.print("current thread: " + Thread.currentThread().getName() + " waiting for user details...");
        userFuture.thenApplyAsync((user) -> {
            System.out.println("User details fetched for: " + user.name);
            return user;
        }).exceptionally((e) -> {
            System.out.println("Error fetching user details: " + e.getMessage());
            return null;
        });

        for (int i = 5; i < 10; i++) {
            System.out.println("Doing other work: " + i);
            sleep(200);
        }
        try {
            u = userFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        
        System.out.println("User: " + u.name + ", Age: " + u.age);
        // executor.shutdown();

        CompletableFuture<String> future1 = CompletableFuture.supplyAsync(() -> {
            sleep(300);
            System.out.println(Thread.currentThread().getName() + " executing future1");
            return "Hello";
        }, executor);
        CompletableFuture<String> future2 = CompletableFuture.supplyAsync(() -> {
            System.out.println(Thread.currentThread().getName() + " executing future2");
            return "cutie swastik";
        }, executor);

        CompletableFuture<String> future3 = CompletableFuture.supplyAsync(() -> {
            sleep(400);
            System.out.println(Thread.currentThread().getName() + " executing future3");
            return "How are you?";
        }, executor);

        // allOf, anyOf can be used to find  when all or any of the futures are completed
        CompletableFuture<Void> combinedFuture = CompletableFuture.allOf(future1, future2, future3);
        combinedFuture.thenRun(() -> {
            try {
                System.out.println(Thread.currentThread().getName() + " all futures completed, fetching results...");
                String result1 = future1.get();
                String result2 = future2.get();
                String result3 = future3.get();
                System.out.println("Combined results: " + result1 + "\n" + result2 + "\n" + result3);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        });
        executor.shutdown();
    }
}