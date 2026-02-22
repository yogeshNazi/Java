import java.util.concurrent.CompletableFuture;

public class AsyncCode {
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
    }
}