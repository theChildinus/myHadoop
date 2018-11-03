import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.io.IOException;

public class ConfigWatcher implements Watcher {
    private ActiveKeyValueStore store;
    public ConfigWatcher(String host) throws InterruptedException, IOException {
        store = new ActiveKeyValueStore();
        store.connect(host);
    }

    public void displayConfig() throws InterruptedException, KeeperException {
        String value = store.read(ConfigUpdater.PATH, this);
        System.out.printf("Read %s as %s\n", ConfigUpdater.PATH, value);
    }

    public void process(WatchedEvent event) {
        if (event.getType() == Event.EventType.NodeDataChanged) {
            try {
                displayConfig();
            } catch (InterruptedException e) {
                System.err.println("Interrupted Exiting");
                Thread.currentThread().interrupt();
            } catch (KeeperException e) {
                System.err.printf("KeeperExceptionL %s Exiting\n", e);
            }
        }
    }

    public static void main(String[] args) throws Exception{
        ConfigWatcher configWatcher = new ConfigWatcher(args[0]);
        configWatcher.displayConfig();
        Thread.sleep(Long.MAX_VALUE);
    }
}
