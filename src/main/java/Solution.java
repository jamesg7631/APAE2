import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Solution implements CommandRunner{
    private final TaskManager taskManager;
    public Solution() {
        this.taskManager = new TaskManager();
    }

    public String runCommand(String command) {
        String[] parts = command.split(" ");
        String commandType = parts[0];

        if (commandType.equals("start")) {
            long taskId = Long.parseLong(parts[1]);
            return taskManager.addTask(taskId, new HashSet<>());
        } else if (commandType.equals("cancel")) {
            long taskId = Long.parseLong(parts[1]);
            return taskManager.cancelTask(taskId);
        } else if (commandType.equals("get")) {
            long taskID = Long.parseLong(parts[1]);
            return taskManager.getTaskStatus(taskID);
        } else if (commandType.equals("after")) {
            long taskId = Long.parseLong(parts[1]);
            long afterTaskId = Long.parseLong(parts[2]);
            return after(taskId, afterTaskId);
        } else if (commandType.equals("running")) {
            return taskManager.getRunningTasks();
        } else if (commandType.equals("finish")) {
            return taskManager.finish();
        } else if (commandType.equals("abort")) {
            return taskManager.abort();
        } else {
            return "Invalid command";
        }
    }

    public synchronized String after(long taskId, long afterTaskId) {
        Set<Long> dependsOn = new HashSet<>();
        dependsOn.add(afterTaskId);

        return taskManager.addTask(taskId, dependsOn);
    }
}
