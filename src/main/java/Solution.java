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

        if (!isCommandValid(parts)) {
            return "Invalid command";
        }

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

    public boolean isCommandValid(String[] parts) {
        if ("start".equals(parts[0])) {
            if (parts.length != 2) {
                return false;
            }
            return isLong(parts[1]);
        } else if ("cancel".equals(parts[0])) {
            if (parts.length != 2) {
                return false;
            }
            return isLong(parts[1]);
        } else if ("running".equals(parts[0])) {
            return parts.length == 1;
        }
        else if ("get".equals(parts[0])) {
            if (parts.length != 2) {
                return false;
            }
            return isLong(parts[1]);
        } else if ("after".equals(parts[0])) {
            if (parts.length != 3) {
                return false;
            }

            if (!isLong(parts[1]) || !isLong(parts[2])) {
                return false;
            }
        } else if ("finish".equals(parts[0])) {
            return parts.length == 1;
        } else if ("abort".equals(parts[0])) {
            return parts.length == 1;
        }
        return false;
    }

    private boolean isLong(String number) {
        try {
            long longNumber = Long.parseLong(number);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    public synchronized String after(long dependencyTaskId, long dependentTaskId) {
        Set<Long> dependsOn = new HashSet<>();
        dependsOn.add(dependencyTaskId);

        return taskManager.addTask(dependentTaskId, dependsOn);
    }
}
