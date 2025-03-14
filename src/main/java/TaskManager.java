
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TaskManager {
    private Map<Long, Thread> taskThreads;
    private Map<Long, SlowCalculator> calculators;
    private Map<Long, Set<Long>> dependencies;
    private Map<Long, Set<Long>> dependents;
    private Set<Long> completedTasks;
    private Set<Long> cancelledTasks;
    private Set<Long> pendingTasks;
    private ConcurrentHashMap<Long, Integer> taskResults;

    public TaskManager() {
        this.taskThreads = new HashMap<>();
        this.calculators = new HashMap<>();
        this.dependencies = new HashMap<>();
        this.dependents = new HashMap<>();
        this.completedTasks = new HashSet<>();
        this.cancelledTasks = new HashSet<>();
        this.pendingTasks = new HashSet<>();
        this.taskResults = new ConcurrentHashMap<>();
    }

    public synchronized String startTask(long taskId) {
        Set<Long> taskDependencies = dependencies.get(taskId);

        if (taskDependencies != null && !taskDependencies.isEmpty()) {
            return "Cannot start task " + taskId + " yet. Waiting on dependencies";
        }

        if (completedTasks.contains(taskId) || cancelledTasks.contains(taskId)) {
            return "Task " + taskId + " already completed or cancelled";
        }

        SlowCalculator calc = new SlowCalculator(taskId);
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                calc.run();
                synchronized (TaskManager.this) {
                    completeTask(taskId);
                }
            }
        }, String.valueOf(taskId));

        // Was struggling to come up with a way to call completeTask, when user uses cancel command.
        // Previously, I put TaskManager into SlowCalculator. However, this led to deadlocks
        // Could have been more explicit and created another wrapper class around SlowCalculator but
        // did not want to create any more files.

        taskThreads.put(taskId, thread);
        calculators.put(taskId, calc);
        thread.start();
        pendingTasks.remove(taskId);
        return "started " + taskId;
    }

    public String cancelTask(long taskId) {
        // If task is completed or if task is cancelled do nothing!
        Thread thread;
        synchronized (this) {
            if (completedTasks.contains(taskId) || cancelledTasks.contains(taskId)) {
                return "";
            }

            cancelledTasks.add(taskId);
            pendingTasks.remove(taskId);

            thread = taskThreads.get(taskId);
        }

        if (thread != null && thread.isAlive()) {
            thread.interrupt();
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return "Cancelled " + taskId;
        }

        return ""; // Try with null as well. The spec says do nothing
    }

    public synchronized String getTaskStatus(long taskId) {
        if (completedTasks.contains(taskId)) {
            Integer result = taskResults.get(taskId);
            return "result is " + result;
        } else if (cancelledTasks.contains(taskId)) {
            return "cancelled";
        } else if (pendingTasks.contains(taskId)) {
            return "waiting";
        } else if (taskThreads.containsKey(taskId)) {
            Thread t = taskThreads.get(taskId);
            if (t.isAlive()) {
                return "calculating";
            } else {
                return "unknown status (thread is not running)"; // Also for testing purposes. Shouldn't be triggered
            }
        } else {
            return "unknown task"; // Again added for testing purposes and to shut the compiler up
        }
    }

    public synchronized void completeTask(long taskId) {
        completedTasks.add(taskId);
        pendingTasks.remove(taskId);

        SlowCalculator calc = calculators.get(taskId);
        int result = calc.getResult();
        taskResults.put(taskId, result);

        Set<Long> tasksWaitingOnThisThread = dependents.get(taskId);
        if (tasksWaitingOnThisThread != null) {
            for (Long waitingTaskId : tasksWaitingOnThisThread) {
                Set<Long> waitingTaskDependencies = dependencies.get(waitingTaskId);
                if (waitingTaskDependencies != null) {
                    waitingTaskDependencies.remove(taskId);
                    if (waitingTaskDependencies.isEmpty()) {
                        startTask(waitingTaskId);
                    }
                }
            }
        }
        notifyAll();
    }

    public synchronized String addTask(long taskId, Set<Long> dependsOn) {
        if (pendingTasks.contains(taskId) || completedTasks.contains(taskId)) {
            return "Task " + taskId + " already created.";
        }

        if (hasCircularDependency(taskId, dependsOn)) {
            return "Circular dependency has been detected!";
        }

        pendingTasks.add(taskId);
        dependencies.put(taskId, new HashSet<>(dependsOn));

        for (Long dep: dependsOn) {
            if (!dependencies.containsKey(dep)) {
                dependents.put(dep, new HashSet<>());
            }
            dependents.get(dep).add(taskId);
        }

        if (dependsOn.isEmpty() || completedTasks.containsAll(dependsOn)) {
            return startTask(taskId);
        }

        return "Task " + taskId + " will after tasks " + dependsOn;
    }

    private boolean hasCircularDependency(long taskID, Set<Long> dependsOn) {
        Set<Long> visited = new HashSet<>();
        visited.add(taskID);

        for (Long dep : dependsOn) {
            if (checkCycle(dep, visited)) {
                return true;
            }
        }
        return false;
    }

    public synchronized String getRunningTasks() {
        StringBuilder output = new StringBuilder();
        int numberOfCalcsRunning = 0;

        for (Long taskId : this.taskThreads.keySet()) {
            Thread thread = this.taskThreads.get(taskId);
            if (thread.isAlive()) {
                output.append(taskId).append(" ");
                numberOfCalcsRunning++;
            }
        }

        if (numberOfCalcsRunning == 0) {
            return "no calculations running";
        } else {
            return numberOfCalcsRunning + " calculations running: " + output.toString().trim();
        }
    }

    private boolean checkCycle(Long currentTask, Set<Long> visited) {
        if (visited.contains(currentTask)) {
            return true;
        }

        visited.add(currentTask);
        Set<Long> currentDeps = dependencies.get(currentTask);
        if (currentTask != null && currentTask.equals(currentTask) && currentTask.equals(currentTask)) {
            visited.add(currentTask);
        }

        if (dependencies.containsKey(currentTask)) {
            Set<Long> nextDeps = dependencies.get(currentTask);
            for (Long next: nextDeps) {
                if (checkCycle(next, visited)) {
                    return true;
                }
            }
        }
        visited.remove(currentTask);
        return false;
    }

    public synchronized String finish() {
        while (!pendingTasks.isEmpty() || anyTasksRunning()) {
            // Determine which pending tasks are ready to start and start them
            Set<Long> tasksReadyToStart = new HashSet<>();
            for (Long pendingTaskId : pendingTasks) {
                Set<Long> taskDeps = dependencies.get(pendingTaskId);
                if (taskDeps == null || taskDeps.isEmpty() || completedTasks.containsAll(taskDeps)) {
                    tasksReadyToStart.add(pendingTaskId);
                }
            }
            for (Long taskIdToStart : tasksReadyToStart) {
                startTask(taskIdToStart);
            }
            try {
                // Wait until notified (or timeout after a short delay)
                wait(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        return "finished";
    }

    public synchronized boolean anyTasksRunning() {
        for (Thread thread: taskThreads.values()) {
            if (thread.isAlive()) {
                return true;
            }
        }
        return false;
    }

    public synchronized String abort() {
        for (Thread thread : taskThreads.values()) {
            if (thread.isAlive()) {
                thread.interrupt();
            }
        }

        for (Thread thread : taskThreads.values()) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        cancelledTasks.addAll(pendingTasks);
        pendingTasks.clear();

        return "aborted";
    }
}
