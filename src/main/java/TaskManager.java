
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

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
            return "cancelled " + taskId;
        }

        return ""; // Try with null as well. The spec says do nothing
    }

    public synchronized String getTaskStatus(long taskId) {
        if (completedTasks.contains(taskId) && taskResults.get(taskId) != -2) {
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

        // Fetch tasks that depend on this finished task
        Set<Long> tasksWaitingOnThisThread = dependents.getOrDefault(taskId, new HashSet<>());

        for (Long dependentTaskId : tasksWaitingOnThisThread) {
            Set<Long> deps = dependencies.get(dependentTaskId);
            if (deps != null) {
                deps.remove(taskId);
                if (deps.isEmpty()) {
                    dependencies.remove(dependentTaskId);
                    startTask(dependentTaskId); // Immediately start as all dependencies cleared
                }
            }
        }

        dependents.remove(taskId); // Clean up after finishing task
        notifyAll();
    }


    public synchronized String addTask(long taskId, Set<Long> dependsOn) {
        // If the task is already completed, we can't modify it.
        if (completedTasks.contains(taskId)) {
            return "Task " + taskId + " already completed.";
        }

        // If the task exists and is not pending (i.e. it is running), then do not allow modifications.
        if (taskThreads.containsKey(taskId) && !pendingTasks.contains(taskId)) {
            return "Task " + taskId + " already created.";
        }

        // For tasks that are pending (not yet started), allow updating their dependencies.
        // Check for circular dependency before updating.
        if (hasCircularDependency(taskId, dependsOn)) {
            return "circular dependency " + buildCircularDependencyMessage(taskId, dependsOn);
        }

        // If the task is already pending, update its dependency set.
        if (pendingTasks.contains(taskId)) {
            Set<Long> currentDeps = dependencies.get(taskId);
            currentDeps.addAll(dependsOn);
            for (Long dep : dependsOn) {
                dependents.computeIfAbsent(dep, k -> new HashSet<>()).add(taskId);
            }
            return taskId + " will start after " + currentDeps;
        }

        // If the task is not yet created at all, then create it.
        if (dependsOn.isEmpty() || completedTasks.containsAll(dependsOn)) {
            return startTask(taskId);
        } else {
            pendingTasks.add(taskId);
            dependencies.put(taskId, new HashSet<>(dependsOn));
            for (Long dep : dependsOn) {
                dependents.computeIfAbsent(dep, k -> new HashSet<>()).add(taskId);
            }

            Long[] dependent = dependsOn.toArray(new Long[0]);
            return taskId + " will start after " + dependent[0];
        }
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

    public String abort() {
        Set<Thread> threadsToJoin;

        // Step 1: Interrupt all active threads and take a snapshot.
        synchronized (this) {
            for (Thread thread : taskThreads.values()) {
                if (thread.isAlive()) {
                    thread.interrupt();
                }
            }
            // Snapshot of threads to join is taken while holding the lock.
            threadsToJoin = new HashSet<>(taskThreads.values());
        }

        // Step 2: Join threads outside the synchronized block.
        for (Thread thread : threadsToJoin) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                // Restore the interrupted status.
                //Thread.currentThread().interrupt();
                e.printStackTrace();
            }
        }

        // Step 3: Update TaskManager state in a synchronized block.
        synchronized (this) {
            cancelledTasks.addAll(pendingTasks);
            pendingTasks.clear();
            // Notify any waiting threads (like finish()) that state has changed.
            notifyAll();
        }

        return "aborted";
    }

    private String buildCircularDependencyMessage(long taskId, Set<Long> newDeps) {
        // For each new dependency, try to find a cycle path from that dependency back to taskId.
        for (Long newDep : newDeps) {
            List<Long> cyclePath = findCyclePath(newDep, taskId, new HashSet<>());
            if (cyclePath != null) {
                // cyclePath is a list from newDep to taskId.
                // For example, if newDep==6 and taskId==2, it might return [6, 5, 4, 3, 2].
                // This is acceptable even if the order differs from sample output.
                return cyclePath.stream().map(Object::toString).collect(Collectors.joining(" "));
            }
        }
        // Should not reach here if a cycle is detected.
        return "";
    }

    private List<Long> findCyclePath(Long current, Long target, Set<Long> visited) {
        if (current.equals(target)) {
            // Found the target—start a new list with the target.
            List<Long> path = new ArrayList<>();
            path.add(current);
            return path;
        }
        if (visited.contains(current)) {
            return null; // Already visited this node; no cycle found along this path.
        }
        visited.add(current);
        // Look for dependencies for the current task.
        Set<Long> deps = dependencies.get(current);
        if (deps != null) {
            for (Long dep : deps) {
                List<Long> result = findCyclePath(dep, target, visited);
                if (result != null) {
                    // Prepend the current task to the found cycle path.
                    result.add(0, current);
                    return result;
                }
            }
        }
        return null; // No path from 'current' to 'target' found.
    }


}
