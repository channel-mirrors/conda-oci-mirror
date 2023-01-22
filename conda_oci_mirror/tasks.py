import multiprocessing as mp
import time

import conda_oci_mirror.package as pkg

# Counters for lifetime of tasks
package_counter = mp.Value("i", 0)
counter_start = mp.Value("d", time.time())
last_upload_time = mp.Value("d", time.time())


class Task:
    """
    A single task to download a package, push, and cleanup.
    """

    def __init__(self, *args, dry_run=False, **kwargs):
        self.pkg = pkg.Package(*args, **kwargs)
        self.dry_run = dry_run

    def run(self):
        """
        Run the task. This means:

        1. Creating the package-specific cache directory.
        2. Downloading the current file.
        3. Upload the package (or emulating it)
        """
        self.pkg.ensure_file()

        global package_counter, counter_start, last_upload_time

        with last_upload_time.get_lock():
            lt = last_upload_time.value
            now = time.time()
            rt = 0.5
            if now - lt < rt:
                print(f"Rate limit sleep for {(lt + rt) - now}")
                time.sleep((lt + rt) - now)
            last_upload_time.value = now

        # This has retry wrapper - we get back metadata about the package pushed
        result = self.pkg.upload(self.dry_run)

        with package_counter.get_lock(), counter_start.get_lock():
            package_counter.value += 1
            if package_counter.value % 10 == 0:
                elapsed_min = (time.time() - counter_start.value) / 60.0
                print(
                    "Average no packages / min: ", package_counter.value / elapsed_min
                )

            if package_counter.value % 50 == 0:
                package_counter.value = 0
                counter_start.value = time.time()

        # delete the package
        self.pkg.delete()
        return result


class TaskRunner:
    """
    A task runner knows how to time and run tasks!
    """

    def __init__(self, workers=4):
        self.workers = workers
        self.tasks = []

    def add_task(self, task):
        self.tasks.append(task)

    def run(self):
        """
        Run the tasks!
        """
        global counter_start
        with counter_start.get_lock():
            counter_start.value = time.time()

        # Keep track of results
        items = []
        # for task in tasks:
        #     # start = time.time()
        #     task.run()
        #     # end = time.time()
        #     # elapsed = end - start

        #     # This should at least take 20 seconds
        #     # Otherwise we sleep a bit
        #     if elapsed < 3:
        #         print("Sleeping for ", 3 - elapsed)
        #         time.sleep(3 - elapsed)
        with mp.Pool(processes=self.workers) as pool:
            for result in pool.map(run_task, self.tasks):

                # This is a smaller list of packages pushes
                items += result

        # Return all results from running the task
        return items


def run_task(t):
    return t.run()
