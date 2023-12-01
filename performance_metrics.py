import tracemalloc
import time
from subprocess import call
from utils import config
from utils import Tools

def performance_metrics():
	tracemalloc.stop()
	tracemalloc.start()
	start = time.time()
	call(["python", "execution_report.py"])
	end = time.time()
	perf_file  = config.get_config('output','perf_file')
	t_perf = Tools(perf_file,'w')
	str_time = ("time elapsed {} seconds".format((end-start))) + '.\n'

	first_size, first_peak = tracemalloc.get_traced_memory()
	peak = first_peak/(1024*1024)
	str_mem = (f'Peak Size in MB - , {peak}')
	t_perf.string_write(str_time+str_mem)

if __name__ == "__main__":
        performance_metrics()
        print("Completed performance report")
