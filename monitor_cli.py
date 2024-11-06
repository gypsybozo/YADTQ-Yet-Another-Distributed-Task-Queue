from yadtq.core.result_db import ResultStore
from yadtq.monitoring_fns import YADTQMonitor

result_store = ResultStore()
monitor = YADTQMonitor(result_store)
monitor.run()