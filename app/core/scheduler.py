from apscheduler.executors.pool import ThreadPoolExecutor  # type: ignore
from apscheduler.schedulers.background import BackgroundScheduler  # type: ignore
from apscheduler.schedulers.blocking import BlockingScheduler  # type: ignore

customer_scheduler = BlockingScheduler({'apscheduler.timezone': 'UTC'}, executors={'default': ThreadPoolExecutor(1)})
backdrop_scheduler = BackgroundScheduler({'apscheduler.timezone': 'UTC'})
