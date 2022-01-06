import datetime
import functools

from apscheduler.schedulers.background import BackgroundScheduler

from .job_stores import job_stores

# Declaration of a scheduler that will manage a queue of tasks, useful for managing transactions
transaction_scheduler = BackgroundScheduler(jobstores=job_stores)


def init_scheduler():
    transaction_scheduler.start()
    # Here, we resume the paused jobs in the jobstore (in case of an ungraceful program exit)
    pending_jobs = transaction_scheduler.get_jobs()
    for job in pending_jobs:
        job.resume()


def repeat_deco(repeat_count, reschedule_count=0, always_reschedule=False):
    def deco_wrapper(func):
        # this notation is copying the information (name, docstring, ecc.) of the original function to the
        # function wrapper
        @functools.wraps(func)
        def func_wrapper(trans_id=None, *args, **kwargs):  # Here, we added the default parameter 'trans_id'
            counter = 0
            while counter < repeat_count:
                try:
                    # If the 'trans_id' is valid, we forward the parameter to the wrapped function 'func'
                    if trans_id is not None:
                        return func(trans_id, *args, **kwargs)
                    else:
                        # Otherwise, we call the wrapped function 'func' with the passed arguments (positional and
                        # keywords)
                        return func(*args, **kwargs)
                except:
                    counter += 1
            if func_wrapper.reschedule_count > 0 and trans_id is not None:
                if not always_reschedule:
                    func_wrapper.reschedule_count -= 1
                transaction_scheduler.add_job(func_wrapper,
                                              id=str(trans_id),
                                              run_date=datetime.datetime.now() + datetime.timedelta(seconds=5),
                                              args=[trans_id, *args], kwargs=kwargs, replace_existing=True,
                                              misfire_grace_time=3600)

            raise Exception(f'Failed execution in function: {func.__name__!r}')

        func_wrapper.reschedule_count = reschedule_count
        return func_wrapper

    return deco_wrapper
