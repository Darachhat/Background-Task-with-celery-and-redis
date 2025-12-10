import time

from celery.result import AsyncResult

from worker import random_number, app

time.sleep(5)  # Wait for the worker to be ready

result_future = random_number.delay(100) # promise to get a random number up to 100
result = AsyncResult(result_future.id, app=app ) # get the AsyncResult using the task id

print('Submitted task') 
print(result.state) # PENDING


while True:
    if result.ready():  # Check if the task is complete
        print('Task completed')
        print('Random number:', result.get())  # Get the result
        break
    else:
        print('Task not yet completed, current state:', result.state)
        time.sleep(1)  # Wait before checking again
