import time
import datetime

unix_timestamp = int(time.time() * 1000)
print(unix_timestamp)

# standard_time = datetime.datetime.fromtimestamp(unix_timestamp)
# Convert to seconds and microseconds
timestamp_sec = unix_timestamp // 1000
timestamp_microsec = (unix_timestamp % 1000) * 1000

# Create a datetime object
standard_time = datetime.datetime.fromtimestamp(timestamp_sec).replace(microsecond=timestamp_microsec)
print(standard_time)