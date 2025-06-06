import datetime
import random
import string

def log(message):
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {message}")

def short_id(length=4):
    chars = string.ascii_letters + string.digits  # Base62
    return ''.join(random.choices(chars, k=length))