
import os
from dotenv import load_dotenv

def debug():
    load_dotenv()
    print("All environment variable names:")
    for key in sorted(os.environ.keys()):
        print(key)

if __name__ == "__main__":
    debug()
