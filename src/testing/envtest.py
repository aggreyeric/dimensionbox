from dotenv import load_dotenv
import os
load_dotenv(dotenv_path=os.path.join(os.path.dirname(os.path.dirname(__file__)),".env.values"))

# Access environment variables as if they came from the actual environment
BASE_PATH = os.getenv('BASE_PATH')


print(f'BASE_PATH: {BASE_PATH}')
