from tweepy import Client
import json
from datetime import datetime
from dotenv import load_dotenv
import os 

load_dotenv()

# Cadastrar as chaves de acesso
client = Client(
  bearer_token=os.environ.get('BEARER_TOKEN'), 
  consumer_key=os.environ.get('CONSUMER_KEY'), 
  consumer_secret=os.environ.get('CONSUMER_SECRET'), 
  access_token =os.environ.get('ACCES_TOKEN'), 
  access_token_secret=os.environ.get('ACCESS_TOKEN_SECRET'),
  return_type=dict)

search = '49ers'

q = '49ers lang:pt -is:retweet'

res = client.search_recent_tweets(query=q)

out = open(f'./results/colected_tweets_{datetime.now().strftime("%Y-%m-%d-%H-%M-%S")}.txt', 'w')

out.write(json.dumps(res))

out.close()
