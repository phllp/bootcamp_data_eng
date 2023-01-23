import json
from tweepy import OAuthHandler, Stream, StreamListener
from datetime import datetime

i = 0

# Cadastrar as chaves de acesso
consumer_key = 'ppm6iH8hpiaL2EBQhxF4LxTho'
consumer_secret = 'zw9T4irknnkt3gXnPjvQaLmGpojCPmUrDHQuk8zg2PwLuR5EDJ'

acess_token = '1617526238268248065-g3wKSdpZpPYjNug6hhlgywIv9EcKqF'
acess_token_secret = 'FpmwvOnPcjifv3vqcsyIL9lKzpakp4ABgUsQ2TTJaroLG'

# Definir um arquivo de saida pra armazenar os tweets coletados
out = open(f'colected_tweets_{datetime.now().strftime("%Y-%m-%d-%H-%M-%S")}.txt', 'w')

# Implementar uma classe para conexao com o Twitter
class MyListener(StreamListener):
  def on_data(self, data):
    itemString = json.dumps(data)
    out.write(itemString + "\n")
    if i >= 10:
      return False
    i += 1
    return True

  def on_error(self, status):
    out.close()
    print(status)

# Implementar a funcao main

if __name__ == '__main__':
  l = MyListener()
  auth = OAuthHandler(consumer_key=consumer_key, consumer_secret=consumer_secret)
  auth.set_access_token(acess_token=acess_token, acess_token_secret=acess_token_secret)

  stream = Stream(auth, l)
  stream.filter(track=['49ers'])