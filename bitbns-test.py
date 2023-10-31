from bitbnspy import bitbns
import pprint,json,datetime,pytz
bitbnsObj = bitbns.publicEndpoints()


key = '2C863ADBD5296D66392455B8D3A4DB65'
secretKey = '3F0468C103141A452CCB1CD6759AD474'
bitbnsObj = bitbns(key, secretKey)

pprint.pp(bitbnsObj.fetchTickers())

bbData = bitbnsObj.fetchTickers()['data']

for key in bbData:
    bbData[key].update({"timestamp":str(datetime.datetime.now(tz=pytz.timezone('Asia/Kolkata')))})
with open("bitbns-response.json", 'w') as file:
    json.dump(bbData, file, indent=4)