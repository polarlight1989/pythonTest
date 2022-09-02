import urllib.request
import json


def getCampaignType(adAccountId,campaignId):
    # campaign
    url = "https://sino-channel-api-gateway.meetsocial.cn/sino_channel_google/eq8jsgl4i6s9/v9/customers/{adAccountId}/googleAds:searchStream".format(
        adAccountId = adAccountId
        )
    #print(url)
    headers = {"X-AD-ACCOUNT-ID":adAccountId,"Content-Type":"application/json"}
    params = {
        "query" : "SELECT campaign.id,campaign.advertising_channel_type FROM campaign WHERE campaign.id = '{campaignId}'".format(campaignId=campaignId)
    }
    #print(params)
    params = bytes(json.dumps(params),encoding = 'utf-8')
    req = urllib.request.Request(url=url, data=params, headers=headers, method='POST')
    response = urllib.request.urlopen(req).read()
    responseJson = json.loads(response)
    return responseJson[0]["results"][0]["campaign"]["advertisingChannelType"]
with open('/Volumes/develop/web/python/mediahub/41.txt','r') as f:
    for line in f:
        s = line.split("\t")
        adAccountId = s[0].replace(" ","")
        campaignId = s[1].replace(" ","").replace("\n","")
        print("广告账号:{adAccountId},Campaign:{campaignId},类型:{campaignType}".format(adAccountId=adAccountId,campaignId=campaignId,campaignType=getCampaignType(adAccountId,campaignId)))
exit()
adAccountId = "9378271262"
campaignId = "14167558745"

# campaign
url = "https://sino-channel-api-gateway.meetsocial.cn/sino_channel_google/eq8jsgl4i6s9/v9/customers/{adAccountId}/googleAds:searchStream".format(
    adAccountId = adAccountId
    )
headers = {"X-AD-ACCOUNT-ID":adAccountId,"Content-Type":"application/json"}
params = {
    "query" : "SELECT campaign.id,campaign.advertising_channel_type,metrics.cost_micros FROM campaign WHERE campaign.id = '{campaignId}'".format(campaignId=campaignId)
}
params = bytes(json.dumps(params),encoding = 'utf-8')
req = urllib.request.Request(url=url, data=params, headers=headers, method='POST')
response = urllib.request.urlopen(req).read()
responseJson = json.loads(response)
campaignCost = int(responseJson[0]["results"][0]["metrics"]["costMicros"])
 

# adGroupAd
url = "https://sino-channel-api-gateway.meetsocial.cn/sino_channel_google/eq8jsgl4i6s9/v9/customers/{adAccountId}/googleAds:searchStream".format(
    adAccountId = adAccountId
    )
headers = {"X-AD-ACCOUNT-ID":adAccountId,"Content-Type":"application/json"}
params = {
    "query" : "SELECT ad_group.id,metrics.cost_micros FROM ad_group WHERE campaign.id = '{campaignId}'".format(campaignId=campaignId)
}
params = bytes(json.dumps(params),encoding = 'utf-8')
req = urllib.request.Request(url=url, data=params, headers=headers, method='POST')
response = urllib.request.urlopen(req).read()
responseJson = json.loads(response)
results = responseJson[0]["results"]
adGroupCost = 0
for r in results:
    adGroupCost += int(r["metrics"]["costMicros"])

print("campaign Cost : "+str(campaignCost / 1000000))
print("adGroupAd Cost : "+str(adGroupCost / 1000000))
print("差值 : " + str(campaignCost - adGroupCost) )