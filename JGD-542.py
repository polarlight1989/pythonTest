import urllib.request
import json
from db import SqlLib
import threading

# 所有token列表
loginCustomerIdList = {"1122880238":"eq8jsgl4i6s9","7802171637":"eq8jsgl4i6s9"}

def getMccList(adAccountId,loginCustomerId,tokenId):
    dict = {"tokenId":tokenId,"adAccountId":adAccountId,"loginCustomerId":loginCustomerId}
    url = "https://sino-channel-api-gateway.meetsocial.cn/sino_channel_google/{tokenId}/v8/customers/{adAccountId}/googleAds:searchStream".format(**dict)
    headers = {"X-BUSINESS-PLATFORM-ID":dict["loginCustomerId"],"Content-Type":"application/json"}
    params = {"query" : "SELECT customer_manager_link.manager_customer,customer_manager_link.status FROM customer_manager_link WHERE customer_manager_link.status = 'ACTIVE'"}
    params = bytes(json.dumps(params),encoding = 'utf-8')
    req = urllib.request.Request(url=url, data=params, headers=headers, method='POST')
    response = urllib.request.urlopen(req).read()
    return json.loads(response)
def getLoginCustomerIdList(topMccId):
    dict = {"tokenId":loginCustomerIdList[topMccId]}
    url = "https://sino-channel-api-gateway.meetsocial.cn/sino_channel_google/{tokenId}/v8/customers:listAccessibleCustomers".format(**dict)
    headers = {"X-BUSINESS-PLATFORM-ID":topMccId,"Content-Type":"application/json"}
    req = urllib.request.Request(url=url, headers=headers, method='GET')
    response = urllib.request.urlopen(req).read()
    return json.loads(response)["resourceNames"]
def saveLoginCustomerAdAccountId(mccId):
    loginCustomerId = mccId
    tokenId = loginCustomerIdList[mccId]
    dict = {"mccId":mccId,"token":loginCustomerIdList[mccId]}
    url = "https://sino-channel-api-gateway.meetsocial.cn/sino_channel_google/{token}/v8/customers/{mccId}/googleAds:searchStream".format(**dict)
    headers = {"X-BUSINESS-PLATFORM-ID":mccId,"Content-Type":"application/json"}
    params = {"query" : "SELECT customer_client.id,customer_client.descriptive_name FROM customer_client"}
    params = bytes(json.dumps(params),encoding = 'utf-8')
    req = urllib.request.Request(url=url, data=params, headers=headers, method='POST')
    response = urllib.request.urlopen(req).read()
    responseJson = json.loads(response)
    for resultsDict in responseJson:
        for results in resultsDict['results']:
            data = {
                "ad_account_id" : results['customerClient']['id']
                ,"login_customer_id" : loginCustomerId
                ,"token_id" : loginCustomerId
                ,"mcc_id" : mccId
                }
            db.insert("google_mcc",data)
            print(results['customerClient']['id'])
    return responseJson
# r = getMccList("2395658437",'1122880238',"eq8jsgl4i6s9")
# mccList = r[0]['results']
def getMcc(adAccountId,loginCustomerId):
    dict = {"token":loginCustomerIdList[loginCustomerId],"adAccountId":adAccountId}
    url = "https://sino-channel-api-gateway.meetsocial.cn/sino_channel_google/{token}/v8/customers/{adAccountId}/googleAds:search".format(**dict)
    headers = {"X-BUSINESS-PLATFORM-ID":loginCustomerId,"Content-Type":"application/json"}
    params = {"query" : "SELECT customer_manager_link.manager_customer,customer_manager_link.status FROM customer_manager_link WHERE customer_manager_link.status = 'ACTIVE'"}
    params = bytes(json.dumps(params),encoding = 'utf-8')
    try:
        bindMccIdsStr = "[]"
        try:
            req = urllib.request.Request(url=url, data=params, headers=headers, method='POST')
            response = urllib.request.urlopen(req).read()
            responseJson = json.loads(response)
            bindMccIds = []
            for results in responseJson['results']:
                customerManagerLink = results['customerManagerLink']
                bindMccIds.append(customerManagerLink['managerCustomer'].split("/")[1])
            bindMccIdsStr = json.dumps(bindMccIds)
        except Exception as e:
            bindMccIdsStr = "[\"error\"]"
        sql = "UPDATE google_mcc SET bind_mcc_list = '"+bindMccIdsStr+"' WHERE ad_account_id = '"+adAccountId+"'"
        print(sql)
        db = SqlLib()
        db.update(sql)
    except Exception as e:
        print("error:",e)
    return

db = SqlLib()
data = db.select("SELECT ad_account_id,login_customer_id FROM google_mcc WHERE bind_mcc_list is null order by rand()")
sthread = []
for item in data:
    t = threading.Thread(target=getMcc, name=item[0], args=(item[0],item[1]))
    sthread.append(t)
    t.start()
    if len(sthread) == 50:
        index = 0
        for i in sthread:
            i.join(timeout=5)
            index+= 1
            print(str(index)  + " over")
        sthread = []
        print("over")
#getMcc("6403407711","1122880238")
# for item in mccList:
#     customerManagerLink = item['customerManagerLinks']
#     resourceName = customerManagerLink['resourceName']
#     resourceNameSplit = resourceName.split("/")
#     mccId = resourceNameSplit[3].split("~")[0]
#     data = {'ad_account_id':resourceNameSplit[1],'login_customer_id':resourceNameSplit[1],'mcc_id':mccId,'token_id':'eq8jsgl4i6s9'}
#     db.insert("google_mcc",data)


# saveLoginCustomerAdAccountId("7802171637")

# for loginCustomerId in loginCustomerIdList:
#     mccList = getLoginCustomerIdList(loginCustomerId)
#     for customers in mccList:
#         customerId = customers.split("/")[1]
#         print(customerId)