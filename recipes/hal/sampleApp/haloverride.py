import sys
import json
import requests

musicLocation =""
base_url = ""
#base_url = "http://localhost:8080/MUSIC/rest"
keyspaceName = ""
#keyspaceName = "hal_sdnc"
tableName = "replicas"


def parseConfig():
    global musicLocation, base_url, keyspaceName
    config = json.load(open('config.json'))
    musicLocation = config["musicLocation"]
    base_url = "http://" + musicLocation + ":8080/MUSIC/rest"
    keyspaceName = "hal_" + config["appName"]

def getReplica(id):
    response = requests.get(base_url+"/keyspaces/"+keyspaceName+"/tables/"+tableName+"/rows?id="+id)
    return response.json()["row 0"]

def acquireLock(lockref):
    response = requests.get(base_url+"/locks/acquire/"+lockref)
    return response.json()

def releaseLock(lockref):
    print "releasing lock: " + lockref
    response = requests.delete(base_url+"/locks/release/"+lockref)
    #return response.json()
    return

def getCurrentLockHolder(lockname):
    response = requests.get(base_url+"/locks/enquire/"+lockname)
    print response.text
    return response.json()

def releaseLocksUntil(lockname, lockref):
    acquire = acquireLock(lockref)
    while acquire["status"]=="FAILURE":
        if acquire["lock"]["message"]=="Lockid doesn't exist":
            print "[ERROR] Lock" , lockref, "cannot be found."
            return
        currentLockHolder = getCurrentLockHolder(lockname)
        if currentLockHolder["lock"]["lock-holder"] is not lockref:
            releaseLock(currentLockHolder["lock"]["lock-holder"])
        acquire = acquireLock(lockref)

def deleteLock(lockname):
    response = requests.delete(base_url + "/locks/delete/"+lockname)
    return response.json()


if __name__=="__main__":
    if len(sys.argv)<2:
        print "usage: haloverride <hal_id>"
        print " <hal_id> is the replica site to force to become active"
        print " this should be run from the same directory as the config.json"
        print " to read information about music location and keyspace information"
        exit(1)
    id = sys.argv[1]
    parseConfig()

    replicaInfo = getReplica(id)
    releaseLocksUntil(keyspaceName+".active", replicaInfo["lockref"])
