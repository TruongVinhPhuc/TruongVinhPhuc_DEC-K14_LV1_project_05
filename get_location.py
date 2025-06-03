import pymongo
import IP2Location
from bson import ObjectId

myclient = pymongo.MongoClient("mongodb://localhost:27017/")

db = myclient["test"]

summary_collection = db["summary"]
ip_database = IP2Location.IP2Location("ip.BIN")
ip_location_collection = db["ip_location"]
ip_location_error_collection = db["ip_location_error"]


def get_data(last_access_id: ObjectId, limit: int):
    return summary_collection.find({"_id": {"$gt": last_access_id}}).limit(limit)


def process_ip(ip: str):
    res = ip_database.get_all(ip)
    return res.country_long, res.region, res.city, res.latitude, res.longtitude, res.district


def main():
    ip_location_collection.delete_many({})
    ip_location_error_collection.delete_many({})
    min_id = summary_collection.find().sort({"_id": 1}).limit(1) # Find minimum _id of the collection as using find() alone doesn't guarantee the order of the documents 
    last_access_id = min_id[0]["_id"]
    limit = 10000
    data = list(get_data(last_access_id, limit))
    unique_ips = set()

    while data:
        valid_ips = []
        invalid_ips = []
        for item in data:
            # print(item["_id"])
            ip = item["ip"]
            if ip not in unique_ips:
                country, region, city, latitude, longtitude, district = process_ip(ip)
                unique_ips.add(ip)
                valid_ips.append(
                    {"summary_id": item["_id"], "ip": ip, "country": country, 'region': region, 'city': city, 'district': district}
                )

        if valid_ips:
            ip_location_collection.insert_many(valid_ips)
        if invalid_ips:
            ip_location_error_collection.insert_many(invalid_ips)

        last_access_id = data[-1]["_id"]
        # print(last_access_id)
        data = list(get_data(last_access_id, limit))
        # print(len(data))


main()
