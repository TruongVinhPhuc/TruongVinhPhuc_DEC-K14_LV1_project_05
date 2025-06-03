import IP2Location
from bson import ObjectId
import pymongo
import time
myclient = pymongo.MongoClient("mongodb://localhost:27017/")

db = myclient["test"]

summary_collection = db["summary"]
ip_database = IP2Location.IP2Location("ip.BIN")
ip_location_collection = db["ip_location"]
ip_location_error_collection = db["ip_location_error"]


def get_data():
    pipeline = [
        { "$group": { "_id": "$ip" } }
    ]
    cursor = summary_collection.aggregate(pipeline, allowDiskUse=True)
    return [doc["_id"] for doc in cursor]


def process_ip(ip: str):
    res = ip_database.get_all(ip)
    return res.country_long, res.region, res.city, res.district


def main():
    start = time.time()
    ip_location_collection.delete_many({})
    ip_location_error_collection.delete_many({})
    data = get_data()

    batch = []
    batch_len = 0

    for ip in data:
        try:
            country_long, region, city, district = process_ip(ip)
            batch.append({
                "ip": ip,
                "country_long": country_long,
                "region": region,
                "city": city,
                "district": district
            })
            batch_len += 1
            if batch_len == 100000:
                ip_location_collection.insert_many(batch)
                batch = []
                batch_len = 0

        except Exception as e:
            print(f"Error processing IP {ip}: {e}")
            ip_location_error_collection.insert_one({
                "ip": ip,
                "error": str(e),
            })
    print("IP location data processing completed.")

    end = time.time()
    print(f"Time taken: {end - start} seconds")

main()
