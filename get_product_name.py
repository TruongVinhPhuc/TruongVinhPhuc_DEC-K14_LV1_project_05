from bs4 import BeautifulSoup
import pymongo
import requests
import time

def get_summary_collection():
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client['test']
    return db['summary']

def get_product_collection():
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client['test']
    return db['product']

def get_product_error_collection():
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client['test']
    return db['product_error']

def get_unique_product_ids(collection, collection_field):
    pipeline = [
        {"$match": {"collection": collection_field}},
        {"$group": {"_id": "$product_id"}},
        {"$project": {"product_id": "$_id", "_id": 0}}
    ]
    product_ids = [doc['product_id'] for doc in collection.aggregate(pipeline, allowDiskUse=True)]
    return product_ids

def process_task(collection_field, summary_collection=None, product_collection=None, product_error_collection=None):
    if summary_collection is None:
        summary_collection = get_summary_collection()
    if product_collection is None:
        product_collection = get_product_collection()
    if product_error_collection is None:
        product_error_collection = get_product_error_collection()

    product_ids = get_unique_product_ids(summary_collection, collection_field)
    print(f"[{collection_field}] Found {len(product_ids)} unique product IDs")

    headers = {
        "accept": "*/*",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
        "content-type": "text/plain;charset=UTF-8",
        "origin": "https://www.glamira.com.mx",
        "referer": "https://www.glamira.com.mx/",
        "sec-ch-ua": "\"Google Chrome\";v=\"135\", \"Not-A.Brand\";v=\"8\", \"Chromium\";v=\"135\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "no-cors",
        "sec-fetch-site": "cross-site",
        "sec-fetch-storage-access": "active",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
    }

    session = requests.Session()
    products_name = []
    error_products = []

    for product_id in product_ids:
        current_doc = summary_collection.find_one({"product_id": product_id, "collection": collection_field},sort=[("_id", pymongo.ASCENDING)])

        while current_doc:
            try:
                print(f"Trying product_id: {product_id}, URL: {current_doc['current_url']}")
                response = session.get(current_doc['current_url'], headers=headers, timeout=10)

                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    title_span = soup.find("h1", class_="page-title")
                    if title_span:
                        product_name = title_span.find("span").text.strip()
                        products_name.append({
                            "product_id": product_id,
                            "product_name": product_name
                        })
                        break
                    
            except Exception as e:
                print(f"Error fetching product_id {product_id}: {e} on URL {current_doc['current_url']}")
                error_products.append({
                    "product_id": product_id,
                    "error": str(e),
                    "url": current_doc['current_url']
                })
                
            finally:
                last_accessed_id = current_doc['_id']
                current_doc = summary_collection.find_one({"collection": collection_field,"product_id": product_id,"_id": {"$gt": last_accessed_id}},sort=[("_id", pymongo.ASCENDING)])

    if products_name:
        product_collection.insert_many(products_name, ordered=False)

    if error_products:
        product_error_collection.insert_many(error_products, ordered=False)

    
def main():
    product_collection = get_product_collection()
    product_collection.create_index([("product_id", pymongo.ASCENDING)], unique=True)
    
    product_collection.delete_many({})
    
    product_error_collection = get_product_error_collection()
    product_error_collection.delete_many({})
    
    summary_collection = get_summary_collection()
    # start_time = time.time()
    # summary_collection.create_index([("collection", 1), ("product_id", 1)])
    # end_time = time.time()
    # print(f"Index creation time: {end_time - start_time} seconds")
    # print("Summary collection index created")
    
    collection_field = ['view_product_detail', 'select_product_option', 'select_product_option_quality']
    
    start = time.time()
    
    process_task(collection_field[0], summary_collection=summary_collection, product_collection=product_collection, product_error_collection=product_error_collection)
    # process_task(collection_field[1], summary_collection=get_summary_collection(), product_collection=product_collection, product_error_collection=product_error_collection)
    # process_task(collection_field[2], summary_collection=get_summary_collection(), product_collection=product_collection, product_error_collection=product_error_collection)
    end = time.time()
    print(f"Time taken: {end - start} seconds")
    # process_task(collection_field, summary_collection=summary_collection, product_collection=product_collection, product_error_collection=product_error_collection)
    
if __name__ == "__main__":
    main()
    
        
