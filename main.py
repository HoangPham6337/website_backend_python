import redis
import uuid
import bcrypt
import os
import pymongo
import pandas as pd
import re
import json

from datetime import timedelta
from tabulate import tabulate
from redisbloom.client import Client
from pymongo.database import Database
from pymongo.cursor import Cursor
from typing import Optional
from time import perf_counter as timer

CACHE_EXPIRATION = 60
SESSION_TIMEOUT = 360
# MONGO_CLIENT: pymongo.MongoClient = pymongo.MongoClient("mongodb://localhost:27017/")
# REDIS_CLIENT = redis.Redis(host="localhost", port=6379, decode_responses=True)
# REDIS_BLOOM_CLIENT = Client()
MONGO_CLIENT: pymongo.MongoClient =  pymongo.MongoClient("the_login_detail_has_been_deleted")
REDIS_CLIENT = redis.Redis(host="the_login_detail_has_been_deleted", port=6379, password="the_login_detail_has_been_deleted", decode_responses=True)
REDIS_BLOOM_CLIENT = Client(host="the_login_detail_has_been_deleted", port=6379, password="the_login_detail_has_been_deleted")
REDIS_CACHE_CLIENT = redis.Redis(host="localhost", port=6379, decode_responses=True)
DATABASE = MONGO_CLIENT["website"]
CUCKOO_FILTER = "usernames"


# Manage hashing and verifying password
def hash_password(password: str) -> bytes:
    """A wrapper function to hash the password. Returns the hashed password in string."""
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt())


def verify_password(password: str, password_hashed: str) -> bool:
    """A wrapper function to verify entered password to the hashed one."""
    return bcrypt.checkpw(password.encode("utf-8"), password_hashed.encode("utf-8"))


# Function to manage user account
def check_username_exist(rb_client: Client, cuckoo_filter: str, username: str) -> bool:
    """A function to check if username has been taken or not."""
    return rb_client.cfExists(cuckoo_filter, username)


def user_register(
    redis_client: redis.Redis,
    rb_client: Client,
    cuckoo_filter: str,
    username: str,
    password: str,
) -> str:
    """A function to add the user to Redis database. Also add the username to a cuckoo filter."""
    if check_username_exist(rb_client, cuckoo_filter, username):
        return "Username already exists."

    redis_client.set(f"user:{username}:password", hash_password(password))
    rb_client.cfAdd(cuckoo_filter, username)
    create_user_basket(redis_client, username)
    return "User created successfully."


def user_delete(
    redis_client: redis.Redis, rb_client: Client, cuckoo_filter: str, username: str
):
    """A function to delete the user from the Redis database."""
    if not check_username_exist(rb_client, cuckoo_filter, username):
        return "Username doesn't exist."
    rb_client.delete(username)

    redis_client.delete(f"user:{username}:password")
    return "Account deleted."


# Manage user login, logout, current session
def create_session(redis_client: redis.Redis, username: str) -> str:
    """A function to create a session for user to stay logged-in. Returns the session_id in string."""
    session_id = str(uuid.uuid4())
    session_key = f"session:{session_id}"
    redis_client.hset(
        session_key,
        mapping={
            "username": username,
        },
    )
    redis_client.expire(session_key, SESSION_TIMEOUT)
    return session_id


def session_valid_check(redis_client: redis.Redis, session_id: str) -> bool:
    """A function to check if the current session is valid or not."""
    session_key = f"session:{session_id}"
    if redis_client.exists(session_key):
        return redis_client.ttl(session_key) > 0
    return False


def user_login(
    redis_client: redis.Redis,
    redis_cache_client: redis.Redis,
    rb_client: Client,
    cuckoo_filter: str,
    username: str,
    password: str,
) -> tuple[str, bool, str]:
    """A function for user to log in to the website. Returns a tuple: (Message, Result, SessionID if success else None)"""
    if not check_username_exist(rb_client, cuckoo_filter, username):
        return ("Username or password is wrong, please check again!", False, "None")
    user_password = redis_client.get(f"user:{username}:password")
    if user_password and verify_password(password, user_password):
        ssid = create_session(redis_cache_client, username)
        return ("Password is correct", True, ssid)
    else:
        return ("Username or password is wrong, please check again!", False, "None")


def user_logout(redis_client: redis.Redis, session_id: str):
    """A function to logout of the website."""
    session_key = f"session:{session_id}"
    if redis_client.exists(session_key):
        redis_client.delete(session_key)


# Manage user's product basket
def create_user_basket(redis_client: redis.Redis, username: str) -> None:
    """
    A function to initialize the user basket as a Redis list.
    """
    basket_key = f"user:{username}:basket"
    if not redis_client.exists(basket_key):
        redis_client.lpush(basket_key, "init")  # Add a placeholder
        redis_client.ltrim(basket_key, 1, 0)  # Trim to effectively remove placeholder


def check_user_basket_exist(redis_client: redis.Redis, username: str) -> bool:
    """
    A function to check if the user basket has been created.
    """
    return redis_client.exists(f"user:{username}:basket") == 1


def add_item_to_basket(
    redis_client: redis.Redis, username: str, item_data: dict, collection_name: str
) -> bool:
    """A function to add an item to the basket. Returns True if added successfully else False."""
    basket_key = f"user:{username}:basket"
    # Create a dictionary for the item and add it as a JSON string to the Redis list
    if item_data:
        item = {
            "collection": collection_name,
            "_id": item_data["_id"],
            "name": item_data["Name"],
        }
        redis_client.lpush(basket_key, json.dumps(item))  # Add the item to the list
        return True
    else:
        return False


def remove_item_from_basket(
    redis_client: redis.Redis, username: str, item_id: int, collection_name: str
) -> bool:
    """A function to remove an item to the basket. \n
    Returns True if added successfully else False."""
    basket_key = f"user:{username}:basket"

    items = redis_client.lrange(basket_key, 0, -1)

    for item_json in items:
        item = json.loads(item_json)
        if (
            int(item.get("_id")) == item_id
            and item.get("collection") == collection_name
        ):
            # Convert the item back to JSON and remove it from the list
            redis_client.lrem(basket_key, 1, item_json)
            return True

    return False


def display_basket(redis_client: redis.Redis, username: str):
    """A function to print all items in the basket."""
    basket_key = f"user:{username}:basket"
    # Get all items in the basket list
    items = redis_client.lrange(basket_key, 0, -1)

    for item_json in items:
        product = json.loads(item_json)  # Decode JSON item
        for k, v in product.items():
            print(f"{k}: {v}")
        print("---")


def len_basket(redis_client: redis.Redis, username: str) -> int:
    """A function to get the basket length."""
    basket_key = f"user:{username}:basket"
    return redis_client.llen(basket_key)


# Manage data with MongoDB
# Get all products with their respected category
def get_all_documents(database: Database) -> dict:
    """A function to return all documents within the database. Returns a dictionary of all documents."""
    all_documents = {}
    for collection_name in database.list_collection_names():
        collection = database[collection_name]
        all_documents[collection_name] = list(collection.find())
    return all_documents


def display_all_products(database: Database):
    collections_data = get_all_documents(database)
    display_data = {
        collection: [doc.get("Name") for doc in data]
        for collection, data in collections_data.items()
    }
    headers = [key for key in display_data]
    print(tabulate(display_data, headers=headers, tablefmt="grid"))


# Query products within a category
def get_all_document_in_collection(database: Database, collection_name: str) -> list:
    """A function to return all documents within a collection. Returns a list of documents."""
    return database[collection_name].find().to_list()


def get_document_in_collection(
    database: Database, product_id: int, collection_name: str
) -> dict:
    """A function to return a document within a collection. """
    return database[collection_name].find({"_id": product_id}).to_list()[0]


def create_word_query(query: str) -> str:
    """A helper function to convert a string query to regex."""
    escaped_query = re.escape(query)
    return rf"(?i)\b{escaped_query}\b"


def fuzzy_search(database: Database, collection_name: str, query: str) -> list:
    """A function to search for matches of a query in a collection. Return the list of all possible matches."""
    return (
        database[collection_name]
        .find({"Name": {"$regex": f"{create_word_query(query)}"}})
        .to_list()
    )


# Use Redis as a cache
def get_product_details(
    redis_cache_client: redis.Redis, database: Database, product_id: int, collection: str
) -> dict:
    """
    This function fetch the product details.
    First it queries Redis for the cache version.
    If the cache is not available, it will queries MongoDB.
    Import the retrieved data to Redis for caching, use CACHE_EXPIRATION as default value.
    Returns the document data as a dictionary.
    """
    cache_key_data = f"product:data:{collection}:{product_id}"
    cache_key_name = f"product:name:{collection}:{product_id}"

    cached_data = redis_cache_client.get(cache_key_data)
    if cached_data:
        print("Cache hit for product data")
        return json.loads(cached_data)

    print("Cache miss, querying database.")
    product_data = get_document_in_collection(database, product_id, collection)
    if product_data:
        redis_cache_client.set(cache_key_data, json.dumps(product_data), ex=CACHE_EXPIRATION)
        product_name = str(product_data.get("Name"))
        redis_cache_client.set(cache_key_name, product_name, ex=CACHE_EXPIRATION)
    return product_data


def search_for_specific_document(
    redis_cache_client: redis.Redis, database: Database, collection_name: str, query: str
):
    """
    A function to print a specific product detail.\n
    Utilizes fuzzy search to locate the product.\n
    Return the product detail as a dict.\n
    """
    results = fuzzy_search(database, collection_name, query)
    if not results or len(results) == 0:
        print("No product found.")
        return
    for idx, result in enumerate(results, start=1):
        print(f"{idx}. {result.get("Name")}")

    display_which = int(input("Enter a number to display the product in detail: "))
    while True:
        try:
            index = results[display_which - 1]["_id"]
            product_data = get_product_details(
                redis_cache_client, database, index, collection_name
            )
            return product_data
        except IndexError:
            print("Invalid choice!")

            
def delete_all_cache(redis_cache_client: redis.Redis):
    """
    Deletes all cache entries related to product data and names.
    This will scan Redis for keys that match the patterns used for product data caching.
    """
    # Patterns for product cache keys
    data_pattern = "product:data:*"
    name_pattern = "product:name:*"
    
    # Helper function to delete keys matching a given pattern
    def delete_keys(pattern):
        cursor = 0  # Starting cursor for SCAN
        while True:
            cursor, keys = redis_cache_client.scan(cursor=cursor, match=pattern, count=100)
            if keys:
                redis_cache_client.delete(*keys)
            if cursor == 0:
                break

    # Delete all product data and product name caches
    delete_keys(data_pattern)
    delete_keys(name_pattern)
    # print("All product cache entries have been deleted.")


def pretty_print_product_info(product_info: dict):
    if product_info:
        print("Product Information:")
        print("=====================")
        for key, value in product_info.items():
            print(f"{key:<20}: {value}")
        print("=====================")


if __name__ == "__main__":
    try:
        username = ""
        session_id = ""
        while True:
            os.system("clear")
            if not session_valid_check(REDIS_CACHE_CLIENT, session_id):
                delete_all_cache(REDIS_CLIENT)
                choice = int(input("1. Create account.\n2. Log in\n3. Display products\n"))
                match choice:
                    case 1:
                        print(
                            user_register(
                                REDIS_CLIENT,
                                REDIS_BLOOM_CLIENT,
                                CUCKOO_FILTER,
                                input("Enter your username: "),
                                input("Enter your password: "),
                            )
                        )
                    case 2:
                        username = input("Username: ")
                        password = input("Password: ")
                        login_result = user_login(
                            REDIS_CLIENT,
                            REDIS_CACHE_CLIENT,
                            REDIS_BLOOM_CLIENT,
                            CUCKOO_FILTER,
                            username,
                            password,
                        )
                        if not check_user_basket_exist(REDIS_CLIENT, username):
                            create_user_basket(REDIS_CLIENT, username)
                        session_id = login_result[-1]
                        print(login_result[0])
                    case 3:
                        display_all_products(DATABASE)
                        pretty_print_product_info(search_for_specific_document(REDIS_CLIENT, DATABASE, input("Enter collection name: "), input("Enter product name: ")))
                    case 4:
                        user_delete(
                            REDIS_CLIENT,
                            REDIS_BLOOM_CLIENT,
                            CUCKOO_FILTER,
                            input("Username: "),
                        )
                input()
            else:
                account_details = REDIS_CACHE_CLIENT.hget(f"session:{session_id}", "username")
                print(f"Username: {account_details}")
                print(f"Item in basket: {len_basket(REDIS_CLIENT, username)}")
                action = input(
                    "1. Display products\n2. Get a product details\n3. Search product by name\n4. Display basket\n5. Add item to basket\n6. Remove item from basket\n7. Log out\n"
                )
                match action:
                    case "1":
                        display_all_products(DATABASE)
                    case "2":
                        product_id = int(input("Enter product ID: "))
                        collection = input("Enter collection name: ")
                        start = timer()
                        data = get_product_details(
                            REDIS_CACHE_CLIENT, DATABASE, product_id, collection
                        )
                        end = timer()
                        print(f"Data retrieve time: {end - start}")
                        if data:
                            pretty_print_product_info(data)
                        else:
                            print("No result found.")
                    case "3":
                        query = input("Enter product name to search: ")
                        collection = input("Enter collection name: ")
                        pretty_print_product_info(search_for_specific_document(
                            REDIS_CACHE_CLIENT, DATABASE, collection, query
                        ))
                    case "4":
                        display_basket(REDIS_CLIENT, username)
                    case "5":
                        query = input("Enter product name to search: ")
                        collection = input("Enter collection name: ")
                        product_data = search_for_specific_document(
                            REDIS_CACHE_CLIENT, DATABASE, collection, query
                        )
                        if product_data:
                            add_item_result = add_item_to_basket(
                                REDIS_CLIENT, username, product_data, collection
                            )
                            print("Item added.")
                        else:
                            print("Failed to add item.")
                    case "6":
                        display_basket(REDIS_CLIENT, username)
                        remove_item_result = remove_item_from_basket(
                            REDIS_CLIENT,
                            username,
                            int(input("Item ID: ")),
                            input("Collection Name: "),
                        )
                        print(
                            "Item removed."
                            if remove_item_result
                            else "Failed to remove item."
                        )
                    case "7":
                        user_logout(REDIS_CACHE_CLIENT, session_id)
                        session_id = ""
                input()
    except Exception:
        user_logout(REDIS_CACHE_CLIENT, session_id)
        delete_all_cache(REDIS_CACHE_CLIENT)
    except KeyboardInterrupt:
        user_logout(REDIS_CACHE_CLIENT, session_id)
        delete_all_cache(REDIS_CACHE_CLIENT)
