import logging
import os
import atexit
import uuid

import redis

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response
import sys

from helpers import consume_event, publish_event


DB_ERROR_STR = "DB error"


app = Flask("payment-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class UserValue(Struct):
    credit: int


def get_user_from_db(user_id: str) -> UserValue | None:
    try:
        # get serialized data
        entry: bytes = db.get(user_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
    if entry is None:
        # if user does not exist in the database; abort
        abort(400, f"User: {user_id} not found!")
    return entry


@app.post('/create_user')
def create_user():
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'user_id': key})

@app.get('/all_users')
def all_users():
    try:
        # Get all keys in the Redis database
        keys = db.keys('*')
        users = []
        for key in keys:
            user_entry: UserValue = get_user_from_db(key.decode('utf-8'))
            users.append({
                "user_id": key.decode('utf-8'),
                "credit": user_entry.credit
            })
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify(users)

@app.post('/batch_init/<n>/<starting_money>')
def batch_init_users(n: int, starting_money: int):
    n = int(n)
    starting_money = int(starting_money)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(UserValue(credit=starting_money))
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for users successful"})


@app.get('/find_user/<user_id>')
def find_user(user_id: str):
    user_entry: UserValue = get_user_from_db(user_id)
    return jsonify(
        {
            "user_id": user_id,
            "credit": user_entry.credit
        }
    )


@app.post('/add_funds/<user_id>/<amount>')
def add_credit(user_id: str, amount: int):
    user_entry: UserValue = get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit += int(amount)
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


@app.post('/pay/<user_id>/<amount>')
def remove_credit(user_id: str, amount: int):
    app.logger.debug(f"Removing {amount} credit from user: {user_id}")
    user_entry: UserValue = get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit -= int(amount)
    if user_entry.credit < 0:
        abort(400, f"User: {user_id} credit cannot get reduced below zero!")
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)

# PROCESS PAYMENT, similar to remove_credit but not linked to a route.
def process_payment(user_id: str, amount: int):
    user_entry: UserValue = get_user_from_db(user_id)
    if user_entry.credit < amount:
        abort(400, f"User {user_id} does not have enough credit")
    user_entry.credit -= amount
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        abort(400, "DB error")

def handle_stock_subtracted(event):
    user_id = event['user_id']
    amount = event['total_cost']
    order_id = event['order_id']
    # Process payment
    try:
        process_payment(user_id, amount)
        # Publish payment_processed event, handled by the order service
        publish_event("payment_processed", {"order_id": order_id, "user_id": user_id, "total_cost": amount})
    except Exception as e:
        print(e)
        # Publish payment_failed event, handled by the order service.
        publish_event("payment_failed", {"order_id": order_id, "user_id": user_id, "total_cost": amount, "reason": str(e)})


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)

# PASSIVELY CONSUME THE STOCK SUBTRACTED EVENT TO PROCESS PAYMENT
consume_event("stock_subtracted", handle_stock_subtracted)