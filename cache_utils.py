import redis
import hashlib
import json

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

def get_cache_key(question: str):
    return hashlib.sha256(question.lower().encode()).hexdigest()

def get_cached_result(question):
    return r.get(get_cache_key(question))

def set_cached_result(question, result):
    r.set(get_cache_key(question), json.dumps(result), ex=3600)  # 1 hour