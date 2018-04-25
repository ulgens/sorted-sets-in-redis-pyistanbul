import json
from random import randint
from time import time
from uuid import UUID, uuid4

from redis import StrictRedis
from tqdm import tqdm

leaderboard = "users"
member_data = "additional_info"

# Redis vs. StrictRedis: https://stackoverflow.com/a/19024045/1726238
client = StrictRedis(
    # host="localhost",
    # port=6379,
    # password=None,
    db=1,
    charset="utf-8",
    decode_responses=True,
)


def timer(func):
    def wrapper(*args, **kwargs):
        ts = time()
        result = func(*args, **kwargs)
        te = time()
        print('func:%r took: %2.4f sec' % (func.__name__, te - ts))
        return result
    return wrapper


class MemberNotFoundException(Exception):
    def __init__(self):
        default_message = "A member with given id doesn't exist."
        super().__init__(default_message)


def member_required(func):
    def wrapper(id, *args, **kwargs):
        score = client.zscore(leaderboard, id)
        # Check for None explicitly, not boolean false
        if score is None:
            raise MemberNotFoundException

        return func(id, *args, **kwargs)
    return wrapper


@timer
def add_member(data: dict = None, score: float = 0) -> str:
    if not any([data, score]):
        raise ValueError("Empty member cannot be created.")

    data = data or {}

    id = uuid4()
    client.zadd(leaderboard, score, id)  # Refer to zadd docstring for parameters
    client.hset(member_data, id, json.dumps(data))

    return str(id)


@timer
@member_required
def get_member(id: str) -> dict:
    data = client.hget(member_data, id)
    data = json.loads(data or {})

    member = {
        "id": id,
        "score": client.zscore(leaderboard, id),
        "rank": client.zrevrank(leaderboard, id),
        **data
    }

    return member


@timer
@member_required
def update_member(id: str, data: dict = None, score: float = 0) -> bool:
    data = data or {}

    current_data = client.hget(member_data, id)
    current_data = json.loads(current_data)

    # Override existing keys with new values
    new_data = {**current_data, **data}

    # json.dumps handle JSON validation, call it first
    client.hset(member_data, id, json.dumps(new_data))
    client.zadd(leaderboard, score, id)

    return True


@timer
@member_required
def delete_member(id: UUID) -> bool:
    client.zrem(leaderboard, id)
    client.hdel(member_data, id)

    return True


@timer
def get_leaders(limit: int = 25) -> list:
    leaders = client.zrevrange(leaderboard, 0, limit-1)
    leaders_w_score_rank = []

    for l in leaders:
        rank = client.zrevrank(leaderboard, l)
        score = client.zscore(leaderboard, l)

        leaders_w_score_rank.append({"id": id, "score": score, "rank": rank})

    return leaders_w_score_rank


@timer
@member_required
def get_around(id: str, limit: int = 25) -> list:
    member_rank = client.zrevrank(leaderboard, id)

    if member_rank < round(limit / 2):
        start, end = 0, limit
    else:
        start = member_rank - round(limit / 2)
        end = start + limit

    around = client.zrevrange(leaderboard, start, end)
    around_w_score_rank = []

    for a in around:
        rank = client.zrevrank(leaderboard, a)
        score = client.zscore(leaderboard, a)

        around_w_score_rank.append({"id": id, "score": score, "rank": rank})

    return around_w_score_rank


def load_dummy_data(size: int = 1000000):
    for i in tqdm(range(size)):
        client.zadd(leaderboard, randint(0, size), f"member#{i}")

