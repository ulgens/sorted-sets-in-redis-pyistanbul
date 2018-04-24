import json
from uuid import UUID, uuid4

from redis import StrictRedis

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

        func(id, *args, **kwargs)
    return wrapper


def add_member(fullname: str = "", score: float = 0) -> UUID:
    if not any([fullname, score]):
        raise ValueError("Empty member cannot be created.")

    id = uuid4()
    client.zadd(leaderboard, score, id)  # Refer to zadd docstring for parameters
    client.hset(member_data, id, json.dumps({"fullname": fullname}))

    return id


@member_required
def get_member(id: UUID) -> dict:
    data = client.hget(member_data, id)
    data = json.loads(data)

    member = {
        "id": id,
        "score": client.zscore(leaderboard, id),
        "rank": client.zrank(leaderboard, id),
        **data
    }

    return member


@member_required
def update_member(id: UUID, data: dict = None, score: float = 0) -> bool:
    data = data or {}

    current_data = client.hget(member_data, id)
    current_data = json.loads(current_data)

    # Override existing keys with new values
    new_data = {**current_data, **data}

    client.zadd(leaderboard, score, id)
    client.hset(member_data, id, json.dumps(new_data))

    return True


@member_required
def delete_member(id: UUID) -> bool:
    client.zrem(leaderboard, id)
    client.hdel(member_data, id)

    return True
