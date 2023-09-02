
from datetime import datetime, timezone
from dateutil import parser as date_parse

import pytest

from app.truth_service import TruthService


@pytest.fixture(scope="module")
def truth():
    return TruthService()


def as_datetime(date_str):
    """Datetime formatter function. Ensures timezone is UTC. Consider moving to Api class."""
    return date_parse.parse(date_str).replace(tzinfo=timezone.utc)


def test_get_user(truth):
    user = truth.get_user(username="realDonaldTrump")
    assert list(user.keys()) == [
        'id', 'username', 'acct', 'display_name',
        'locked', 'bot', 'discoverable', 'group', 'created_at',
        'note', 'url', 'avatar', 'avatar_static', 'header', 'header_static',
        'followers_count', 'following_count', 'statuses_count', 'last_status_at',
        'verified', 'location', 'website',
        'accepting_messages', 'chats_onboarded', 'feeds_onboarded',
        'show_nonmember_group_statuses', 'pleroma', 'emojis', 'fields'
    ]
    assert isinstance(user["id"], str)


def test_pull_statuses(truth):
    username = "truthsocial"

    # COMPLETE PULLS

    # it fetches a timeline of the user's posts:
    full_timeline = list(truth.get_user_timeline(username=username, replies=False, verbose=True))
    assert len(full_timeline) > 25 # more than one page

    # the posts are in reverse chronological order:
    latest, earliest = full_timeline[0], full_timeline[-1]
    latest_at, earliest_at = as_datetime(latest["created_at"]), as_datetime(earliest["created_at"])
    assert earliest_at < latest_at

    # EMPTY PULLS

    # can use created_after param for filtering out posts:
    next_pull = list(truth.get_user_timeline(username=username, replies=False, created_after=latest_at, verbose=True))
    assert not any(next_pull)

    # can use since_id param for filtering out posts:
    next_pull = list(truth.get_user_timeline(username=username, replies=False, since_id=latest["id"], verbose=True))
    assert not any(next_pull)

    # PARTIAL PULLS

    n_posts = 50 # two and a half pages worth, to verify everything is ok
    recent = full_timeline[n_posts]
    recent_at = as_datetime(recent["created_at"])

    # can use created_after param for filtering out posts:
    partial_pull = list(truth.get_user_timeline(username=username, replies=False, created_after=recent_at, verbose=True))
    assert len(partial_pull) == n_posts
    assert recent["id"] not in [post["id"] for post in partial_pull]

    # can use since_id param for filtering out posts:
    partial_pull = list(truth.get_user_timeline(username=username, replies=False, since_id=recent["id"], verbose=True))
    assert len(partial_pull) == n_posts
    assert recent["id"] not in [post["id"] for post in partial_pull]



    # POST INFO
    # contains status info
    assert list(latest.keys()) == ['id', 'created_at',
        'in_reply_to_id', 'quote_id', 'in_reply_to_account_id',
        'sensitive', 'spoiler_text', 'visibility', 'language', 'uri', 'url',
        'content', 'account', 'media_attachments', 'mentions', 'tags', 'card',
        'group', 'quote', 'in_reply_to', 'reblog', 'sponsored',
        'replies_count', 'reblogs_count', 'favourites_count', 'favourited', 'reblogged',
        'muted', 'pinned', 'bookmarked', 'poll', 'emojis', '_pulled'
    ]
    assert isinstance(latest["id"], str)
