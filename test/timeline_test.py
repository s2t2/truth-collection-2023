
import pytest
from datetime import datetime, timedelta, timezone

from app.truth_service import TruthService, to_utc


@pytest.fixture(scope="module")
def ts():
    return TruthService()

def test_to_utc():
    recent = datetime.now() - timedelta(days=7)
    recent_tz = to_utc(recent)
    assert recent_tz.tzname() == "UTC"


def test_get_user(ts):
    user = ts.get_user(username="realDonaldTrump")
    #breakpoint()
    assert list(user.keys()) == [
        'id', 'username', 'acct', 'display_name', 'locked', 'bot', 'discoverable', 'group', 'created_at',
        'note', 'url', 'avatar', 'avatar_static', 'header', 'header_static',
        'followers_count', 'following_count', 'statuses_count', 'last_status_at',
        'verified', 'location', 'website', 'accepting_messages',
        'chats_onboarded', 'feeds_onboarded', 'tv_onboarded', 'show_nonmember_group_statuses',
        'pleroma', 'tv_account', 'receive_only_follow_mentions', 'emojis', 'fields'
    ]
    assert isinstance(user["id"], str)


def test_pull_statuses(ts):
    username = "truthsocial"

    full_timeline = list(ts.get_user_timeline(username=username, replies=False, verbose=True))
    assert len(full_timeline) > 25 # more than one page

    # the posts are in reverse chronological order:
    latest, earliest = full_timeline[0], full_timeline[-1]
    #print(latest["created_at"], earliest["created_at"]) #> "2024-07-14T01:44:50.160Z"
    latest_at, earliest_at = to_utc(latest["created_at"]), to_utc(earliest["created_at"])
    assert earliest_at < latest_at

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
