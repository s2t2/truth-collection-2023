
class GroupProfile:

    def __init__(self, attrs:dict):
        self.attrs = attrs

    @property
    def group_id(self):
        return self.attrs.get("id")

    @property
    def owner_id(self) -> str:
        return self.attrs.get("owner").get("id") # {'id': '123'}

    @property
    def slug(self) -> str:
        return self.attrs.get("slug") #

    @property
    def url(self) -> str:
        return f"https://truthsocial.com/group/{self.slug}"

    @property
    def created_at(self) -> str:
        return self.attrs.get("created_at") # '2023-04-20T00:00:00.000Z'

    @property
    def membership_required(self) -> bool:
        return self.attrs.get("membership_required")

    @property
    def visibility(self) -> str:
        return self.attrs.get("group_visibility") #> 'everyone'

    @property
    def display_name(self) -> str:
        return self.attrs.get("display_name")

    @property
    def members_count(self) -> int:
        return self.attrs.get("members_count") # int

    @property
    def avatar_url(self) -> str:
        return self.attrs.get("avatar_static") or self.attrs.get("avatar")

    @property
    def header_url(self) -> str:
        return self.attrs.get("header_static") or self.attrs.get("header")

    @property
    def note_html(self):
        return self.attrs.get("note") # '<p>TOGETHER WE WILL MAKE AMERICA GREAT AGAIN! 🇺🇸</p>'

    @property
    def tags(self):
        # {'created_at': '2022-02-15T16:49:18.371Z',
        #    'id': 74,
        #    'last_status_at': '2023-02-17T15:24:36.418Z',
        #    'listable': True,
        #    'max_score': None,
        #    'max_score_at': None,
        #    'name': 'MakeAmericaGreatAgain',
        #    'requested_review_at': None,
        #    'reviewed_at': None,
        #    'trendable': True,
        #    'updated_at': '2023-02-17T15:24:36.507Z',
        #    'usable': True},
        return self.attrs.get("tags")

    @property
    def tag_names(self):
        return [tag["name"] for tag in self.tags]

    @property
    def tag_names_csv(self):
        return ",".join(self.tag_names)
