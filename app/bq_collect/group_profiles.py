import os
from time import sleep
from datetime import date

from pandas import DataFrame

from app.truth_service import TruthService, VERBOSE_MODE
from app.bq_service import BigQueryService, generate_timestamp
from app.db import EXPORTS_DIR
from imports.groups import GROUPS


class Group:

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



if __name__ == "__main__":

    ts = TruthService()
    bq = BigQueryService()

    records = []
    for group_info in GROUPS[0:3]:
        print("-------------")
        print(group_info)
        slug = group_info["slug"]
        display_name = group_info["display_name"]

        try:
            # we need to search by the name, not the slug
            results = ts.client.search_simpler(resource_type="groups", query=display_name)
            # match results on display name:
            group_info = [g for g in results if g["display_name"].upper() == display_name.upper()][0]

            group = Group(group_info)

            records.append({
                "group_id": group.group_id,
                "owner_id": group.owner_id,
                "slug": group.slug,
                "created_at": group.created_at,
                "membership_required": group.membership_required,
                "visibility": group.visibility,
                "display_name": group.display_name,
                "members_count": group.members_count,
                "avatar_url": group.avatar_url,
                "header_url": group.header_url,
                "note_html": group.note_html,
                "tag_names": group.tag_names,
                "collected_at": generate_timestamp()
            })
        except Exception as err:
            print("OOPS", err)
            #breakpoint()
            continue
            #break # assume we are getting access restricted

        #sleep(2)

    print("SAVING TO CSV...")
    groups_df = DataFrame(records)
    print(groups_df.head())
    csv_filepath = os.path.join(EXPORTS_DIR, f"group_profiles_{date.today().strftime('%Y%m%d')}.csv")
    groups_df.to_csv(csv_filepath, mode="a")
    print(csv_filepath)

    print("SAVING TO BQ...")
    bq.insert_records_in_batches(bq.group_profiles_table, records)
