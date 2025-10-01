from typing import List

from models.Repo import Repo

selected_repos: List[Repo] = [
    # Repo.from_dict({'author': 'wagtail', 'name': 'wagtail', 'version': 'v7.1.1', 'wiki': None}),
     Repo.from_dict({'author': 'saleor', 'name': 'saleor', 'version': '3.21.19', 'wiki': None}),
     Repo.from_dict({'author': 'netbox-community', 'name': 'netbox', 'version': 'v4.4.1', 'wiki': None}),
     Repo.from_dict({'author': 'paperless-ngx', 'name': 'paperless-ngx', 'version': 'v2.18.4', 'wiki': None}),
     Repo.from_dict({'author': 'zulip', 'name': 'zulip', 'version': '11.2', 'wiki': None})
]


all_repos: List[Repo] = [
    Repo.from_dict({'author': 'wagtail', 'name': 'wagtail', 'version': 'v7.1.1', 'wiki': None}),
    Repo.from_dict({'author': 'saleor', 'name': 'saleor', 'version': '3.21.19', 'wiki': None}),
    Repo.from_dict({'author': 'netbox-community', 'name': 'netbox', 'version': 'v4.4.1', 'wiki': None}),
    Repo.from_dict({'author': 'paperless-ngx', 'name': 'paperless-ngx', 'version': 'v2.18.4', 'wiki': None}),
    Repo.from_dict({'author': 'zulip', 'name': 'zulip', 'version': '11.2', 'wiki': None})
]