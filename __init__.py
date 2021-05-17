"""
https://cogs-staging-drafter.publishmydata.com/
"""

# %%
from dataclasses import dataclass, field
from uuid import UUID
import requests
from requests.exceptions import RequestException

AUTH0_URL = "https://swirrl-staging.eu.auth0.com/oauth/token"
DEFAULT_BASE_URL = "https://cogs-staging-drafter.publishmydata.com/v1/"


@dataclass
class PublishMyData():
    client_id: str
    client_secret: str
    base_url: str = DEFAULT_BASE_URL

    def __post_init__(self):
        self.get_token(self.client_id, self.client_secret)

    def get_token(
        self,
        client_id,
        client_secret,
        audience="https://pmd",
        grant_type="client_credentials"
    ):
        """Authenticate using client id and secret."""
        response = requests.post(
            AUTH0_URL,
            headers={
                "content-type": "application/json"
            },
            json={
                "client_id": client_id,
                "client_secret": client_secret,
                "audience": audience,
                "grant_type": grant_type
            }
        )

        if response.status_code == 200:
            self.access_token = response.json()["access_token"]
        else:
            raise RequestException(str(response.content, "utf-8"))

    def get_draftsets(self, include="all", union_with_live=False):
        """
        Lists draftsets visible to the user. The include parameter can be used
        to filter the result list to just those owned by the current user, or
        those not owned which can be claimed by the current user. By default all
        owned and claimable draftsets are returned.
        """
        assert include in ("owned", "claimable", "all")
        assert isinstance(union_with_live, bool)

        response = requests.get(
            "https://cogs-staging-drafter.publishmydata.com/v1/draftsets",
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {self.access_token}"
            },
            data={
                "incude": include,
                "union-with-live": union_with_live
            }
        )

        if response.status_code == 200:
            data = response.json()
            draftsets = [
                Draftset(
                    _requester=self,
                    id=d["id"],
                    type=d["type"],
                    created_at=d["created-at"],
                    updated_at=d["updated-at"],
                    changes=d["changes"],
                    display_name=d.get("display-name", None),
                    current_owner=d.get("current-owner", None),
                    submitted_by=d.get("submitted-by", None),
                    claim_role=d.get("claim-role", None),
                    claim_user=d.get("claim-user", None),
                    description=d.get("description", None)
                ) for d in data
            ]
            return draftsets
        else:
            raise RequestException(str(response.content, "utf-8"))

    def get_draftset(self, id: UUID, union_with_live=False):
        """Returns metadata about the draftset."""

        assert isinstance(union_with_live, bool)

        response = requests.get(
            f"https://cogs-staging-drafter.publishmydata.com/v1/draftset/{id}",
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {self.access_token}"
            },
            data={
                "union-with-live": union_with_live
            }
        )

        if response.status_code == 200:
            data = response.json()
            draftset = Draftset(
                _requester=self,
                id=data["id"],
                type=data["type"],
                created_at=data["created-at"],
                updated_at=data["updated-at"],
                changes=data["changes"],
                display_name=data.get("display-name", None),
                current_owner=data.get("current-owner", None),
                submitted_by=data.get("submitted-by", None),
                claim_role=data.get("claim-role", None),
                claim_user=data.get("claim-user", None),
                description=data.get("description", None)
            )
            return draftset
        else:
            raise RequestException(str(response.content, "utf-8"))

    def create_draftset(
        self,
        display_name=None,
        description=None,
        union_with_live=False
    ):
        """
        Creates a new draftset in the database. Optionally accepts query string
        parameters for a name and a description.
        """
        response = requests.post(
            "https://cogs-staging-drafter.publishmydata.com/v1/draftsets",
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {self.access_token}"
            },
            data={
                "display-name": display_name,
                "description": description,
                "union-with-live": union_with_live
            },
            # Create draftset returns a HTTP 303 which we do not want to
            # redirect to. Redirecting produces a HTTP 401 response.
            allow_redirects=False
        )

        if response.status_code == 303:
            draftset_id = response.headers["location"].rsplit("/")[-1]
            return self.get_draftset(id=draftset_id)
        else:
            raise RequestException(str(response.content, "utf-8"))

@dataclass
class Draftset():
    _requester: PublishMyData = field(repr=False)
    id: UUID
    type: str
    created_at: str
    updated_at: str
    changes: dict
    display_name: str = None
    current_owner: str = None
    submitted_by: str = None
    claim_role: str = None
    claim_user: str = None
    description: str = None

    def __post_init__(self):
        assert self.type in ("Endpoint", "Draftset")

    def delete(self, metadata=None):
        """Deletes the draftset and its contents."""
        response = requests.delete(
            f"https://cogs-staging-drafter.publishmydata.com/v1/draftset/{self.id}",
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {self._requester.access_token}"
            },
            data={
                "metadata": metadata
            }
        )
        if response.status_code == 202:
            return True
        else:
            raise RequestException(str(response.content, "utf-8"))

    def claim(self):
        """
        Sets the Draftset’s current-owner to be the same as the user performing
        this operation. This is necessary to prevent other’s from making changes
        to the data contained within the Draftset.

        Each role in the system has a pool of 0 or more claimable draftsets
        associated with it. Claimable draftsets are draftsets in a pool where
        the rank of the pools role is less than or equal to the user’s role’s
        rank.
        """
        response = requests.post(
            f"https://cogs-staging-drafter.publishmydata.com/v1/draftset/{self.id}/claim",
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {self._requester.access_token}"
            }
        )
        if response.status_code == 200:
            return self._requester.get_draftset(id=self.id)
        else:
            raise RequestException(str(response.content, "utf-8"))

    def submit_to(self, role=None, user=None):
        """
        Submits this draftset for review and potential publication.
        Draftsets are submitted directly to a user, or into a pool for users of
        a given role.

        Users with a role greater than or equal to the role the draftset was
        submitted to can then lay claim to it.
        """

        assert role in ("editor", "publisher", "manager", None)
        assert isinstance(user, str) or (user is None)
        assert bool(role) ^ bool(user) # Exclusive OR - only specify one.

        response = requests.post(
            f"https://cogs-staging-drafter.publishmydata.com/v1/draftset/{self.id}/submit-to",
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {self._requester.access_token}"
            },
            json={
                "role": role,
                "user": user
            }
        )

        if response.status_code == 200:
            return self._requester.get_draftset(id=self.id)
        else:
            raise RequestException(str(response.content, "utf-8"))


    def publish(self, metadata=None):
        """
        Requests that this Draftset is published asynchronously to the live
        site. If a job is successfully scheduled then an AsyncJob object will be
        returned.
        """
        requests.post(
            f"https://cogs-staging-drafter.publishmydata.com/v1/draftset/{self.id}/publish",
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {self._requester.access_token}"
            },
            data={
                "metadata": metadata
            }
        )

    def append_data(
        self,
        filepath,
        extension=None,
        content_type=None,
        graph=None,
        metadata=None,
        content_encoding=None
    ):
        """
        Appends the supplied data to the Draftset identified by this resource.

        If the RDF data is supplied in a quad serialisation then the graph query
        parameter can be ommited. If quads are uploaded and a graph parameter is
        specified the graph parameter will take precedence, causing all quads to
        be loaded into the same graph.

        If a graph parameter is supplied then the RDF data can be supplied in a
        triple serialisation.

        The RDF data to add can be gzip-compressed - in this case the
        Content-Encoding header should be set to gzip on the request.
        """

        extension_map = {
            ".trig": "application/trig",
            ".ttl": "text/turtle",
            ".nq": "application/n-quads",
            ".trix": "application/trix",
            ".nt": "application/n-triples",
            ".rdf": "application/rdf+xml"
        }

        assert(extension or content_type)

        assert extension in (
            ".trig", ".ttl", ".nq", ".trix", ".nt", ".rdf", None
        )

        assert content_type in (
            "application/trig",
            "text/turtle",
            "application/n-quads",
            "application/trix",
            "application/n-triples",
            "application/rdf+xml",
            None
        )

        if extension:
            content_type = extension_map[extension]

        if content_type in (
            "text/turtle", 
            "application/trix", 
            "application/n-triples",
            "application/rdf+xml"
        ):
            assert graph

        assert content_encoding in ("gzip", "x-gzip", None)

        rdf = open(filepath).read()

        requests.put(
            f"https://cogs-staging-drafter.publishmydata.com/v1/draftset/{self.id}/data",
            headers={
                "Content-Type": content_type,
                "Content-Encoding": content_encoding,
                "Authorization": f"Bearer {self._requester.access_token}"
            },
            params={
                "graph": graph,
                "metadata": metadata
            },
            data=rdf
        )
