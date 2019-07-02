import os
import json
import asyncio
from pathlib import Path
from itertools import repeat
from urllib.parse import urljoin

import singer
import requests
import pendulum
from singer.bookmarks import write_bookmark, get_bookmark
from pendulum import datetime, period


class PagerdutyAuthentication(requests.auth.AuthBase):
    def __init__(self, api_token: str):
        self.api_token = api_token

    def __call__(self, req):
        req.headers.update({"Authorization": " Token token=" + self.api_token})

        return req


class PagerdutyClient:
    def __init__(self, auth: PagerdutyAuthentication, url="https://api.pagerduty.com/"):
        self._base_url = url
        self._auth = auth
        self._session = None

    @property
    def session(self):
        if not self._session:
            self._session = requests.Session()
            self._session.auth = self._auth
            self._session.headers.update({"Accept": "application/json"})

        return self._session

    def _get(self, path, params=None):
        #url = urljoin(self._base_url, path)
        url = self._base_url + path
        response = self.session.get(url, params=params)
        response.raise_for_status()

        return response


    def incidents(self):
        try:
            incidents = self._get(f"incidents")
            return incidents.json()
        except:
            return None

    def alerts(self, incident_id):
        try:
            alerts = self._get(f"incidents/{incident_id}/alerts")
            return alerts.json()
        except:
            return None

    def services(self):
        try:
            services = self._get(f"services")
            return services.json()
        except:
            return None

    def escalationPolicies(self):
        try:
            policies = self._get(f"escalation_policies")
            return policies.json()
        except:
            return None

    def teams(self):
        try:
            teams = self._get(f"teams")
            return teams.json()
        except:
            return None

    def users(self):
        try:
            users = self._get(f"users")
            return users.json()
        except:
            return None

    def vendors(self):
        try:
            vendors = self._get(f"vendors")
            return vendors.json()
        except:
            return None

class PagerdutySync:
    def __init__(self, client: PagerdutyClient, state={}):
        self._client = client
        self._state = state

    @property
    def client(self):
        return self._client

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, value):
        singer.write_state(value)
        self._state = value

    def sync(self, stream, schema):
        func = getattr(self, f"sync_{stream}")
        return func(schema)

    async def sync_incidents(self, schema):
        """Incidents."""
        stream = "incidents"
        loop = asyncio.get_running_loop()

        singer.write_schema(stream, schema.to_dict(), ["id"])
        incidents = await loop.run_in_executor(None, self.client.incidents)
        if incidents:
            for incident in incidents['incidents']:
                singer.write_record(stream, incident)

    async def sync_alerts(self, schema, period: pendulum.period = None):
        """Alerts per incidents."""
        stream = "alerts"
        loop = asyncio.get_running_loop()

        singer.write_schema(stream, schema.to_dict(), ["id"])
        incidents = await loop.run_in_executor(None, self.client.incidents)
        if incidents:
            for incident in incidents['incidents']:
                alerts = await loop.run_in_executor(None, self.client.alerts, incident['id'])
                if (alerts):
                    for alert in alerts['alerts']:
                        singer.write_record(stream, alert)


    async  def sync_services(self, schema, period: pendulum.period = None):
        """All Services."""
        stream = "services"
        loop = asyncio.get_running_loop()

        singer.write_schema(stream, schema.to_dict(), ["id"])
        services = await loop.run_in_executor(None, self.client.services)
        if services:
            for service in services['services']:
                singer.write_record(stream, service)

    async def sync_escalationPolicies(self, schema):
        """All Escalation Policies."""
        stream = "escalationPolicies"
        loop = asyncio.get_running_loop()

        singer.write_schema(stream, schema.to_dict(), ["id"])
        policies = await loop.run_in_executor(None, self.client.escalationPolicies)
        if policies:
            for policie in policies['escalation_policies']:
                singer.write_record(stream, policie)

    async def sync_teams(self, schema):
        """All Teams."""
        stream = "teams"
        loop = asyncio.get_running_loop()
        singer.write_schema(stream, schema.to_dict(), ["id"])
        teams = await loop.run_in_executor(None, self.client.teams)
        if teams:
            for team in teams['teams']:
                singer.write_record(stream, team)

    async def sync_users(self, schema):
        """All Users."""
        stream = "users"
        loop = asyncio.get_running_loop()
        singer.write_schema(stream, schema.to_dict(), ["id"])
        users = await loop.run_in_executor(None, self.client.users)
        if users:
            for user in users['users']:
                singer.write_record(stream, user)

    async def sync_vendors(self, schema):
        """All Vendors."""
        stream = "vendors"
        loop = asyncio.get_running_loop()
        singer.write_schema(stream, schema.to_dict(), ["id"])
        vendors = await loop.run_in_executor(None, self.client.vendors)
        if vendors:
            for vendor in vendors['vendors']:
                singer.write_record(stream, vendor)