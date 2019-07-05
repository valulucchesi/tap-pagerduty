import os
import json
import asyncio
import urllib
from pathlib import Path
from itertools import repeat
from urllib.parse import urljoin

import singer
import requests
import pendulum
from singer.bookmarks import write_bookmark, get_bookmark
from pendulum import datetime, period
import datetime
from dateutil import relativedelta


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


    def incidents(self, state, config):
        try:
            bookmark = get_bookmark(state, "incidents", "since")
            query_base = f"incidents?limit=100&total=true&utc=true"
            if bookmark:
                start_date = "&since=" + urllib.parse.quote(bookmark) + "&utc=true"
            else:
                start_date = datetime.datetime.strptime(config['start_date'], '%Y-%m-%d')
                #query += "&since=" + urllib.parse.quote(start_date) + '&until='+ datetime.datetime.utcnow().isoformat()
            r = relativedelta.relativedelta(datetime.datetime.utcnow(),start_date)
            result = {}
            if r.years > 0 or r.months >= 5:
                while r.years > 0 or r.months >= 5:
                    until = (start_date + datetime.timedelta(5*365/12))
                    query = query_base +  "&since=" + urllib.parse.quote(start_date.isoformat()) + "&until="+ urllib.parse.quote(until.isoformat())
                    incidents = self._get(query)
                    iterable = incidents.json()
                    if 'incidents' in result:
                        result['incidents'].extend(iterable['incidents'])
                    else:
                        result = iterable
                    offset = 0
                    while iterable['more']:
                        offset = offset + result['limit']
                        query += "&offset=" + str(offset)
                        incidents = self._get(query)
                        iterable = incidents.json()
                        result['incidents'].extend(iterable['incidents'])
                    start_date = until
                    r = relativedelta.relativedelta(datetime.datetime.utcnow(), start_date)
            query = query_base +  "&since=" + urllib.parse.quote(start_date.isoformat()) + '&until='+ urllib.parse.quote(datetime.datetime.utcnow().isoformat())
            incidents = self._get(query)
            iterable = incidents.json()
            if 'incidents' in result:
                result['incidents'].extend(iterable['incidents'])
            else:
                result = iterable
            offset = 0
            while iterable['more']:
                offset = offset + result['limit']
                query += "&offset=" + str(offset)
                incidents = self._get(query)
                iterable = incidents.json()
                result['incidents'].extend(iterable['incidents'])
            return result
        except:
            return None

    def alerts(self, incident_id):
        try:
            query = f"incidents/{incident_id}/alerts?limit=100&total=true"
            alerts = self._get(query)
            iterable = alerts.json()
            result = iterable
            offset = 0
            while iterable['more']:
                offset = offset + result['limit']
                query += "&offset=" + str(offset)
                alerts = self._get(query)
                iterable = alerts.json()
                result += iterable
            return result
        except:
            return None

    def services(self):
        try:
            query = f"services?limit=100&total=true"
            services = self._get(query)
            iterable = services.json()
            result = iterable
            offset = 0
            while iterable['more']:
                offset = offset + result['limit']
                query += "&offset=" + str(offset)
                services = self._get(query)
                iterable = services.json()
                result += iterable
            return result
        except:
            return None

    def escalationPolicies(self):
        try:
            query = f"escalation_policies?limit=100&total=true"
            policies = self._get(query)
            iterable = policies.json()
            result = iterable
            offset = 0
            while iterable['more']:
                offset = offset + result['limit']
                query += "&offset=" + str(offset)
                policies = self._get(query)
                iterable = policies.json()
                result += iterable
            return result
        except:
            return None

    def teams(self):
        try:
            query = f"teams?limit=100&total=true"
            teams = self._get(query)
            iterable = teams.json()
            result = iterable
            offset = 0
            while iterable['more']:
                offset = offset + result['limit']
                query += "&offset=" + str(offset)
                teams = self._get(query)
                iterable = teams.json()
                result += iterable
            return result
        except:
            return None

    def users(self):
        try:
            query = f"users?limit=100&total=true"
            users = self._get(query)
            iterable = users.json()
            result = iterable
            offset = 0
            while iterable['more']:
                offset = offset + result['limit']
                query += "&offset=" + str(offset)
                users = self._get(query)
                iterable = users.json()
                result += iterable
            return result
        except:
            return None

    def vendors(self):
        try:
            query = f"vendors?limit=100&total=true"
            vendors = self._get(query)
            iterable = vendors.json()
            result = iterable
            offset = 0
            while iterable['more']:
                offset = offset + result['limit']
                query += "&offset=" + str(offset)
                vendors = self._get(query)
                iterable = vendors.json()
                result += iterable
            return result
        except:
            return None

class PagerdutySync:
    def __init__(self, client: PagerdutyClient, state={}, config={}):
        self._client = client
        self._state = state
        self._config = config

    @property
    def client(self):
        return self._client

    @property
    def state(self):
        return self._state

    @property
    def config(self):
        return self._config

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
        incidents = await loop.run_in_executor(None, self.client.incidents, self.state, self.config)
        if incidents:
            for incident in incidents['incidents']:
                singer.write_record(stream, incident)
            self.state = write_bookmark(self.state, stream, "since", datetime.datetime.utcnow().isoformat())

    async def sync_alerts(self, schema, period: pendulum.period = None):
        """Alerts per incidents."""
        stream = "alerts"
        loop = asyncio.get_running_loop()

        singer.write_schema(stream, schema.to_dict(), ["id"])
        incidents = await loop.run_in_executor(None, self.client.incidents, self.state, self.config)
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