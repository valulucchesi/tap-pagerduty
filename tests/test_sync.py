import asyncio
import os
import unittest

# Our test case class
import requests_mock
import simplejson
from singer import Schema
import mock

from tap_pagerduty import PagerdutyAuthentication, PagerdutyClient, PagerdutySync


def load_file(filename):
    myDir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(myDir, "data_test", filename)
    with open(path) as file:
        return simplejson.load(file)

def load_schema(filename):
    myDir = os.path.dirname(os.path.abspath(''))
    path = os.path.join(myDir, "tap_pagerduty/schemas", filename)
    with open(path) as file:
        return simplejson.load(file)

class MyGreatClassTestCase(unittest.TestCase):

    @requests_mock.mock()
    def test_getAll(self, m):
        auth = PagerdutyAuthentication('111')
        test_client = PagerdutyClient(auth)
        record = load_file("service_record.json")
        m.get('https://api.pagerduty.com/services?limit=100&total=true', json={'services':[record],'limit':100, 'offset':0, 'total':28, 'more': False})
        config = {"start_hour":"2019-08-07T12"}
        self.assertEqual(test_client.getAll('services'), {'services':[record],'limit':100, 'offset':0, 'total':28, 'more': False})
        m.get('https://api.pagerduty.com/services?limit=100&total=true', json={'services':[record],'limit':100, 'offset':0, 'total':28, 'more': True})
        test_client.getAll('services')
        count = m.call_count
        self.assertEqual(count, 3)

    @requests_mock.mock()
    def test_sync_services(self, m):
        loop = asyncio.get_event_loop()
        auth = PagerdutyAuthentication('111')
        test_client = PagerdutyClient(auth)
        record = load_file("service_record.json")
        m.get('https://api.pagerduty.com/services?limit=100&total=true', json={'services':[record],'limit':100, 'offset':0, 'total':28, 'more': False})
        config = {"start_hour":"2019-08-07T12"}
        dataSync = PagerdutySync(test_client, config=config)
        schema = load_schema("services.json")
        resp = dataSync.sync_services(Schema(schema))
        with mock.patch('singer.write_record') as patching:
            task = asyncio.gather(resp)
            loop.run_until_complete(task)
            patching.assert_called_once_with('services', record)


    @requests_mock.mock()
    def test_sync_alerts(self, m):
        loop = asyncio.get_event_loop()
        auth = PagerdutyAuthentication('111')
        test_client = PagerdutyClient(auth)
        with mock.patch.object(PagerdutyClient, 'incidents', return_value={"incidents":[{"id":"1"}]}):
            record = load_file("alert_record.json")
            m.get('https://api.pagerduty.com/incidents/1/alerts?limit=100&total=true', json={'alerts':[record],'limit':100, 'offset':0, 'total':28, 'more': False})
            config = {"start_hour":"2019-08-07T12"}
            dataSync = PagerdutySync(test_client, config=config)
            schema = load_schema("alerts.json")
            resp = dataSync.sync_alerts(Schema(schema))
            with mock.patch('singer.write_record') as patching:
                task = asyncio.gather(resp)
                loop.run_until_complete(task)
                patching.assert_called_once_with('alerts', record)


    @requests_mock.mock()
    def test_sync_escalationPolicies(self, m):
        loop = asyncio.get_event_loop()
        auth = PagerdutyAuthentication('111')
        test_client = PagerdutyClient(auth)
        record = load_file("escalationPolicy_record.json")
        m.get('https://api.pagerduty.com/escalation_policies?limit=100&total=true', json={'escalation_policies':[record],'limit':100, 'offset':0, 'total':28, 'more': False})
        config = {"start_hour":"2019-08-07T12"}
        dataSync = PagerdutySync(test_client, config=config)
        schema = load_schema("escalationPolicies.json")
        resp = dataSync.sync_escalationPolicies(Schema(schema))
        with mock.patch('singer.write_record') as patching:
            task = asyncio.gather(resp)
            loop.run_until_complete(task)
            patching.assert_called_once_with('escalationPolicies', record)


    @requests_mock.mock()
    def test_sync_teams(self, m):
        loop = asyncio.get_event_loop()
        auth = PagerdutyAuthentication('111')
        test_client = PagerdutyClient(auth)
        record = load_file("team_record.json")
        m.get('https://api.pagerduty.com/teams?limit=100&total=true', json={'teams':[record],'limit':100, 'offset':0, 'total':28, 'more': False})
        config = {"start_hour":"2019-08-07T12"}
        dataSync = PagerdutySync(test_client, config=config)
        schema = load_schema("teams.json")
        resp = dataSync.sync_teams(Schema(schema))
        with mock.patch('singer.write_record') as patching:
            task = asyncio.gather(resp)
            loop.run_until_complete(task)
            patching.assert_called_once_with('teams', record)

    @requests_mock.mock()
    def test_sync_users(self, m):
        loop = asyncio.get_event_loop()
        auth = PagerdutyAuthentication('111')
        test_client = PagerdutyClient(auth)
        record = load_file("user_record.json")
        m.get('https://api.pagerduty.com/users?limit=100&total=true', json={'users':[record],'limit':100, 'offset':0, 'total':28, 'more': False})
        config = {"start_hour":"2019-08-07T12"}
        dataSync = PagerdutySync(test_client, config=config)
        schema = load_schema("users.json")
        resp = dataSync.sync_users(Schema(schema))
        with mock.patch('singer.write_record') as patching:
            task = asyncio.gather(resp)
            loop.run_until_complete(task)
            patching.assert_called_once_with('users', record)

    @requests_mock.mock()
    def test_sync_vendor(self, m):
        loop = asyncio.get_event_loop()
        auth = PagerdutyAuthentication('111')
        test_client = PagerdutyClient(auth)
        record = load_file("vendor_record.json")
        m.get('https://api.pagerduty.com/vendors?limit=100&total=true', json={'vendors':[record],'limit':100, 'offset':0, 'total':28, 'more': False})
        config = {"start_hour":"2019-08-07T12"}
        dataSync = PagerdutySync(test_client, config=config)
        schema = load_schema("vendors.json")
        resp = dataSync.sync_vendors(Schema(schema))
        with mock.patch('singer.write_record') as patching:
            task = asyncio.gather(resp)
            loop.run_until_complete(task)
            patching.assert_called_once_with('vendors', record)

if __name__ == '__main__':
    unittest.main()