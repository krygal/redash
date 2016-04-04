import datetime
import json
import logging
import sys

from redash.query_runner import *
from redash import models

import importlib

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

import httplib2
from oauth2client import client
from oauth2client import file
from oauth2client import tools

logger = logging.getLogger(__name__)

"""Example 'query'
see https://developers.google.com/analytics/devguides/reporting/core/v3/reference#introduction for details
see http://docs.redash.io/en/latest/dev/results_format.html on how to structure columns data
{
  "start_date": "2012-04-01",
  "end_date": "today",
  "metrics": "ga:sessions",
  "dimensions": "ga:year,ga:month",
  "segment": "sessions::condition::ga:customVarValue2!@student",
  "columns": [
    {
      "name" : "year",
      "type" : "string"
    },
    {
      "name" : "month",
      "type" : "string"
    },
    {
      "name" : "new visitors",
      "type" : "integer"
    }
  ]
}
"""
class GoogleAnalytics(BaseQueryRunner):
    @classmethod
    def configuration_schema(cls):
        return {
            'type': 'object',
            'properties': {
                'profile': {
                    'type': 'string',
                    'title': 'GA view profile id'
                },
                'credentialsPath': {
                    'type': 'string',
                    'title': 'Path to the json file with credentials'
                }
            },
        }

    @classmethod
    def enabled(cls):
        return True

    @classmethod
    def annotate_query(cls):
        return False

    def __init__(self, configuration):
        super(GoogleAnalytics, self).__init__(configuration)

        self.syntax = "sql"

        self._enable_print_log = True

        if self.configuration.get("credentialsPath", None):
            self._credentials_path = self.configuration["credentialsPath"]
        else:
            raise Exception("credentialsPath must be set")

        if self.configuration.get("profile", None):
            self._profile = self.configuration["profile"]
        else:
            raise Exception("credentialsPath must be set")

    def run_query(self, query):
        try:
            error = None

            scope = ['https://www.googleapis.com/auth/analytics.readonly']
            json_file_location = self._credentials_path
            profile = self._profile

            service = self.get_service('analytics', 'v3', scope, json_file_location)

            result = {}

            params = json.loads(query)
            params['ids'] = 'ga:' + profile

            result['columns'] = params['columns']
            result['rows'] = self.get_results(service, params)
            json_data = json.dumps(result)
        except KeyboardInterrupt:
            error = "Query cancelled by user."
            json_data = None
        except Exception as e:
            error = str(e)
            json_data = None

        return json_data, error

    def get_service(self, api_name, api_version, scope, json_file_location):

        credentials = ServiceAccountCredentials.from_json_keyfile_name(json_file_location, scopes=scope)
        http = credentials.authorize(httplib2.Http())

        service = build(api_name, api_version, http=http)

        return service

    def get_results(self, service, params):
        results = service.data().ga().get(
          ids=params['ids'],
          start_date=params['start_date'],
          end_date=params['end_date'],
          metrics=params['metrics'],
          dimensions=params['dimensions'],
          segment=params['segment']).execute()

        return_data = []
        for row in results.get('rows'):
            append_row = {}
            index = 0
            for column in params['columns']:
                append_row[column['name']] = row[index]
                index = index + 1
            return_data.append(append_row)

        return return_data


register(GoogleAnalytics)
