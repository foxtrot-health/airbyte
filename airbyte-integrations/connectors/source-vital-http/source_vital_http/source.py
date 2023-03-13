#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from airbyte_cdk.sources.streams.http.auth import NoAuth

"""
TODO: Most comments in this class are instructive and should be deleted after the source is implemented.

This file provides a stubbed example of how to use the Airbyte CDK to develop both a source connector which supports full refresh or and an
incremental syncs from an HTTP API.

The various TODOs are both implementation hints and steps - fulfilling all the TODOs should be sufficient to implement one basic and one incremental
stream from a source. This pattern is the same one used by Airbyte internally to implement connectors.

The approach here is not authoritative, and devs are free to use their own judgement.

There are additional required TODOs in the files within the integration_tests folder and the spec.yaml file.
"""


class VitalHttpStream(HttpStream, ABC):
    url_base = "https://api.sandbox.tryvital.io"

    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__()
        self.api_key = config['api_key']

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None
    
    def request_headers(
        self, stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> Mapping[str, Any]:
        # The api requires that we include apikey as a header so we do that in this method
        return {
            'x-vital-api-key': self.api_key
        }

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        # Override this method to define any query parameters to be set. Remove this method if you don't need to define request params.
        # Usually contains common params e.g. pagination size etc.
        return {}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        TODO: Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        yield {}

class IncrementalVitalHttpStream(VitalHttpStream, ABC):
    """
    TODO fill in details of this class to implement functionality related to incremental syncs for your connector.
         if you do not need to implement incremental sync for any streams, remove this class.
    """

    # TODO: Fill in to checkpoint stream reads after N records. This prevents re-reading of data if the stream fails for any reason.
    state_checkpoint_interval = None

    @property
    def cursor_field(self) -> str:
        """
        TODO
        Override to return the cursor field used by this stream e.g: an API entity might always use created_at as the cursor field. This is
        usually id or date based. This field's presence tells the framework this in an incremental stream. Required for incremental.

        :return str: The name of the cursor field.
        """
        return []

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Override to determine the latest state after reading the latest record. This typically compared the cursor_field from the latest record and
        the current state and picks the 'most' recent cursor. This is how a stream's state is determined. Required for incremental.
        """
        return {}

class Users(VitalHttpStream):
    primary_key = 'user_id'

    def path(
        self,
        stream_state: Mapping[str, Any] = None, 
        stream_slice: Mapping[str, Any] = None, 
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "/v2/user" 

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        json = response.json()
        new_offset = json['offset'] + len(json['users'])

        if json['total'] > new_offset:
            return {
                'offset': new_offset,
                'limit': 5,
            }
        else:
            return None
    
    def request_params(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        if next_page_token:
            return {
                'offset': next_page_token['offset'],
                'limit': next_page_token['limit']
            }
        else:
            return {
                'offset': 0,
                'limit': 5
            }
    
    def parse_response(
        self,
        response: requests.Response,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Iterable[Mapping]:
        # The response is a simple JSON whose schema matches our stream's schema exactly, 
        # so we just return a list containing the response
        return response.json()['users']

class Sleep(HttpSubStream, VitalHttpStream):
    primary_key = "id"

    def __init__(self, parent: HttpStream, config: Mapping[str, Any], **kwargs):
        super().__init__(Users(config=config, **kwargs), config=config, **kwargs)

        self.config = config
        self.parent = parent

    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        users_stream = Users(config=self.config, **kwargs)

        for user in users_stream.read_records(sync_mode = SyncMode.full_refresh):
            yield {"user_id": user['user_id']}

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        user_id = stream_slice["user_id"]
        return f"/v2/summary/sleep/{user_id}"
    
    def request_params(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        return {
            'start_date': '2023-02-23'
        }

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        return response.json()['sleep']

class SourceVitalHttp(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        if config['api_key']:
            return True, None
        else:
            return False, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        users_stream = Users(config=config)

        return [
            users_stream,
            Sleep(parent=users_stream, config=config)
        ]
