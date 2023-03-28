from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
import xmltodict
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream

class CnsvsStream(HttpStream, ABC):
    url_base = "https://sync.cnsvs.com/sync.php"

    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__()
        self.config = config

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None
    
    @property
    def http_method(self) -> str:
        return "POST"
    
    @property
    def body_data(self) -> Optional[Mapping]:
        return {
            "signature": "cnsvs-api-testing",
            "username": self.config['username'],
            "password": self.config['password'],
            "account": self.config['account'],
            "request": "list_reports",
            "begin_date": self.config['start_date']
        }

class Reports(CnsvsStream):
    primary_key = "sync_id"

    def path(
        self,
        stream_state: Mapping[str, Any] = None, 
        stream_slice: Mapping[str, Any] = None, 
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return ""
    
    def _lowercase(self, obj):
        if isinstance(obj, dict):
            return {k.lower(): self._lowercase(v) for k, v in obj.items()}
        elif isinstance(obj, (list, set, tuple)):
            t = type(obj)
            return t(self._lowercase(o) for o in obj)
        elif isinstance(obj, str):
            return obj.lower()
        else:
            return obj
    
    def parseXml(self, response: requests.Response):
        parsed = xmltodict.parse(response.content)
        list_reports = parsed.get('LIST_REPORTS')
        return self._lowercase(list_reports)
    
    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        list_reports = self.parseXml(response)
        if int(list_reports.get("pages")) > 0:
            return str(int(list_reports.get("page_no")) + 1)
        else:
            return None


    def request_body_data(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> Optional[Mapping]:
        return {
            **self.body_data,
            "request": "list_reports",
            "begin_date": self.config["start_date"],
            "page_number": next_page_token if next_page_token is not None else "1"
        }
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        list_reports = self.parseXml(response)

        if int(list_reports.get("num_results")) > 0:
            return list_reports.get("list").get("result");
        else:
            return []

class SourceCnsvs(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        return [
            Reports(config=config)
        ]
