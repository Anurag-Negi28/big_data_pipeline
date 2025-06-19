import requests
from requests.auth import HTTPBasicAuth
import json
import xml.etree.ElementTree as ET
from pyspark.sql import SparkSession
from typing import Dict, Optional

class APIConnector:
    """
    Handles API connections with authentication and streaming support
    """
    
    def __init__(self, spark_session: SparkSession, logger, config: Dict):
        self.spark = spark_session
        self.logger = logger
        self.config = config
        self.api_config = config.get('ingestion', {}).get('api', {})
    
    def fetch_data(self, url: str, method: str = 'GET', headers: Optional[Dict] = None, 
                   params: Optional[Dict] = None, auth: Optional[Dict] = None, 
                   format: str = 'json') -> SparkSession.DataFrame:
        """
        Fetch data from an API endpoint
        """
        try:
            self.logger.info(f"Fetching data from API: {url}")
            
            # Set up authentication
            if auth and auth.get('type') == 'basic':
                auth = HTTPBasicAuth(auth['username'], auth['password'])
            
            # Make request
            response = requests.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                auth=auth,
                stream=True if format == 'stream' else False
            )
            response.raise_for_status()
            
            # Handle different response formats
            if format == 'json':
                data = response.json()
                if isinstance(data, list):
                    df = self.spark.createDataFrame(data)
                else:
                    df = self.spark.createDataFrame([data])
            elif format == 'xml':
                root = ET.fromstring(response.text)
                data = self._xml_to_dict(root)
                df = self.spark.createDataFrame(data)
            elif format == 'stream':
                # Handle streaming data (line-delimited JSON)
                data = []
                for line in response.iter_lines():
                    if line:
                        data.append(json.loads(line.decode('utf-8')))
                df = self.spark.createDataFrame(data)
            else:
                raise ValueError(f"Unsupported format: {format}")
            
            self.logger.info(f"Successfully fetched {df.count()} records from API")
            return df
            
        except Exception as e:
            self.logger.error(f"Error fetching API data: {str(e)}")
            raise
    
    def _xml_to_dict(self, element: ET.Element) -> list:
        """
        Convert XML element to list of dictionaries
        """
        result = []
        for child in element:
            item = {}
            for subchild in child:
                item[subchild.tag] = subchild.text
            result.append(item)
        return result