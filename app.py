import time
import os
from prometheus_client import start_http_server
from prometheus_client.core import GaugeMetricFamily, REGISTRY, CounterMetricFamily, StateSetMetricFamily, InfoMetricFamily
from requests.exceptions import RequestException
import logging
import requests
import dateutil.parser
import datetime



HEADERS = {"accept": "application/json"}

class DkronMetricsController(object):
    def __init__(self, host):
        self.host = host

    def collect(self):
        try:
            result = requests.get(
                    url=self.host+"/v1/jobs",
                    headers=HEADERS                
                    )
            logging.debug(f"{result.url} - {result.status_code}")
        except RequestException as e:
            logging.error(e)
            raise os._exit(status=1)
        yield self.get_error_metrics(result.json())
        yield self.get_success_metrics(result.json())
        yield self.get_last_exec_time_metrics(result.json())
        yield self.get_next_exec_time_metrics(result.json())
        yield self.get_status_metrics(result.json())
        yield self.get_info_metrics(result.json())
        yield self.get_schedule_status_metrics(result.json())

    def get_info_metrics(self, result):
        metric = InfoMetricFamily(
            'dkron_info',
            'Dkron existings jobs information',
            labels=["jobname"])

        for job in result:
            name = job['name']
            owner = job.get('owner')
            owner_email = job.get('owner_email')
            metric.add_metric([name], {'owner': owner, 'owner_email': owner_email})
        return metric

    def get_status_metrics(self, result):
        metric = StateSetMetricFamily(
            'dkron_job_status',
            'Dkron job status',
            labels=["jobname"])
        for job in result:
            name = job['name']
            status = job.get('status') or "None"
            metric.add_metric([name], {'success': status})
        return metric

    def get_schedule_status_metrics(self, result):
        metric = StateSetMetricFamily(
            'dkron_job_schedule_status',
            'Dkron job schedule status',
            labels=["jobname"])

        for job in result:
            name = job['name']
            # If there's a null result, we want to export a zero.
            next_date_str = job.get('next') or "None"
            next_date = dateutil.parser.parse(next_date_str)
            diff_date = next_date - datetime.datetime.now(datetime.timezone.utc)
            if diff_date < datetime.timedelta(minutes=-1):
                metric.add_metric([name], {'success': False})
            else:
                metric.add_metric([name], {'success': True})
            # job_status = job['status']
        return metric

    def get_last_exec_time_metrics(self, result):
        metric = GaugeMetricFamily(
            'dkron_job_last_success_ts',
            'Dkron job last successful execution timestamp in unixtime',
            labels=["jobname"])

        for job in result:
            name = job['name']
            # If there's a null result, we want to export a zero.
            status = job.get('last_success') or "None"
            d = dateutil.parser.parse(status)
            metric.add_metric([name], d.timestamp() / 1000.0)
            # job_status = job['status']
        return metric

    def get_next_exec_time_metrics(self, result):
        metric = GaugeMetricFamily(
            'dkron_job_next_exec_ts',
            'Dkron job next execution timestamp in unixtime',
            labels=["jobname"])

        for job in result:
            name = job['name']
            # If there's a null result, we want to export a zero.
            status = job.get('next') or "None"
            d = dateutil.parser.parse(status)
            metric.add_metric([name], d.timestamp() / 1000.0)
            # job_status = job['status']
        return metric

    def get_error_metrics(self, result):
        metric = CounterMetricFamily(
            'dkron_job_error_counts',
            'Dkron job error counts',
            labels=["jobname"])

        for job in result:
            name = job['name']
            status = job.get('error_count') or 0
            metric.add_metric([name], status)
        return metric

    def get_success_metrics(self, result):
        metric = CounterMetricFamily(
            'dkron_job_success_count',
            'Dkron job success count',
            labels=["jobname"])

        for job in result:
            name = job['name']
            status = job.get('success_count') or 0
            metric.add_metric([name], status)
        return metric


if __name__ == "__main__":
    host = os.getenv("DKRON_TARGET_HOSTNAME","http://dkron.svc.cluster.local:8080")
    port = os.getenv("EXPORTER_PORT", "8000")
    loglevel = os.getenv("EXPORTER_LOGLEVEL", "INFO")
    numeric_level = getattr(logging, loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level specified: {loglevel}")
    logging.basicConfig(level=numeric_level)
    REGISTRY.register(DkronMetricsController(host))
    start_http_server(int(port))
    while True: 
        time.sleep(1)
