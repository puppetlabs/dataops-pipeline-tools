import sys
import logging

from requests import Session, Response
from requests.adapters import HTTPAdapter
from requests.exceptions import HTTPError
from requests.packages.urllib3.util.retry import Retry


def perform_get(session: Session, url: str, params: str = None) -> Response:
    """Performs HTTPS GET request using python Requests.

    Args:
        session: python Requests session
        url: the FQDN to perform the GET request against
    Returns:
        The requests Response object
    """
    retries = Retry(total=20, backoff_factor=.1,
                    status_forcelist=[429, 503, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    try:
        if params:
            response = session.get(url, params=params)
        else:
            response = session.get(url)
        response.raise_for_status()
    except HTTPError as err:
        logging.error(err)

    return response

def perform_post(session: Session, url: str, data: dict = None) -> Response:
    """Performs HTTPS POST request using python Requests.

    Args:
        session: python Requests session
        url: the FQDN to perform the POST request against
        data: the data payload to send to the FQDN
    Returns:
        The requests Response object
    """
    retries = Retry(total=20, backoff_factor=.1,
                    status_forcelist=[429, 503, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    try:
        response = session.post(url=url, data=data)
        response.raise_for_status()
    except HTTPError as err:
        logging.error(err)

    return response

def perform_put(session: Session, url: str, data: dict = None) -> Response:
    """Performs HTTPS PUT request using python Requests.

    Args:
        session: python Requests session
        url: the FQDN to perform the POST request against
        data: the data payload to send to the FQDN
    Returns:
        The requests Response object
    """
    retries = Retry(total=20, backoff_factor=.1,
                    status_forcelist=[429, 503, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    try:
        response = session.put(url=url, data=data)
        response.raise_for_status()
    except HTTPError as err:
        logging.error(err)

    return response
