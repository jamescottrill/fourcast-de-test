import google.auth
import google.auth.transport.requests
import requests
import six.moves.urllib.parse


def main():
    """
    Make an unauthenticated API call to Composer to get the Client Id
    `Code from Google <https://github.com/GoogleCloudPlatform/python-docs-samples/blob/master/composer/rest/get_client_id.py>`_
    :rtype: str
    :return: Client Id
    """
    # Authenticate with Google Cloud.
    # See: https://cloud.google.com/docs/authentication/getting-started
    credentials, _ = google.auth.default(
        scopes=['https://www.googleapis.com/auth/cloud-platform'])
    authed_session = google.auth.transport.requests.AuthorizedSession(
        credentials)

    project_id = '%PROJECT_ID%'
    location = '%LOCATION%'
    composer_environment = '%ENVIRONMENT_NAME%'

    environment_url = (
        'https://composer.googleapis.com/v1beta1/projects/{}/locations/{}'
        '/environments/{}').format(project_id, location, composer_environment)
    composer_response = authed_session.request('GET', environment_url)
    environment_data = composer_response.json()
    airflow_uri = environment_data['config']['airflowUri']

    # The Composer environment response does not include the IAP client ID.
    # Make a second, unauthenticated HTTP request to the web server to get the
    # redirect URI.
    redirect_response = requests.get(airflow_uri, allow_redirects=False)
    redirect_location = redirect_response.headers['location']

    # Extract the client_id query parameter from the redirect.
    parsed = six.moves.urllib.parse.urlparse(redirect_location)
    query_string = six.moves.urllib.parse.parse_qs(parsed.query)
    print(query_string['client_id'][0])
    return query_string['client_id'][0]


if __name__ == '__main__':
    main()
