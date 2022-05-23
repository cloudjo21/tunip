import requests


def use_serp_api(word, timeout_time=20):
    # TODO use your config
    url = "http://0.0.0.0:30000/serpapi/search"
    data = {'q': word, 'gl': 'kr',
            'hl': 'ko', 'location': 'South Korea'}

    response = requests.post(url, data=data, timeout=timeout_time)
    serp = response.text.encode('utf-8').decode('unicode_escape')
    return serp
