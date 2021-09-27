import requests


def use_serp_api(word, timeout_time=20):
    url = "http://ascentkorea.iptime.org:30080/serpapi/search"
    data = {'q': word, 'gl': 'kr',
            'hl': 'ko', 'location': 'South Korea'}

    response = requests.post(url, data=data, timeout=timeout_time)
    serp = response.text.encode('utf-8').decode('unicode_escape')
    return serp
