from tunip.nugget_api import Nugget


ap = Nugget(tagger_type="seunjeon")

text = "오늘 아침 학교 가는 버스안에서\n학교 가자\n지미 카터"
# response = list(ap.record([text]))
# response = list(ap(text))
response = list(ap(text.splitlines()))
print(response)

text = "학교 가자"
response = list(ap.record([text]))
print(response)
text = "지미 카터"
response = list(ap.record([text]))
print(response)