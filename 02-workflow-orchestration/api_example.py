import requests

r = requests.get('http://api.github.com/repos/kestra-io/kestra')
gh_starts = r.json()['stargazers_count']
print(f"Kestra has {gh_starts} stars on GitHub")

pass