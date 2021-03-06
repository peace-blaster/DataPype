# sample bot post

# imports:
import json
import requests

#post function
def slack_post(message='', channel='bot-test', user='bottst', webhook=''):
    headers = {'Content-Type': 'application/json'}
    data = json.dumps({'text': message, 'channel': channel, 'username': user})
    requests.post(webhook, headers=headers, data=data)

# run it
slack_post(message='tst', channel='bot-test', user='bottst', webhook='https://hooks.slack.com/services/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')
