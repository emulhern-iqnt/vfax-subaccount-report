import requests
import json

def send_with_attach(api_url, sender, recipient, subject, body, attachment):
	url = api_url

	payload = {'from': sender,
			   'to': recipient,
			   'subject': subject,
			   'body': body,
			   'plaintext': 'true',
			   'identifier': 'Min commit Report'
			   }

	headers = {}

	files = [('file', (attachment, open(attachment, 'rb'), 'application/octet-stream'))]
	response = requests.request("POST", url, headers=headers, data=payload, files=files)

	print(response.text)
	return response.text


def send_no_attach(api_url, sender, recipient, subject, body):
	#import requests

	url = api_url

	payload = {'from': sender,
			   'to': recipient,
			   'subject': subject,
			   'body': body,
			   'plaintext': 'true',
			   'identifier': 'Min Commit Report'}

	#headers = {'Content-type': 'application/json'}

	response = requests.request("POST", url,  data=payload, files={'value_1': (None, '12345')})

	print(response.text)
	return response.text

