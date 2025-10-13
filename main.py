import requests
import json

# Replace 'YOUR_API_KEY' with your actual CoinMarketCap API key
api_key = '789baac407ae42629c5bf43a1d4fc7bf'

# The API endpoint URL you want to request data from
url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'

# The headers must include your API key
headers = {
  'Accepts': 'application/json',
  'X-CMC_PRO_API_KEY': api_key,
}

aptos_tokens = []
start = 1
limit = 5000

while True:
  # Parameters to send with the request
  parameters = {
    'start': str(start),
    'limit': str(limit),
    'convert': 'USD'
  }

  try:
    # Make the GET request to the API
    response = requests.get(url, params=parameters, headers=headers)
    data = response.json()

    if response.status_code == 200:
      cryptocurrencies = data['data']
      if not cryptocurrencies:
        break  # No more data to fetch

      for crypto in cryptocurrencies:
        if crypto.get('platform') and crypto['platform'].get('name') == 'Aptos':
          aptos_tokens.append(crypto)

      start += limit
    else:
      print(f"Error: {response.status_code}")
      print(data.get('status', {}).get('error_message', 'No error message provided.'))
      break

  except requests.exceptions.RequestException as e:
    print(f"An error occurred: {e}")
    break

# Write the data to a JSON file
with open('aptos_tokens.json', 'w') as f:
  json.dump(aptos_tokens, f, indent=4)

print(f"Found {len(aptos_tokens)} Aptos tokens and saved them to aptos_tokens.json")
