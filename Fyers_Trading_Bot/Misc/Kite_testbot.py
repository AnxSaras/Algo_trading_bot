from kiteconnect import kiteconnect

Set the access token
kite.set_access_token(data["access_token"])

# Get user profile
user_profile = kite.profile()
print(user_profile)

instruments = ["RELIANCE", "TCS"]
quotes = kite.quote(instruments)
print(quotes)

