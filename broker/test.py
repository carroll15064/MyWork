from broker.ib_connector import IBConnector


ib = IBConnector()
if ib.connect():
    print(f"Account Value: ${ib.get_account_value():,.2f}")
    positions = ib.get_positions()
    print("Current Positions:", positions)
else:
    print("Failed to connect to IB")