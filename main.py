from etl_clients import load_clients
from etl_accounts import load_accounts
from etl_cards import load_cards

def main():
    load_clients()
    load_accounts()
    load_cards()

if __name__ == "__main__":
    main()
