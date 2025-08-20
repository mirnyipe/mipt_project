from etl_load_transactions import load_transactions
from etl_load_terminals import load_terminals
from etl_load_blacklist import load_passport_blacklist

def main():
    load_transactions("data/transactions_03032021.txt")
    load_terminals("data/terminals_03032021.xlsx")
    load_passport_blacklist("data/passport_blacklist_03032021.xlsx")
    
if __name__ == "__main__":
    main()
