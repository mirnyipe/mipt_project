# -*- coding: utf-8 -*-
import argparse
from py_scripts.db import init_db, load_dim_scd1_from_public, print_connection_info
from py_scripts.io import process_inbox_and_build_reports, build_missing_reports

def main():
    parser = argparse.ArgumentParser(description="ETL (STG→DWH→REP_FRAUD)")
    parser.add_argument("--init", action="store_true")
    parser.add_argument("--load-public", action="store_true")
    parser.add_argument("--process", action="store_true")
    args = parser.parse_args()

    print_connection_info()

    if not any([args.init, args.load_public, args.process]):
        init_db()
        load_dim_scd1_from_public()
        process_inbox_and_build_reports()
        build_missing_reports()
        print("ETL FINISHED.")
        return

    if args.init:
        init_db()
    if args.load_public:
        load_dim_scd1_from_public()
    if args.process:
        process_inbox_and_build_reports()
        build_missing_reports()

if __name__ == "__main__":
    main()
