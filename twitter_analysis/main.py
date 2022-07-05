import argparse


def parse_args():
    parser = argparse.ArgumentParser(
        description="Analyse Posts from a twitter search",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--search",
        dest="search",
        default="",
        help="twitter search criteria",
        required=True,
    )
    parser.add_argument(
        "--config",
        dest="config",
        default="config.yaml",
        help="Path to config file (Default: config.yaml)",
    )

    args = parser.parse_args()

    return args.search, args.config


def main():
    search, config_file = parse_args()

    print(search, config_file)


if __name__ == "__main__":
    main()
