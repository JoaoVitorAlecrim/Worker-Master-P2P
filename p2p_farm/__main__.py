from p2p_farm.cli import build_parser


def main() -> None:
    build_parser().parse_args()


if __name__ == "__main__":
    main()