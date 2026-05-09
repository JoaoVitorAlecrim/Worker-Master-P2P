from p2p_farm.cli import build_parser


def test_cli_exposes_master_and_worker_subcommands():
    parser = build_parser()

    assert parser.parse_args(["master"]).role == "master"
    assert parser.parse_args(["worker"]).role == "worker"