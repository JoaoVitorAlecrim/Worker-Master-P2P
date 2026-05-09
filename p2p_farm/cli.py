from __future__ import annotations

import argparse


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="p2p-farm")
    subparsers = parser.add_subparsers(dest="role", required=True)
    subparsers.add_parser("master")
    subparsers.add_parser("worker")
    return parser
