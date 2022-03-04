import sys
import argparse

from . import gcp as gcp


def init_parser():
    main_parser = argparse.ArgumentParser(
        prog='hailctl cluster create',
        description='Create Hail Mini-Batch clusters.')
    subparsers = main_parser.add_subparsers()

    gcp_parser = subparsers.add_parser(
        'gcp',
        help="Create a GCP Hail Mini-Batch cluster",
        description="Create a GCP Hail Mini-Batch cluster")

    gcp_parser.set_defaults(module='gcp')
    gcp.init_parser(gcp_parser)

    return main_parser


def main(args, pass_through_args):
    if not args:
        init_parser().print_help()
        sys.exit(0)

    jmp = {
        'gcp': gcp,
    }

    args, pass_through_args = init_parser().parse_known_args(args=pass_through_args)

    if not args or 'module' not in args:
        init_parser().print_help()
        sys.exit(0)

    if args.module not in jmp:
        sys.stderr.write(f"ERROR: no such module: {args.module!r}")
        init_parser().print_help()
        sys.exit(1)

    jmp[args.module].main(args, pass_through_args)
