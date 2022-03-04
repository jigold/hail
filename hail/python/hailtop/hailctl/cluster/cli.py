import sys
import argparse

from . import create


def parser():
    main_parser = argparse.ArgumentParser(
        prog='hailctl cluster',
        description='Manage Hail Mini-Batch clusters.')
    subparsers = main_parser.add_subparsers()

    create_parser = subparsers.add_parser(
        'create',
        help='Create a Hail Mini-Batch cluster',
        description='Create a Hail Mini-Batch cluster')

    create_parser.set_defaults(module='create')

    return main_parser


def main(args):
    if not args:
        parser().print_help()
        sys.exit(0)

    args, pass_through_args = parser().parse_known_args(args=args)

    if not args or 'module' not in args:
        parser().print_help()
        sys.exit(0)

    if args.module == 'create':
        from .create import cli  # pylint: disable=import-outside-toplevel
        cli.main(args, pass_through_args)
        return

    raise Exception(f'unknown module {args.module}')
