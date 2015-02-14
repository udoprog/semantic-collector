import os
import sys
import argparse
import logging
import signal

from semcollect.core import Core

signal_reload = False
signal_terminate = False
core = None


def handle_signal_reload(sig, frame):
    global signal_reload, core
    signal_reload = True
    core.signalled()


def handle_signal_terminate(sig, frame):
    global signal_terminate, core
    signal_terminate = True
    core.signalled()


log = logging.getLogger(__name__)


def setup_parser(root):
    parser = argparse.ArgumentParser()

    resource_root = os.path.abspath(os.path.join(
        os.path.dirname(os.path.dirname(__file__)), 'collectors'))

    parser.add_argument(
        "-c", "--config",
        dest="config",
        help="Configuration file to read", metavar="<config>",
        default=None,
        type=str)

    parser.add_argument(
        "-p", "--path",
        dest="collectors",
        help="Add path when scanning for collectors", metavar="<path>",
        action="append",
        default=[resource_root])

    parser.add_argument(
        "-t", "--timeout",
        dest="timeout",
        help="Collection timeout", metavar="<num>",
        default=60.0,
        type=float)

    parser.add_argument(
        "-i", "--interval",
        dest="interval",
        help="Collection interval", metavar="<num>",
        default=120.0,
        type=float)

    parser.add_argument(
        "-b", "--backoff",
        dest="backoff",
        help="Collection back-off if run takes too long", metavar="<num>",
        default=10.0,
        type=float)

    parser.add_argument(
        "--debug",
        dest="level",
        help="Enable debug logging",
        default=logging.INFO,
        action='store_const',
        const=logging.DEBUG)

    parser.add_argument(
        "--warn",
        dest="level",
        help="Set log level to WARN",
        default=logging.INFO,
        action='store_const',
        const=logging.WARN)

    return parser


def main(args):
    global signal_reload, signal_terminate, core

    root = os.path.dirname(os.path.dirname(sys.argv[0]))
    parser = setup_parser(root)
    ns = parser.parse_args(args)

    logging.basicConfig(level=ns.level)

    log.info("pid=%d", os.getpid())

    core = Core(timeout=ns.timeout, interval=ns.interval, backoff=ns.backoff,
                config=ns.config, collectors=ns.collectors)
    core.setup()

    signal.signal(signal.SIGHUP, handle_signal_reload)
    signal.signal(signal.SIGTERM, handle_signal_terminate)

    while True:
        if signal_terminate:
            core.stop()
            break

        if signal_reload:
            core.reload()
            signal_reload = False

        core.run_once()

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
