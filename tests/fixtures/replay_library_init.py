"""Replay the redacted Ocean fixture or an existing, read-only capture directory."""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from crescendo_engine.native_diagnostics import recorded_ocean_warnings, skill_command_diagnostics


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', nargs='?', type=Path)
    parser.add_argument('--marker', default='CRESCENDO_PRIVATE_NETLIST_CAPTURE_DONE')
    args = parser.parse_args()
    root = args.root or Path(__file__).resolve().parent
    if args.root:
        with (root / 'command.json').open(encoding='utf-8') as stream:
            command = json.load(stream)
    else:
        command = {'returncode': 0, 'stdout_tail': (root / 'library_init_ocean.txt').read_text(encoding='utf-8')}
    report = skill_command_diagnostics(command, str(root / 'capture.il'), args.marker,
                                       owned_root=str(root), simulators=('spectre',))
    warnings = recorded_ocean_warnings(report)
    print(json.dumps(dict(ok=report['ok'], first_error=report['first_error'], warnings=warnings),
                     ensure_ascii=True, indent=2))
    expected = ['ADE-5066', 'ADE-5067', 'library_init']
    return int(not report['ok'] or [row.get('native_code') or row.get('category') for row in warnings] != expected)


if __name__ == '__main__':
    sys.exit(main())
