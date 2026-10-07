"""Portable installer checks; no test writes real user systemd units or uses USB."""

import importlib.util
import os
from pathlib import Path
import shlex
import sys
import tempfile
import unittest
from unittest.mock import patch


SCRIPT = Path(__file__).resolve().parents[1] / "scripts/install_pi.py"
SPEC = importlib.util.spec_from_file_location("install_pi", SCRIPT)
installer = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = installer
SPEC.loader.exec_module(installer)


def command_arguments(unit):
    line = next(line for line in unit.splitlines() if line.startswith("ExecStart="))
    # For the escaping subset emitted here, shell quote splitting followed by
    # literal systemd expansion reproduces argv; no shell is ever executed.
    return [word.replace("%%", "%").replace("$$", "$") for word in shlex.split(line.split("=", 1)[1])]


class InstallerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name).resolve()
        self.root = self.base / 'repository with spaces %n $HOME "quoted"'
        (self.root / "live").mkdir(parents=True)
        for filename in installer.RUNTIME_FILES:
            (self.root / "live" / filename).write_text("# test fixture\n")
        self.data = self.base / "data with spaces %h $HOME"
        self.serial = "/dev/serial/by-id/usb-Test_Board_0001-if00"
        self.arguments = ["--root", str(self.root), "--data-dir", str(self.data),
                          "--serial", self.serial, "--bacnet-address", "192.0.2.10/24:47808",
                          "--instance", "75151", "--name", 'Meter %i $HOME "one"']
        self.environment = patch.dict(os.environ, {"XDG_CONFIG_HOME": str(self.base / "config")})
        self.environment.start()
        self.addCleanup(self.environment.stop)

    def config(self, extra=()):
        return installer.Config.from_args(installer.parser().parse_args([*self.arguments, *extra]))

    def test_five_units_preserve_configuration_and_runtime_contract(self):
        config = self.config(["--metasys", "192.0.2.20:47809"])
        units = installer.render_units(config)
        self.assertEqual(set(units), {"iqdata-poll.service", "iqdata-http.service", "iqdata-bacnet.service", "iqdata-snapshot.service", "iqdata-snapshot.timer"})
        poll = command_arguments(units["iqdata-poll.service"])
        self.assertEqual(poll[:3], [str(self.root / ".venv/bin/python"), "-u", str(self.root / "live/poll_meter.py")])
        self.assertEqual(poll[poll.index("--port") + 1], self.serial)
        self.assertEqual(poll[poll.index("--state") + 1], str(self.data / "state"))
        self.assertEqual(poll[poll.index("--database") + 1], str(self.data / "iqdata.sqlite"))
        for option, value in (("--kinds", "all_standard"), ("--duration-ms", "500"), ("--interval", "1.05")):
            self.assertEqual(poll[poll.index(option) + 1], value)
        for flag in ("--diagnostics", "--continuous", "--recover"):
            self.assertIn(flag, poll)
        bacnet = command_arguments(units["iqdata-bacnet.service"])
        for option, value in (("--address", config.bacnet_address), ("--instance", "75151"), ("--name", config.name), ("--metasys", config.metasys)):
            self.assertEqual(bacnet[bacnet.index(option) + 1], value)
        http = command_arguments(units["iqdata-http.service"])
        self.assertEqual(http[http.index("--bind") + 1], "0.0.0.0")
        self.assertEqual(http[http.index("--port") + 1], "8080")
        self.assertEqual(http[http.index("--database-snapshot") + 1], str(self.data / "iqdata-latest.sqlite"))
        self.assertEqual(command_arguments(units["iqdata-snapshot.service"])[-2:], [str(self.data / "iqdata.sqlite"), str(self.data / "iqdata-latest.sqlite")])
        self.assertIn("OnUnitInactiveSec=1h", units["iqdata-snapshot.timer"])
        self.assertIn("Nice=10", units["iqdata-snapshot.service"])
        self.assertIn("IOSchedulingClass=idle", units["iqdata-snapshot.service"])
        self.assertIn("TimeoutStartSec=30min", units["iqdata-snapshot.service"])
        self.assertIn("TimeoutStopSec=60s", units["iqdata-snapshot.service"])
        snapshot = command_arguments(units["iqdata-snapshot.service"])
        self.assertEqual(snapshot[snapshot.index("--max-seconds") + 1], "1500")
        cleanup = command_arguments(units["iqdata-snapshot.service"].replace("ExecStart=", "StartIgnored=")
                                    .replace("ExecStopPost=", "ExecStart="))
        self.assertEqual(cleanup[-3:], ["--cleanup-only", str(self.data / "iqdata.sqlite"),
                                       str(self.data / "iqdata-latest.sqlite")])
        self.assertIn('%%n $$HOME \\"quoted\\"', units["iqdata-poll.service"])
        for name, unit in units.items():
            self.assertNotIn("/bin/sh", unit)
            if name.endswith(".service"):
                self.assertIn("WorkingDirectory=" + str(self.root / "live").replace("%", "%%"), unit)

    def test_optional_metasys_uses_no_site_default(self):
        unit = installer.render_units(self.config())["iqdata-bacnet.service"]
        self.assertNotIn("--metasys", command_arguments(unit))
        self.assertNotIn("--metasys", unit)

    def test_name_cannot_inject_a_command_or_environment_expansion(self):
        for name in ('${HOME} $(touch /tmp/never) ; /bin/false', ';', 'literal \\ and "quotes"'):
            with self.subTest(name=name):
                unit = installer.render_units(self.config(["--name", name]))["iqdata-bacnet.service"]
                arguments = command_arguments(unit)
                self.assertEqual(arguments[arguments.index("--name") + 1], name)
                self.assertEqual(sum(line.startswith("ExecStart=") for line in unit.splitlines()), 1)

    def test_invalid_configuration_changes_nothing(self):
        cases = [
            ["--bacnet-address", "bad/24:47808"], ["--bacnet-address", "192.0.2.10/33:47808"],
            ["--bacnet-address", "192.0.2.10/24:65536"], ["--bacnet-address", "192.0.2.0/24:47808"],
            ["--bacnet-address", "0.0.0.0/24:47808"], ["--bacnet-address", "224.0.0.1/24:47808"],
            ["--metasys", "192.0.2.20;touch /tmp/x"], ["--metasys", "192.0.2.20:0"],
            ["--serial", "/dev/ttyACM0"], ["--serial", "/dev/serial/by-id/../ttyACM0"],
            ["--name", "line\nExecStart=/bin/false"], ["--name", "\t"],
            ["--instance", "-1"], ["--instance", "4194303"],
            ["--data-dir", str(self.root / "state")], ["--root", str(self.base / "missing")],
        ]
        for extra in cases:
            with self.subTest(extra=extra), self.assertRaises(ValueError):
                self.config(extra)
        self.assertFalse(self.data.exists())
        self.assertFalse((self.base / "config").exists())

    def test_resolved_symlink_cannot_hide_a_control_character(self):
        actual = self.base / "data\nExecStart=bad"
        actual.mkdir()
        alias = self.base / "innocent-looking-data"
        alias.symlink_to(actual)
        with self.assertRaises(ValueError):
            self.config(["--data-dir", str(alias)])

    def test_render_does_not_install_or_create_state(self):
        output = self.base / "rendered"
        self.assertEqual(installer.main([*self.arguments, "--output-dir", str(output)]), 0)
        self.assertEqual(len(list(output.glob("iqdata-*"))), 5)
        self.assertFalse(self.data.exists())
        self.assertFalse((self.base / "config").exists())

    def test_active_unit_directory_requires_install(self):
        with self.assertRaises(SystemExit) as error:
            installer.main([*self.arguments, "--output-dir", str(installer.user_unit_dir())])
        self.assertEqual(error.exception.code, 2)
        self.assertFalse((self.base / "config").exists())

    def test_replacement_requires_explicit_flag_and_preserves_backup(self):
        output = self.base / "rendered"
        units = installer.render_units(self.config())
        installer.write_units(units, output)
        old_path = output / "iqdata-poll.service"
        old_path.write_text("prior configuration\n")
        with self.assertRaises(ValueError):
            installer.write_units(units, output)
        self.assertEqual(old_path.read_text(), "prior configuration\n")
        backup = installer.write_units(units, output, replace=True)
        self.assertEqual((backup / old_path.name).read_text(), "prior configuration\n")
        self.assertEqual(old_path.read_text(), units[old_path.name])

    def test_symlink_unit_is_never_overwritten(self):
        output = self.base / "rendered"
        output.mkdir()
        target = self.base / "unrelated"
        target.write_text("keep me\n")
        (output / "iqdata-poll.service").symlink_to(target)
        with self.assertRaises(ValueError):
            installer.write_units(installer.render_units(self.config()), output, replace=True)
        self.assertEqual(target.read_text(), "keep me\n")
        self.assertEqual(len(list(output.iterdir())), 1)

    def test_explicit_install_only_writes_isolated_user_units(self):
        python = self.root / ".venv/bin/python"
        python.parent.mkdir(parents=True)
        python.write_text("#!/bin/false\n")
        python.chmod(0o755)
        with patch.object(installer.sys, "platform", "linux"):
            self.assertEqual(installer.main([*self.arguments, "--install"]), 0)
        self.assertTrue((self.data / "state").is_dir())
        self.assertEqual(len(list(installer.user_unit_dir().glob("iqdata-*"))), 5)
        self.assertFalse((self.root / "build").exists())


if __name__ == "__main__":
    unittest.main()
