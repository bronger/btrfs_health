#!/usr/bin/env python3

"""Helper routines for converting btrfs’ output into something processable.  In
particular, it fullfills the need to decide whethr a btrfs filesystem is
healthy or not.  Unfortunately, btrfs’ userland utilities produce output that
is unstable in ordering, unstable in device naming, complex to parse, and not
accompanied by proper exit codes.
"""

import re, subprocess


def get_filesystems():
    """Returns the “present filesystems” as btrfs calls them.  I suspect, *mounted*
    filesystems are meant.

    :returns:
      All mounted btrfs filesystems, as a dictionary mapping the UUID to
      filesystem data.  The filesystem data is a dictionary mapping field names
      to its value.  The fields are “label” (UUID), “number_devices”,
      “bytes_used”, and “devices”.  For the field “devices”, the value is a
      dictionary mapping the “devid” (a number) to a dictionary mapping field
      names to device data.  Here, the field names are “size”, “used”, and
      “path” (e.g. “/dev/sda”).

    :rtype: dict[str, dict[str, object]]
    """
    btrfs = subprocess.run(["btrfs", "fi", "show"], check=True, capture_output=True, text=True)
    assert not btrfs.stderr, btrfs.stderr
    lines = iter(btrfs.stdout.splitlines())

    def parse_filesystem(lines):
        data = {}

        match = re.match(r"Label: (?P<label>.+)  uuid: (?P<uuid>[-0-9a-f]+)", next(lines))
        uuid = match.group("uuid")
        data["label"] = match.group("label")

        match = re.match("\tTotal devices (?P<number_devices>\\d+) FS bytes used (?P<bytes_used>.+)", next(lines))
        data["number_devices"] = int(match.group("number_devices"))
        data["bytes_used"] = match.group("bytes_used")
        devices = {}

        while line := next(lines):
            match = re.match("\tdevid\\s* (?P<devid>\\d+) size (?P<size>.+) used (?P<used>.+) path (?P<path>.*)", line)
            devices[match.group("devid")] = {"size": match.group("size"), "used": match.group("used"),
                                             "path": match.group("path")}
        data["devices"] = devices

        return uuid, data

    filesystems = {}
    try:
        while True:
            uuid, data = parse_filesystem(lines)
            filesystems[uuid] = data
    except StopIteration:
        pass
    return filesystems


def get_errors(filesystems):
    """Returns filesystem errors detected by “btrfs device stats”.  This call is
    rather fast (< one second).

    :param filesystems: the filesystems to be checked, as returned by
      `get_filesystems`.

    :type filesystems: dict[str, dict[str, object]]

    :returns:
      The device status as a dictionary mapping the device path
      (e.g. “/dev/sda” to the number of recorded errors.

    :rtype: dict[str, int]
    """
    devices = {}
    for data in filesystems.values():
        for device in data["devices"].values():
            path = device["path"]
            for line in subprocess.run(["btrfs", "device", "stats", path], check=True, capture_output=True,
                                       text=True).stdout.splitlines():
                match = re.match(r"\[(?P<device>.+)\]\..+_errs\s+(?P<errors>\d+)", line)
                devices[path] = devices.get(path, 0) + int(match.group("errors"))
    return devices


def get_scrub_results(filesystems):
    """Returns filesystem errors detected by “btrfs device stats”.  This call is
    expensive (scrubbing of all devices).

    :param filesystems: the filesystems to be checked, as returned by
      `get_filesystems`.

    :type filesystems: dict[str, dict[str, object]]

    :returns:
      The device status as a dictionary mapping the device path
      (e.g. “/dev/sda”) to a dictionary mapping field names to values.  In case
      of errors, the full scrub error message is returned in the single field
      “error_message”.

    :rtype: dict[str, dict[str, str]]
    """
    devices = {}
    for uuid, data in filesystems.items():
        for device in data["devices"].values():
            path = device["path"]
            output =  subprocess.run(["btrfs", "scrub", "start", "-B", path], check=True, capture_output=True,
                                     text=True).stdout
            match = re.match(r"""scrub done for (?P<uuid>.+)
Scrub started:\s* (?P<timestamp>.+)
Status:\s* finished
Duration:\s* (?P<duration>.+)
Total to scrub:\s* (?P<total_to_scrub>.+)
Rate:\s* (?P<rate>.+)
Error summary:\s* no errors found
$""", output, re.MULTILINE)
            if match:
                assert match.group("uuid") == uuid
                devices[path] = {"timestamp": match.group("timestamp"),
                                 "duration": match.group("duration"),
                                 "total_to_scrub": match.group("total_to_scrub"),
                                 "rate": match.group("rate")}
            else:
                devices[path] = {"error_message": output}
    return devices
