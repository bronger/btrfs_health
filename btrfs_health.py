#!/usr/bin/env python3

"""Helper routines for converting btrfs’ output into something processable.  In
particular, it fullfills the need to decide whethr a btrfs filesystem is
healthy or not.  Unfortunately, btrfs’ userland utilities produce output that
is unstable in ordering, unstable in device naming, complex to parse, and not
accompanied by proper exit codes.
"""

import re, subprocess, logging, time
from pathlib import Path


def get_filesystems():
    """Returns the mounted btrfs filesystems.

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
    btrfs = subprocess.run(["btrfs", "fi", "show", "--mounted"], check=True, capture_output=True, text=True)
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
            match = re.match("\tdevid\\s* (?P<devid>\\d+) size (?P<size>.+) used (?P<used>.+) path (?P<device_path>.*)", line)
            devices[match.group("devid")] = {"size": match.group("size"), "used": match.group("used"),
                                             "path": match.group("device_path")}
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


def mounted_filesystem_ids():
    """Returns equivalent IDs for all mounted btrfs filesystems.  Unfortunately,
    btrfs filesystems may be identified by three IDs: (1) the UUID of the first
    device, (2) the path to the first device, (3) the mount point.  All three
    IDs are used in the btrfs userland tools in a … well … chaotic way.

    This function makes translation possible by returning all IDs in triplets.

    :returns:
      triplets of the form (UUID, device path, mount point) for all mounted
      btrfs filesystems

    :rtype: set[tuple[str]]
    """
    filesystems = get_filesystems()
    mounts = [line.split()[:4] for line in open("/proc/mounts").readlines()]
    root_mounts = {}
    for mount in mounts:
        if mount[2] == "btrfs":
            options = mount[3].split(",")
            if "subvol=/" in options:
                root_mounts[mount[0]] = mount[1]
    filesystem_ids = set()
    for uuid, data in filesystems.items():
        devices = data["devices"]
        device_path = devices["1"]["path"]
        try:
            filesystem_ids.add((uuid, device_path, root_mounts[device_path]))
        except KeyError:
            raise RuntimeError(f"File system {uuid} is nowhere mounted with subvol=/")
    return filesystem_ids


def get_errors(filesystems):
    """Returns filesystem errors detected by “btrfs device stats”.  This call is
    rather fast (< one second).

    :param filesystems: the filesystems to be checked, as returned by
      `get_filesystems`.

    :type filesystems: dict[str, dict[str, object]]

    :returns:
      The device status as a dictionary mapping the device path
      (e.g. “/dev/sda”) to the number of recorded errors.

    :rtype: dict[str, int]
    """
    devices = {}
    for data in filesystems.values():
        for device in data["devices"].values():
            device_path = device["path"]
            for line in subprocess.run(["btrfs", "device", "stats", device_path], check=True, capture_output=True,
                                       text=True).stdout.splitlines():
                match = re.match(r"\[(?P<device>.+)\]\..+_errs\s+(?P<errors>\d+)", line)
                devices[device_path] = devices.get(device_path, 0) + int(match.group("errors"))
    return devices


def read_scrub_status():
    """Read all scrub status files under ``/var/lib/btrfs``.

    :return:
      The status of all ongoing, cancelled, or finished scrubs as a dictionary
      mapping UUID to a dictionary mapping the device ID to a dictionary
      mapping field names to values.  Impotant field names are “finished”,
      “canceled”, or “total_errors”.  The latter is the only integer value.

    :rtype: dict[str, dict[str, dict[str, (str or int)]]]
    """
    results = {}
    for path in Path("/var/lib/btrfs").glob("scrub.status.*"):
        if not re.match(r"scrub\.status\.[-0-9a-f]{36}$", path.name):
            # Do not read "…_tmp" files.
            continue
        with open(path) as status_file:
            status_lines = status_file.readlines()[1:]
        for device in status_lines:
            device = device.rstrip()
            items = device.split("|")
            uuid, colon, devid = items[0].partition(":")
            assert colon
            results.setdefault(uuid, {})[devid] = device_data = {}
            for item in items[1:]:
                key, colon, value = item.partition(":")
                assert colon, item
                device_data[key] = value
            total_errors = 0
            for key in ("read_errors", "csum_errors", "verify_errors", "csum_discards", "super_errors",
                        "malloc_errors", "uncorrectable_errors", "corrected_errors"):
                total_errors += int(device_data[key])
            device_data["total_errors"] = total_errors
    return results


class ScrubCanceled(RuntimeError):
    pass

def scrub(uuids):
    """Returns filesystem errors detected by “btrfs scrub start”.  This call is
    expensive (scrubbing of all devices).

    :param set[str] uuids: the uuids of the filesystems to be checked; they
      must be mounted

    :returns: The device status as a dictionary mapping the UUID of the
      filesystem to a dictionary mapping the device IDs (numbers starting at 1)
      to dictionaries mapping field names to values.  The most important field
      name is „total_errors“ (calculated by this routine rather than coming
      from btrfs directly) which maps to an integer.

    :rtype: dict[str, dict[str, dict[str, (str or int)]]]
    """
    cancel_scrubs(uuids)
    try:
        for mount_point in (ids[2] for ids in mounted_filesystem_ids() if ids[0] in uuids):
            logging.debug(f"Launch scrub process for {mount_point}")
            subprocess.run(["btrfs", "scrub", "start", mount_point], check=True, stdout=subprocess.DEVNULL)
        while True:
            time.sleep(5)
            if scrub.cancel:
                scrub.cancel_scrubs = False
                raise ScrubCanceled
            results = read_scrub_status()
            unfinished_scrub = False
            for uuid, devices in results.items():
                if uuid in uuids:
                    for device in devices.values():
                        if device["finished"] != "1":
                            unfinished_scrub = True
                        assert device["canceled"] != "1"
            if not unfinished_scrub:
                logging.debug(f"All scrubs finished")
                return results
    except BaseException:
        cancel_scrubs(uuids)
        raise
scrub.cancel = False


def cancel_scrubs(uuids):
    """Cancel the scrubs to the given btrfs filesystems.  If no scrub is ongoing
    for some or all of them, this is ignored.

    :param set[str] uuids: the uuids of the filesystems the scrubs of which
      should be cancelled; they must be mounted
    """
    while True:
        uncanceled_scrubs = set()
        status = read_scrub_status()
        for uuid, devices in status.items():
            if uuid in uuids:
                for device in devices.values():
                    if device["canceled"] != "1":
                        uncanceled_scrubs.add(uuid)
        if not uncanceled_scrubs:
            break
        for mount_point in (ids[2] for ids in mounted_filesystem_ids() if ids[0] in uncanceled_scrubs):
            logging.debug(f"Cancel scrub for {mount_point}")
            process = subprocess.run(["btrfs", "scrub", "cancel", mount_point],
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            assert process.returncode in [0, 2], process.returncode
        time.sleep(1)
