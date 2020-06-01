#!/usr/bin/env python3

import re, subprocess


def get_filesystems():
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
    devices = {}
    for data in filesystems.values():
        for device in data["devices"].values():
            path = device["path"] 
            for line in subprocess.run(["btrfs", "device", "stats", path], check=True, capture_output=True,
                                       text=True).stdout.splitlines():
                match = re.match(r"\[(?P<device>.+)\]\..+_errs\s+(?P<errors>\d+)", line)
                devices[path] = devices.get(path, 0) + int(match.group("errors"))
    return devices
    return devices
