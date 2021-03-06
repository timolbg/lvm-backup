#!/usr/bin/env python3

import sys
import logging
import argparse
import yaml
import subprocess
import os

RESTIC = "restic"
LVS = "lvs"

config = []

class BackupException(Exception):
    pass

class Config:
    def __init__(self, configFile, prune):
        with open(configFile, 'r') as stream:
            configYaml = yaml.safe_load(stream)
            self.mounts_dir = configYaml["mounts_dir"]
            self.target_vg = configYaml["TargetVG"]
            self.target_lv = configYaml["TargetLV"]
            self.password = configYaml["password"]
            self.hourlySnapshots = configYaml.get("hourlySnapshots")
            self.dailySnapshots = configYaml.get("dailySnapshots")
            self.weeklySnapshots = configYaml.get("weeklySnapshots")
            self.monthlySnapshots = configYaml.get("monthlySnapshots")
            self.yearlySnapshots = configYaml.get("yearlySnapshots")
            self.prune = prune
            self.sources = []
            for vg in configYaml["VGs"]:
                for lv in vg["LVs"]:
                    self.sources.append(Source(vg["name"], lv["name"], lv.get("options")))


    def get_sources(self):
        return self.sources

class Source:
    def __init__(self, vg, lv, options):
        self.volume = LVolume(vg, lv)
        if not options: 
            self.options = []
        else:
            self.options = options
        self.check_exists()

    def check_exists(self):
        return self.volume.exists()

class Backup:
    def __init__ (self, source, snapshot):
        self.source = source
        self.snapshot = snapshot
        self.volume = LVolume(config.target_vg, config.target_lv)
        if self.volume.is_mounted():
            self.volume.umount()
        self.volume.mount(ro=False)
        if not runCommandRetVal("RESTIC_PASSWORD='%s' restic -r %s snapshots" % (config.password, self.volume.to_mount_dir()) ):
            BackupException("Restic repository %s not properly initialized." % dir)

    def backup(self):
        logging.info("\nbackup STARTED (%s)" % self.snapshot.volume.lv)
        if "raw" in self.source.options:
            source_volume = LVolume(self.snapshot.volume.vg, self.snapshot.volume.lv, raw=True)
        else:
            source_volume = self.snapshot.volume
        source_volume.mount()
        try: 
            self.__run_backup()
        finally:
            source_volume.umount()
        logging.info("\nbackup COMPLETED (%s)" % self.snapshot.volume.lv)

    def __run_backup(self):
        runCommand("RESTIC_PASSWORD='%s' restic -r %s backup %s" % 
            (config.password, self.volume.to_mount_dir(), self.snapshot.volume.to_mount_dir()), 
            printOutput=True )

    def close(self):
        self.volume.umount()
        self.volume.mount(ro=True)

    def cleanup(self):
        keep = ""
        if config.hourlySnapshots:
            keep += f"--keep-hourly {config.hourlySnapshots} "
        if config.dailySnapshots:
            keep += f"--keep-daily {config.dailySnapshots} "
        if config.weeklySnapshots:
            keep += f"--keep-weekly {config.weeklySnapshots} "
        if config.monthlySnapshots:
            keep += f"--keep-monthly {config.monthlySnapshots} "
        if config.prune:
            keep += "--prune"
        runCommand("RESTIC_PASSWORD='%s' restic -r %s forget %s" % (config.password, self.volume.to_mount_dir(), keep), printOutput=True)
    
class Snapshot:
    def __init__(self, source):
        self.source = source
        self.snapshot_lv = source.volume.lv + "_snapshot"
        self.volume = LVolume(source.volume.vg, self.snapshot_lv, source.options)

    def create(self):
        if self.volume.exists():
            logging.warn("Snapshot %s for LV already exists, need to delete first.\n" % self.snapshot_lv)
            self.volume.remove()
        else:
            logging.debug("Snapshot %s does not exist " % self.snapshot_lv)

        runCommand("lvcreate -s -n %s -L 1G %s/%s" % (self.snapshot_lv, self.source.volume.vg, self.source.volume.lv) )
        logging.info("Snapshot volume %s created.\n" % self.snapshot_lv)

    def remove(self):
        return self.volume.remove()

class LVolume:
    def __init__(self, vg, lv, options=[], raw=False):
        self.vg = vg
        self.lv = lv
        self.options = options
        self.raw = raw

    def to_mount_dir(self):
        return "%s/%s/%s" % (config.mounts_dir, self.vg, self.lv)
        
    def to_device(self):
        lv_escaped = self.lv.replace("-", "--")
        return "/dev/mapper/%s-%s" % (self.vg, lv_escaped)

    def exists(self):
        return runCommandRetVal("lvs %s" % self.to_device()) == 0

    def remove(self):
        return runCommandRetVal("lvremove -y %s/%s" % (self.vg, self.lv)) == 0

    def mount(self, ro=False):
        if self.is_mounted():
            BackupException("Volume %s already mounted. Aborting." % self.lv)
        if not os.path.isdir(self.to_mount_dir()):
            os.makedirs(self.to_mount_dir(), exist_ok=True)
        options=""
        if ro:
            options = "-o ro"
        elif "xfs" in self.options:
            options = "-o nouuid"
        if self.raw:
            self.map_raw()
            device = self.to_device() + "1" #TODO: Assuming that VM raw disks have only one partition
        else:
            device = self.to_device() 
        runCommand("mount %s %s %s" % (options, device, self.to_mount_dir()))

    def is_mounted(self):
        if self.raw:
            device = self.to_device() + "1" #TODO: Assuming that VM raw disks have only one partition
        else:
            device = self.to_device() 
        return runCommandRetVal("findmnt %s" % device ) == 0

    def umount(self):
        runCommand("umount %s" % self.to_mount_dir())
        if self.raw:
            self.unmap_raw()


    def map_raw(self):
        runCommand("kpartx -v -a %s" % self.to_device())

    def unmap_raw(self):
        runCommand("kpartx -d %s" % self.to_device())

def runCommand(*args, printOutput=False):
    result = runCommandRetVal(*args, printOutput=printOutput, ignore=False)    
    if (result !=0):
        raise BackupException("Execution failed: " + str(args))

def runCommandRetVal(*args, printOutput=False, ignore=True):
    logging.debug("Running command" + str(args))
    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    (out, err) = p.communicate()
    logging.log(logging.INFO if printOutput else logging.DEBUG, out.decode())
    if (err and not ignore):
        logging.error(err.decode())
    return p.returncode

def check_dependencies():
    for cmd in [RESTIC, LVS]:
        try:
            returncode = runCommandRetVal(cmd)
            if (returncode != 0):
                raise BackupException("Please install %s" % cmd)
        except FileNotFoundError:
            raise BackupException("Please install %s" % cmd)

def main():
    global config
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="backup config file", required=True)
    parser.add_argument("-d", "--debug", help="enable debug logging", action="store_true")
    parser.add_argument("-p", "--prune", help="also prune repository", action="store_true")
    parser.add_argument("command", help="command to execute (backup|cleanup)", choices=["backup", "cleanup"])
    args = parser.parse_args()

    # logging.basicConfig(format='%(asctime)s - %(message)s', )
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    h1 = logging.StreamHandler(sys.stdout)
    h1.setLevel(logging.DEBUG if args.debug else logging.INFO)
    h1.setFormatter(formatter)
    h2 = logging.StreamHandler(sys.stderr)
    h2.setLevel(logging.WARNING)
    h2.setFormatter(formatter)
    logger.addHandler(h1)
    logger.addHandler(h2)

    config = Config(args.config, args.prune)
    
    try:
        check_dependencies()
    except BackupException as be:
        logging.error("Dependencies are missing:")
        logging.error(be.args[0])
        exit(1)

    switches = {
        "backup": backup,
        "cleanup": cleanup
    }
    switches.get(args.command)()

def backup():
        for source in config.sources:
            snapshot = Snapshot(source)
            snapshot.create()
            backup = Backup(source, snapshot)
            try: 
                backup.backup()
            finally:
                backup.close()
                snapshot.remove()

def cleanup():
    backup = Backup(None, None)
    try:
        backup.cleanup()
    finally:
        backup.close()


if __name__ == "__main__":
    main()
    