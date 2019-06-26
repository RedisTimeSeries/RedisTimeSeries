
from __future__ import absolute_import
import platform

#----------------------------------------------------------------------------------------------

class Platform:
    def __init__(self):
        self.os = self.dist = self.os_ver = self.full_os_ver = self.os_nick = self.arch = '?'
    
        self.os = platform.system().lower()
        dist = platform.linux_distribution()
        distname = dist[0].lower()
        self.os_ver = self.full_os_ver = dist[1]
        if self.os == 'linux':
            if distname == 'fedora' or distname == 'ubuntu' or  distname == 'debian' or distname == 'arch':
                pass
            elif distname == 'centos linux':
                distname = 'centos'
            elif distname.startswith('redhat'):
                distname = 'redhat'
            elif distname.startswith('suse'):
                distname = 'suse'
            else:
                Assert(False), "Cannot determine distribution"
            self.dist = distname
        elif self.os == 'darwin':
            self.os = 'macosx'
            self.dist = ''
            mac_ver = platform.mac_ver()
            self.full_os_ver = mac_ver[0] # e.g. 10.14, but also 10.5.8
            self.os_ver = '.'.join(self.full_os_ver.split('.')[:2]) # major.minor
            # self.arch = mac_ver[2] # e.g. x64_64
        elif self.os == 'windows':
            self.dist = self.os
            self.os_ver = platform.release()
            self.full_os_ver = os.version()
        elif self.os == 'sunos':
            self.os = 'solaris'
            self.os_ver = ''
            self.dist = ''
        else:
            Assert(False), "Cannot determine OS"

        self.arch = platform.machine().lower()
        if self.arch == 'amd64' or self.arch == 'x86_64':
            self.arch = 'x64'
        elif self.arch == 'i386' or self.arch == 'i686' or self.arch == 'i86pc':
            self.arch = 'x86'
        elif self.arch == 'aarch64':
            self.arch = 'arm64v8'
        elif self.arch == 'armv7l':
            self.arch = 'arm32v7'

    def is_debian_compat(self):
        return self.dist == 'debian' or self.dist == 'ubuntu'
    
    def is_redhat_compat(self):
        return self.dist == 'redhat' or self.dist == 'centos'
    
    def report(self):
        print("This system is " + self.distname + " " + self.distver + ".\n")

#----------------------------------------------------------------------------------------------

class OnPlatform:
    def __init__(self):
        self.stages = [0]
        self.platform = Platform()

    def invoke(self):
        os = self.os = self.platform.os
        dist = self.dist = self.platform.dist
        self.ver = self.platform.os_ver
        self.common_first()

        for stage in self.stages:
            self.stage = stage
            self.common()
            if os == 'linux':
                self.linux()
                
                if self.platform.is_debian_compat():
                    self.debian_compat()
                if self.platform.is_redhat_compat():
                    self.redhat_compat()
                
                if dist == 'fedora':
                    self.fedora()
                elif dist == 'ubuntu':
                    self.ubuntu()
                elif dist == 'debian':
                    self.debian()
                elif dist == 'centos':
                    self.centos()
                elif dist == 'redhat':
                    self.redhat()
                elif dist == 'suse':
                    self.suse()
                elif dist == 'arch':
                    self.arch()
                else:
                    Assert(False), "Cannot determine installer"
            elif os == 'macosx':
                self.macosx()

        self.common_last()

    def common(self):
        pass

    def common_first(self):
        pass

    def common_last(self):
        pass

    def linux(self):
        pass

    def arch(self):
        pass

    def debian_compat(self): # debian, ubuntu, etc
        pass
    
    def debian(self):
        pass
    
    def centos(self):
        pass
        
    def fedora(self):
        pass

    def redhat_compat(self): # centos, rhel
        pass

    def redhat(self):
        pass
        
    def ubuntu(self):
        pass

    def suse(self):
        pass

    def macosx(self):
        pass

    def windows(self):
        pass

    def bsd_compat(self):
        pass

    def freebsd(self):
        pass
