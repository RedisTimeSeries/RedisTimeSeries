#!/usr/bin/env python3
import zipfile
import json
import sys
import tempfile
import shutil


def main(zip_path):
    zf = zipfile.ZipFile(zip_path, 'r')
    # print zf.namelist()
    with tempfile.NamedTemporaryFile() as fp:
        zf_write = zipfile.ZipFile(fp.name, 'w')
        for item in zf.infolist():
            data = zf.read(item.filename)
            if item.filename == 'module.json':
                modules_json = json.loads(data)
                modules_json['commands'] = list(filter(lambda x: x['command_name'] not in ['ts.mrange', 'ts.mget', 'ts.queryindex'], modules_json['commands']))

                zf_write.writestr(item, json.dumps(modules_json, indent=4))
            else:
                zf_write.writestr(item, data)
        zf_write.close()
        fp.flush()
        zf.close()
        shutil.copy(fp.name, zip_path)


if __name__ == "__main__":
    main(sys.argv[1])
