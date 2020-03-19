#!/bin/bash
export PATH=.:/app/.virtualenvs/py37/bin:$PATH

export LANG=zh_CN
export LANGUAGE='zh_CN.GB18030:zh_CN.GB2312:zh_CN'

python transfer_ims_ntf.py

