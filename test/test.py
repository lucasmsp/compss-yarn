#!/usr/bin/python
# -*- coding: utf-8 -*-

from pycompss.api.task import task
from pycompss.api.parameter import *

import sys
import time
import random


@task(returns=1)
def consumer(wait_time):
    time.sleep(wait_time)
    localtime = time.localtime()
    timestamp_str = time.strftime("%b %d %Y %H:%M:%S", localtime)
    return [wait_time, timestamp_str]


def producer(nfrag, min_time, max_time):

    from pycompss.api.api import compss_wait_on
    time_list = [random.uniform(min_time, max_time) for _ in range(nfrag)]
    result = [consumer(i) for i in time_list]
    result = compss_wait_on(result)
    print (result)


if __name__ == "__main__":

    nfrag = int(sys.argv[1]) if sys.argv[1] is not None else 32
    min_time = int(sys.argv[2]) if sys.argv[2] is not None else 10
    max_time = int(sys.argv[3]) if sys.argv[3] is not None else 30
    producer(nfrag, min_time, max_time)
