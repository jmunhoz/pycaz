# Copyright (c) 2018 Javier M. Mellid <jmunhoz@igalia.com>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import sys

from pycaz.lib.project import Project
from pycaz.lib.options import Options

import pycaz.lib.logger as logger
import pycaz.lib.config as config
import pycaz.lib.wks    as wks

def run_project(args):
    logger.set_up_logger()
    logger.debug('*** caz instance begins')
    logger.debug('grabbing options and parameters')
    # handle '-' bug if needed
    args = wks.fix(args)
    p = args[1:]
    if not p:
        p = ['-h']
    options = Options(p)
    project = Project(options).execute()
    logger.debug('*** caz instance ends')

if __name__ == '__main__':
    run_project(sys.argv)
