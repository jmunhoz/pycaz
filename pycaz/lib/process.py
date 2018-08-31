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

import subprocess
import shlex

from .exceptions import ProcessException
from . import logger

class ProcessResult:
    pass

class Process:

    def execute(self, command):

        logger.debug("Run command : %s" % command)

        p = subprocess.Popen(shlex.split(command),
                             #encoding='utf-8',
                             stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)

        (stdout, stderr) = p.communicate()

        if p.returncode != 0:
            error_msg = "Error executing command : %s" % command
            logger.debug(error_msg)
            logger.debug('command stderr: [%s]' % stderr)
            logger.debug('command stdout: [%s]' % stdout)
            raise ProcessException(error_msg)

        pr = ProcessResult()

        pr.exit_code = p.returncode
        pr.stdout = stdout
        pr.stderr = stderr
        pr.command = command

        return pr
