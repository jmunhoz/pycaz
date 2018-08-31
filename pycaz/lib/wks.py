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

from . import logger

FIXON = False

def fix(args):
    global FIXON
    if len(args) == 4 and (args[1] == '-r' or args[1] == '--recover-object'):
         if args[3].startswith('-'):
              logger.debug("Fixing version-id to avoid '-' bug : {}".format(args[3]))
              args[3] = 'x' + args[3]
              FIXON = True
    return args

def unfix(s3_cmd_params):
      global FIXON
      if FIXON:
            logger.debug("Unfixing version-id to avoid '-' bug : {}".format(s3_cmd_params[1]))
            s3_cmd_params[1] = s3_cmd_params[1][1:]
            FIXON = False
      return s3_cmd_params
