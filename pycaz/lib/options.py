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

from argparse import ArgumentParser

class Options:

    cmds = {}
    current_cmd = None

    def __init__(self, params=None):
        self._init_parser()
        self._parse(params)

    def _init_parser(self):
        self.parser = ArgumentParser(prog='caz')
        # init-db
        self.parser.add_argument("-i",
                                 "--init-db",
                                 help="initialize database",
                                 action='store_true')
        # info-db
        self.parser.add_argument("-s",
                                 "--info-db",
                                 help="show database info",
                                 action='store_true')
        # purge-db
        self.parser.add_argument("-p",
                                 "--purge-db",
                                 help="purge database",
                                 action='store_true')
        # fetch-bucket
        self.parser.add_argument("-f",
                                 "--fetch-bucket",
                                 help="fetch bucket",
                                 type=str, nargs=1,
                                 metavar=('bucket'))
        # list-objects
        self.parser.add_argument("-l",
                                 "--list-objects",
                                 help="list objects (versions and delete markers)",
                                 action='store_true')
        # list-objects-by-key
        self.parser.add_argument("-k",
                                 "--list-objects-by-key",
                                 help="list objects by key (versions and delete markers)",
                                 type=str, nargs=1,
                                 metavar=('key'))
        # list-keys
        self.parser.add_argument("-a",
                                 "--list-keys",
                                 help="list all available object keys",
                                 action='store_true')
        # recover-object
        self.parser.add_argument("-r",
                                 "--recover-object",
                                 help="recover object",
                                 type=str, nargs=2,
                                 metavar=('key','version-id'))
        # trim-objects
        self.parser.add_argument("-t",
                                 "--trim-objects",
                                 help="trim objects",
                                 type=str, nargs=1,
                                 metavar=('date'))

    def _parse(self, args=None):
        self.known, self.unknown = self.parser.parse_known_args(args)[:]
        if len(self.unknown) != 0:
            print('Error: unknown args received: ' + repr(self.unknown))
            exit(-1)
        self.args = self.parser.parse_args()
        if self.args.init_db:
            self.cmds['init-db'] = True
        if self.args.info_db:
            self.cmds['info-db'] = True
        if self.args.purge_db:
            self.cmds['purge-db'] = True
        if self.args.list_objects:
            self.cmds['list-objects'] = True
        if self.args.list_objects_by_key:
            self.cmds['list-objects-by-key'] = True
        if self.args.list_keys:
            self.cmds['list-keys'] = True
        if self.args.fetch_bucket:
            self.cmds['fetch-bucket'] = True
        if self.args.recover_object:
            self.cmds['recover-object'] = True
        if self.args.trim_objects:
            self.cmds['trim-objects'] = True

    def get_command(self):
        cmds = ['init-db',
                'info-db',
                'purge-db',
                'list-objects',
                'list-objects-by-key',
                'list-keys',
                'fetch-bucket',
                'recover-object',
                'trim-objects']
        for c in cmds:
            if c in self.cmds:
                self.current_cmd = c
                return c
        return None

    def get_params(self):
        if self.current_cmd == 'fetch-bucket':
            return self.args.fetch_bucket
        if self.current_cmd == 'list-objects-by-key':
            return self.args.list_objects_by_key
        if self.current_cmd == 'recover-object':
            return self.args.recover_object
        if self.current_cmd == 'trim-objects':
            return self.args.trim_objects
        return None
