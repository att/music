#!/usr/bin/env python
# -*- encoding: utf-8 -*-
#
# Copyright (c) 2016 AT&T
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
#
# See the License for the specific language governing permissions and
# limitations under the License.

'''Voting App Example for Music.'''

# Standard library imports
import time

# Related third party imports

# Local application/library specific imports
from music import Music


def current_time_millis():
    '''Current time in milliseconds.'''
    return int(round(time.time()*1000))

def main():
    '''Sample usage of Music.'''
    kwargs = {'host': 'localhost'}
    music = Music(**kwargs)
    print "Music version %s" % music.version()

    # Randomize the name so that we don't step on each other.
    keyspace = 'NewVotingApp' + str(current_time_millis()/100)
    music.create_keyspace(keyspace)
    print "Created keyspace: %s" % keyspace

    # Create the table
    kwargs = {
        'keyspace': keyspace,
        'table': 'votecount',
        'schema': {
            'name': 'text',
            'count': 'varint',
            'PRIMARY KEY': '(name)'
        }
    }
    music.create_table(**kwargs)

    # Candidate data
    data = {
        'Trump': 5,
        'Bush': 7,
        'Jeb': 8,
        'Clinton': 2,
        'Bharath': 0
    }

    # Create an entry in the voting table for each candidate
    # and with a vote count of 0.
    kwargs = {'keyspace': keyspace, 'table': 'votecount'}
    for name in data.iterkeys():
        kwargs['values'] = {'name': name, 'count': 0}
        music.create_row(**kwargs)

    # Update each candidate's count atomically.
    kwargs = {'keyspace': keyspace, 'table': 'votecount', 'pk_name': 'name'}
    for name, count in data.iteritems():
        kwargs['pk_value'] = name
        kwargs['values'] = {'count': count}
        music.update_row_atomically(**kwargs)

    # Read all rows
    kwargs = {'keyspace': keyspace, 'table': 'votecount'}
    print music.read_all_rows(**kwargs)

    # Delete Clinton, read Trump
    kwargs = {'keyspace': keyspace, 'table': 'votecount', 'pk_name': 'name'}
    kwargs['pk_value'] = 'Clinton'
    music.delete_row_eventually(**kwargs)
    kwargs['pk_value'] = 'Trump'
    print music.read_row(**kwargs)

    # Read all rows again
    kwargs = {'keyspace': keyspace, 'table': 'votecount'}
    print music.read_all_rows(**kwargs)

    # Cleanup.
    music.drop_keyspace(keyspace)
    music.delete_all_locks()

if __name__ == "__main__":
    main()
