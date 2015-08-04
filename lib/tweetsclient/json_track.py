#!/usr/bin/env python
# encoding: utf-8
"""
json_track.py

Created by Jeremy B. Merrill 2015-08-03
Copyright (c) 2015 The New York Times Company. All rights reserved.
"""

import sys
import os
import unittest
import ConfigParser

import anyjson
import logbook

import requests

import tweetsclient

log = logbook.Logger(__name__)

class JsonTrackPlugin(tweetsclient.TrackPlugin):
    def _get_json(self):
        log.debug("Going to the web for who-to-track JSON")
        fucking_url = self.config.get('tracking', 'json_url')
        resp = requests.get(fucking_url).json
        return resp
    
    def _get_trackings(self):
        json = self._get_json()
        return json["Twitter"]
    
    def get_type(self):
        return self.config.get('tweets-client', 'type')
    
    def get_items(self):
        stream_type = self.get_type()
        if stream_type == 'users':
            return self._get_trackings()
        elif stream_type == 'words':
            return []
        else:
            return []
